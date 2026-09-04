using System.Net;
using System.Net.Sockets;
using System.Text;
using BackendServer.Hubs;
using BackendServer.Models;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Globalization;
using System.Threading.Channels;

namespace BackendServer.Services;

public class TcpServerService
{
    private TcpListener? _listener;
    private TcpClient? _raspberryClient;
    private StreamWriter? _writer;
    private StreamReader? _reader;
    private DateTime _lastPongTime = DateTime.MinValue;
    private DateTime _lastTelemetryTime = DateTime.MinValue;
    private DateTime _lastHealthPublishTimeUtc = DateTime.MinValue;
    private string _disconnectReason = "not_connected";
    private bool _isRaspberryConnected;
    private CancellationTokenSource? _heartbeatCts;
    private int _activeConnectionId;

    private readonly TelemetryService _telemetryService;
    private readonly IHubContext<TelemetryHub> _hub;
    private readonly LoggingService _logger;
    private readonly SemaphoreSlim _socketSemaphore = new(1, 1);
    private readonly object _connectionStateLock = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<CommandExecutionResult>> _pendingCommands = new();
    private readonly Channel<string> _telemetryQueue = Channel.CreateBounded<string>(
        new BoundedChannelOptions(1000)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropOldest
        });
    private static readonly TimeSpan CommandAckTimeout = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan RaspberryWatchdogTimeout = TimeSpan.FromSeconds(1.5);
    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromMilliseconds(250);
    private static readonly string[] AccelerationLogDeviceIds = ["4", "5"];
    private IReadOnlyDictionary<string, object>? _lastControlState;
    private readonly Dictionary<string, DeviceHealthState> _deviceHealth = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, AccelerationSnapshot> _accelerationSnapshots = new(StringComparer.OrdinalIgnoreCase);

    /*
    // Otomatik acil durum koruması daha sonra aktif edilecek.
    // Kural:
    // - I3 < 5
    // - Manuel ileri/geri aktif veya cihaz 2 otonom ileri/geri bildiriyor
    // - Bu durumda backend bir kere EMERGENCY yollar ve UI controlState'e
    //   autoEmergencyActive:true ekleyerek "A. ACİL DURUM" gösterir.
    // - Kullanıcı Acil butonuna bir kere basınca latch temizlenir.
    private const double AutoEmergencyCurrentThreshold = 5;
    private bool _autoEmergencyActive;
    private bool _autoEmergencyCommandSent;
    private double? _latestI3;
    private bool? _device2AutonomousForward;
    private bool? _device2AutonomousBackward;
    */

    private sealed class DeviceHealthState
    {
        public long ReceivedPackets { get; set; }
        public long DroppedPackets { get; set; }
        public long OutOfOrderPackets { get; set; }
        public long? LastSequence { get; set; }
        public DateTime FirstPacketTimeUtc { get; set; }
        public DateTime LastPacketTimeUtc { get; set; }
    }

    private sealed class AccelerationSnapshot
    {
        public double? AX { get; set; }
        public double? AY { get; set; }
        public double? AZ { get; set; }
        public bool HasPendingLogValue { get; set; }
    }

    public TcpServerService(
        TelemetryService telemetryService,
        IHubContext<TelemetryHub> hub,
        LoggingService logger)
    {
        _telemetryService = telemetryService;
        _hub = hub;
        _logger = logger;
    }

    public void Start()
    {
        _listener = new TcpListener(IPAddress.Any, 5005);
        _listener.Start();

        _logger.TcpInfo("TCP Server başlatıldı. Raspberry bekleniyor...");
        Task.Run(ProcessTelemetryQueue);
        Task.Run(AcceptClientLoop);
    }

    private async Task AcceptClientLoop()
    {
        var listener = _listener ?? throw new InvalidOperationException("TCP listener başlatılmadı.");

        while (true)
        {
            try
            {
                var raspberryClient = await listener.AcceptTcpClientAsync();
                raspberryClient.NoDelay = true;

                var stream = raspberryClient.GetStream();
                var reader = new StreamReader(stream, new UTF8Encoding(false));
                var writer = new StreamWriter(stream, new UTF8Encoding(false)) { AutoFlush = true };
                var heartbeatCts = new CancellationTokenSource();
                int connectionId;
                TcpClient? previousClient;
                CancellationTokenSource? previousHeartbeatCts;

                await _socketSemaphore.WaitAsync();
                try
                {
                    previousClient = _raspberryClient;
                    previousHeartbeatCts = _heartbeatCts;

                    _raspberryClient = raspberryClient;
                    _reader = reader;
                    _writer = writer;
                    _heartbeatCts = heartbeatCts;
                    lock (_connectionStateLock)
                    {
                        connectionId = ++_activeConnectionId;
                        _lastPongTime = DateTime.UtcNow;
                        _lastTelemetryTime = DateTime.MinValue;
                        _deviceHealth.Clear();
                        _accelerationSnapshots.Clear();
                    }
                }
                finally
                {
                    _socketSemaphore.Release();
                }

                previousHeartbeatCts?.Cancel();
                previousClient?.Close();

                await SetConnectionState(true, "connected");

                _ = Task.Run(() => HeartbeatLoop(raspberryClient, writer, connectionId, heartbeatCts.Token));

                _logger.TcpInfo("Raspberry Pi bağlandı.");

                _ = Task.Run(() => ListenClientLoop(raspberryClient, reader, writer, heartbeatCts, connectionId));
            }
            catch (Exception ex)
            {
                _logger.TcpError("Yeni Raspberry bağlantısı kabul edilemedi: " + ex.Message);
                await Task.Delay(1000);
            }
        }
    }

    private async Task ListenClientLoop(
        TcpClient raspberryClient,
        StreamReader reader,
        StreamWriter writer,
        CancellationTokenSource heartbeatCts,
        int connectionId)
    {
        try
        {
            while (true)
            {
                if (!IsCurrentConnection(connectionId)) break;

                var line = await reader.ReadLineAsync();
                if (line == null)
                {
                    _logger.TcpError("Raspberry bağlantısı koptu.");
                    await SetConnectionStateForConnection(connectionId, false, "socket_closed");
                    break;
                }

                line = line.TrimStart('\uFEFF');

                // PONG gecikse bile geçerli bir paket alınması bağlantının canlı olduğunu gösterir.
                lock (_connectionStateLock)
                {
                    _lastPongTime = DateTime.UtcNow;
                }

                if (line == "PONG")
                {
                    continue;
                }

                if (line.StartsWith("ACK|", StringComparison.Ordinal))
                {
                    await HandleCommandAck(line);
                    continue;
                }

                if (line.StartsWith("STATE|", StringComparison.Ordinal))
                {
                    var state = ParseControlState(line[6..]);
                    if (state != null)
                    {
                        SetLastControlState(state);
                        await _hub.Clients.All.SendAsync("controlState", state);
                    }
                    continue;
                }

                lock (_connectionStateLock)
                {
                    _lastTelemetryTime = DateTime.Now;
                    UpdateDeviceHealth(line, DateTime.UtcNow);
                    LogMergedAcceleration(line);
                }
                WriteTelemetryDeviceIdToTerminal(line);
                _telemetryQueue.Writer.TryWrite(line);

                /*
                // Otomatik acil durum koruması aktif edildiğinde bu çağrı açılacak.
                // Cihaz 2'den gelen F/B/BR/E otonom hareket bilgisini ve I3 akımını
                // son telemetri satırından güncelleyip gerekirse EMERGENCY tetikler.
                await EvaluateAutoEmergencyGuard(line);
                */
            }
        }
        catch (Exception ex)
        {
            _logger.TcpError("TCP okuma hatası: " + ex.Message);
            await SetConnectionStateForConnection(connectionId, false, "read_error");
        }
        finally
        {
            reader.Close();
            writer.Close();
            raspberryClient.Close();
            heartbeatCts.Cancel();

            var cleanedCurrentConnection = false;
            await _socketSemaphore.WaitAsync();
            try
            {
                lock (_connectionStateLock)
                {
                    if (connectionId == _activeConnectionId)
                    {
                        _heartbeatCts = null;
                        _reader = null;
                        _writer = null;
                        _raspberryClient = null;
                        _lastPongTime = DateTime.MinValue;
                        cleanedCurrentConnection = true;
                    }
                }
            }
            finally
            {
                _socketSemaphore.Release();
            }

            if (cleanedCurrentConnection)
            {
                await SetConnectionState(false, _disconnectReason == "connected" ? "connection_cleaned" : _disconnectReason);

                _logger.TcpInfo("Bağlantı temizlendi.");
            }
        }
    }

    private async Task ProcessTelemetryQueue()
    {
        await foreach (var line in _telemetryQueue.Reader.ReadAllAsync())
        {
            try
            {
                await _telemetryService.HandleRawTelemetry(line);
            }
            catch (Exception ex)
            {
                _logger.TelemetryError("Telemetry işleme hatası: " + ex.Message);
            }
        }
    }

    public async Task<CommandExecutionResult> SendCommand(string command)
    {
        var commandId = Guid.NewGuid().ToString("N");
        if (_raspberryClient == null || _writer == null)
        {
            _logger.CommandError("Raspberry bağlı değil, komut gönderilemedi.");
            return new(false, commandId, command, "not_connected");
        }

        var msg = $"CMD|{commandId}|{command}";
        var completion = new TaskCompletionSource<CommandExecutionResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingCommands[commandId] = completion;

        await _socketSemaphore.WaitAsync();
        try
        {
            await _writer.WriteLineAsync(msg);
        }
        catch (Exception ex)
        {
            _logger.CommandError("Komut gönderilemedi: " + ex.Message);
            _raspberryClient?.Close();
            await SetConnectionState(false, "command_send_error");
            _pendingCommands.TryRemove(commandId, out _);
            return new(false, commandId, command, "send_error");
        }
        finally
        {
            _socketSemaphore.Release();
        }

        try
        {
            return await completion.Task.WaitAsync(CommandAckTimeout);
        }
        catch (TimeoutException)
        {
            _logger.CommandError($"Komut ACK zaman aşımı: {command} ({commandId})");
            return new(false, commandId, command, "ack_timeout");
        }
        finally
        {
            _pendingCommands.TryRemove(commandId, out _);
        }
    }

    private async Task HandleCommandAck(string line)
    {
        var parts = line.Split('|', 6);
        if (parts.Length < 5) return;

        var commandId = parts[1];
        var success = string.Equals(parts[2], "OK", StringComparison.OrdinalIgnoreCase);
        var command = parts[3];
        var reason = parts[4];
        var state = parts.Length == 6 ? ParseControlState(parts[5]) : null;

        if (state != null)
        {
            SetLastControlState(state);
            await _hub.Clients.All.SendAsync("controlState", state);
        }

        if (_pendingCommands.TryGetValue(commandId, out var completion))
            completion.TrySetResult(new(success, commandId, command, reason, state));
    }

    private static IReadOnlyDictionary<string, object>? ParseControlState(string raw)
    {
        var state = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        foreach (var field in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var pair = field.Split(':', 2, StringSplitOptions.TrimEntries);
            if (pair.Length != 2) continue;
            if (pair[0].Equals("vfdHz", StringComparison.OrdinalIgnoreCase) ||
                pair[0].Equals("vfdPwmValue", StringComparison.OrdinalIgnoreCase))
            {
                if (int.TryParse(pair[1], out var numericValue))
                    state[pair[0]] = numericValue;
                continue;
            }

            state[pair[0]] = pair[1] == "1" ||
                             bool.TryParse(pair[1], out var booleanValue) && booleanValue;
        }
        return state.Count == 0 ? null : state;
    }

    private static void WriteTelemetryDeviceIdToTerminal(string telemetryLine)
    {
        var separatorIndex = telemetryLine.IndexOf('|');
        if (separatorIndex <= 0) return;

        var deviceId = telemetryLine[..separatorIndex].Trim();
        if (deviceId.Length > 0)
            Console.WriteLine(deviceId);
    }

    private void LogMergedAcceleration(string telemetryLine)
    {
        var separatorIndex = telemetryLine.IndexOf('|');
        if (separatorIndex <= 0) return;

        var deviceId = telemetryLine[..separatorIndex];
        if (!AccelerationLogDeviceIds.Contains(deviceId, StringComparer.OrdinalIgnoreCase)) return;

        var payload = telemetryLine[(separatorIndex + 1)..];
        var values = ParseAccelerationValues(payload);
        if (!values.HasAnyValue) return;

        var snapshot = GetOrCreateAccelerationSnapshot(deviceId);
        if (values.AX.HasValue) snapshot.AX = values.AX;
        if (values.AY.HasValue) snapshot.AY = values.AY;
        if (values.AZ.HasValue) snapshot.AZ = values.AZ;

        if (deviceId.Equals("4", StringComparison.OrdinalIgnoreCase) && values.AX.HasValue)
        {
            snapshot.HasPendingLogValue = true;
        }
        else if (deviceId.Equals("5", StringComparison.OrdinalIgnoreCase) && (values.AY.HasValue || values.AZ.HasValue))
        {
            snapshot.HasPendingLogValue = true;
        }

        if (!_accelerationSnapshots.TryGetValue("4", out var device4) ||
            !_accelerationSnapshots.TryGetValue("5", out var device5) ||
            !device4.AX.HasValue ||
            !device5.AY.HasValue ||
            !device5.AZ.HasValue ||
            !device4.HasPendingLogValue ||
            !device5.HasPendingLogValue)
        {
            return;
        }

        _logger.TelemetryInfo(
            $"4|AX:{FormatAccelerationValue(device4.AX)} " +
            $"5|AY:{FormatAccelerationValue(device5.AY)},AZ:{FormatAccelerationValue(device5.AZ)}");

        device4.HasPendingLogValue = false;
        device5.HasPendingLogValue = false;
    }

    private AccelerationSnapshot GetOrCreateAccelerationSnapshot(string deviceId)
    {
        if (_accelerationSnapshots.TryGetValue(deviceId, out var snapshot))
        {
            return snapshot;
        }

        snapshot = new AccelerationSnapshot();
        _accelerationSnapshots[deviceId] = snapshot;
        return snapshot;
    }

    private static (double? AX, double? AY, double? AZ, bool HasAnyValue) ParseAccelerationValues(string payload)
    {
        double? ax = null;
        double? ay = null;
        double? az = null;
        var hasAnyValue = false;

        foreach (var field in payload.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var pair = field.Split(':', 2, StringSplitOptions.TrimEntries);
            if (pair.Length != 2 ||
                !double.TryParse(pair[1].Replace(',', '.'), NumberStyles.Any, CultureInfo.InvariantCulture, out var value) ||
                double.IsNaN(value) ||
                double.IsInfinity(value))
            {
                continue;
            }

            switch (pair[0].Trim().ToUpperInvariant())
            {
                case "AX":
                    ax = value;
                    hasAnyValue = true;
                    break;
                case "AY":
                    ay = value;
                    hasAnyValue = true;
                    break;
                case "AZ":
                    az = value;
                    hasAnyValue = true;
                    break;
            }
        }

        return (ax, ay, az, hasAnyValue);
    }

    private static string FormatAccelerationValue(double? value)
    {
        return value.HasValue
            ? value.Value.ToString("0.###", CultureInfo.InvariantCulture)
            : "null";
    }

    public IReadOnlyDictionary<string, object>? GetControlState()
    {
        lock (_connectionStateLock)
        {
            return _lastControlState == null
                ? null
                : new Dictionary<string, object>(_lastControlState, StringComparer.OrdinalIgnoreCase);
        }
    }

    private void SetLastControlState(IReadOnlyDictionary<string, object> state)
    {
        lock (_connectionStateLock)
        {
            _lastControlState = new Dictionary<string, object>(state, StringComparer.OrdinalIgnoreCase);

            /*
            // Otomatik acil durum latch'i UI'a controlState içinde gönderilecek.
            if (_autoEmergencyActive)
                ((Dictionary<string, object>)_lastControlState)["autoEmergencyActive"] = true;
            */
        }
    }

    /*
    private async Task EvaluateAutoEmergencyGuard(string telemetryLine)
    {
        UpdateAutoEmergencyInputs(telemetryLine);

        if (_autoEmergencyActive || _autoEmergencyCommandSent || !ShouldTriggerAutoEmergency())
            return;

        _autoEmergencyActive = true;
        _autoEmergencyCommandSent = true;
        _logger.CommandError("Otomatik acil durum tetiklendi: I3 < 5 ve hareket aktif.");

        await SendCommand(CommandNames.Emergency);
        await PublishAutoEmergencyControlState();
    }

    private void UpdateAutoEmergencyInputs(string telemetryLine)
    {
        var separatorIndex = telemetryLine.IndexOf('|');
        var deviceId = separatorIndex > 0 ? telemetryLine[..separatorIndex] : "SYSTEM";
        var payload = separatorIndex >= 0 ? telemetryLine[(separatorIndex + 1)..] : telemetryLine;

        var values = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        foreach (var field in payload.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var pair = field.Split(':', 2, StringSplitOptions.TrimEntries);
            if (pair.Length != 2 || !double.TryParse(pair[1], NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                continue;

            values[pair[0]] = value;
        }

        if (values.TryGetValue("I3", out var i3))
            _latestI3 = i3;

        if (deviceId == "2" &&
            values.TryGetValue("F", out var f) &&
            values.TryGetValue("B", out var b) &&
            values.TryGetValue("BR", out var br) &&
            values.TryGetValue("E", out var e))
        {
            _device2AutonomousForward = f == 1 && b == 0 && br == 0 && e == 0;
            _device2AutonomousBackward = f == 0 && b == 1 && br == 0 && e == 0;
        }
    }

    private bool ShouldTriggerAutoEmergency()
    {
        if (_latestI3 is null || _latestI3 >= AutoEmergencyCurrentThreshold)
            return false;

        var manualForward = _lastControlState?.TryGetValue("forward", out var forward) == true && forward is true;
        var manualBackward = _lastControlState?.TryGetValue("backward", out var backward) == true && backward is true;
        var autonomousMoving = _device2AutonomousForward == true || _device2AutonomousBackward == true;

        return manualForward || manualBackward || autonomousMoving;
    }

    private async Task PublishAutoEmergencyControlState()
    {
        var state = GetControlState() == null
            ? new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, object>(GetControlState()!, StringComparer.OrdinalIgnoreCase);

        state["autoEmergencyActive"] = true;
        state["emergency"] = true;
        SetLastControlState(state);
        await _hub.Clients.All.SendAsync("controlState", state);
    }

    private async Task ClearAutoEmergencyLatch()
    {
        _autoEmergencyActive = false;
        _autoEmergencyCommandSent = false;

        var state = GetControlState() == null
            ? new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, object>(GetControlState()!, StringComparer.OrdinalIgnoreCase);

        state["autoEmergencyActive"] = false;
        SetLastControlState(state);
        await _hub.Clients.All.SendAsync("controlState", state);
    }
    */

    private async Task HeartbeatLoop(TcpClient raspberryClient, StreamWriter writer, int connectionId, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                if (!IsCurrentConnection(connectionId))
                    break;

                DateTime lastPongTime;
                lock (_connectionStateLock)
                {
                    lastPongTime = _lastPongTime;
                }

                if (lastPongTime != DateTime.MinValue && DateTime.UtcNow - lastPongTime > RaspberryWatchdogTimeout)
                {
                    _logger.TcpError("Watchdog timeout! Raspberry cevap vermiyor.");
                    await SetConnectionStateForConnection(connectionId, false, "watchdog_timeout");
                    raspberryClient.Close();
                    break;
                }

                var semaphoreTaken = false;
                try
                {
                    await _socketSemaphore.WaitAsync(token);
                    semaphoreTaken = true;
                    await writer.WriteLineAsync("PING");
                }
                catch
                {
                    _logger.TcpError("PING gönderilemedi.");
                    await SetConnectionStateForConnection(connectionId, false, "ping_send_error");
                    raspberryClient.Close();
                    break;
                }
                finally
                {
                    if (semaphoreTaken)
                        _socketSemaphore.Release();
                }

                var publishHealth = false;
                lock (_connectionStateLock)
                {
                    if (DateTime.UtcNow - _lastHealthPublishTimeUtc >= TimeSpan.FromSeconds(1))
                    {
                        _lastHealthPublishTimeUtc = DateTime.UtcNow;
                        publishHealth = true;
                    }
                }

                if (publishHealth)
                    _ = PublishCurrentConnectionStatusSafely();

                await Task.Delay(HeartbeatInterval, token);
            }
        }
        catch (TaskCanceledException)
        {

        }
    }

    public object GetConnectionStatus()
    {
        lock (_connectionStateLock)
        {
            var now = DateTime.UtcNow;
            var devices = _deviceHealth.ToDictionary(
                pair => pair.Key,
                pair => (object)new
                {
                    receivedPackets = pair.Value.ReceivedPackets,
                    droppedPackets = pair.Value.DroppedPackets,
                    outOfOrderPackets = pair.Value.OutOfOrderPackets,
                    lastSequence = pair.Value.LastSequence,
                    lastPacketTime = pair.Value.LastPacketTimeUtc,
                    frequencyHz = pair.Value.ReceivedPackets > 1
                        ? Math.Round((pair.Value.ReceivedPackets - 1) /
                            Math.Max((pair.Value.LastPacketTimeUtc - pair.Value.FirstPacketTimeUtc).TotalSeconds, 0.001), 2)
                        : 0,
                    receiving = now - pair.Value.LastPacketTimeUtc < TimeSpan.FromSeconds(2)
                });

            return new
            {
                raspberryConnected = _isRaspberryConnected,
                lastPongTime = _lastPongTime == DateTime.MinValue ? (DateTime?)null : _lastPongTime,
                lastTelemetryTime = _lastTelemetryTime == DateTime.MinValue ? (DateTime?)null : _lastTelemetryTime,
                reason = _disconnectReason,
                devices,
                time = DateTime.Now
            };
        }
    }

    private void UpdateDeviceHealth(string line, DateTime receivedAtUtc)
    {
        var separatorIndex = line.IndexOf('|');
        var deviceId = separatorIndex > 0 ? line[..separatorIndex] : "SYSTEM";
        var payload = separatorIndex >= 0 ? line[(separatorIndex + 1)..] : line;
        long? sequence = null;

        foreach (var field in payload.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var pair = field.Split(':', 2, StringSplitOptions.TrimEntries);
            if (pair.Length == 2 && pair[0].Equals("_SEQ", StringComparison.OrdinalIgnoreCase) &&
                long.TryParse(pair[1], out var parsedSequence))
            {
                sequence = parsedSequence;
                break;
            }
        }

        if (!_deviceHealth.TryGetValue(deviceId, out var health))
        {
            health = new DeviceHealthState { FirstPacketTimeUtc = receivedAtUtc };
            _deviceHealth[deviceId] = health;
        }

        health.ReceivedPackets++;
        health.LastPacketTimeUtc = receivedAtUtc;

        if (sequence.HasValue && health.LastSequence.HasValue)
        {
            if (sequence.Value > health.LastSequence.Value + 1)
                health.DroppedPackets += sequence.Value - health.LastSequence.Value - 1;
            else if (sequence.Value <= health.LastSequence.Value)
                health.OutOfOrderPackets++;
        }

        if (sequence.HasValue && (!health.LastSequence.HasValue || sequence.Value > health.LastSequence.Value))
            health.LastSequence = sequence.Value;
    }

    public Task PublishCurrentConnectionStatus()
    {
        return _hub.Clients.All.SendAsync("connectionStatus", GetConnectionStatus());
    }

    private async Task PublishCurrentConnectionStatusSafely()
    {
        try
        {
            await PublishCurrentConnectionStatus();
        }
        catch (Exception ex)
        {
            _logger.TcpError("Bağlantı durumu yayınlanamadı: " + ex.Message);
        }
    }

    private bool IsCurrentConnection(int connectionId)
    {
        lock (_connectionStateLock)
        {
            return connectionId == _activeConnectionId;
        }
    }

    private Task SetConnectionStateForConnection(int connectionId, bool connected, string reason)
    {
        return IsCurrentConnection(connectionId)
            ? SetConnectionState(connected, reason)
            : Task.CompletedTask;
    }

    private async Task SetConnectionState(bool connected, string reason)
    {
        lock (_connectionStateLock)
        {
            var previousConnected = _isRaspberryConnected;
            var previousReason = _disconnectReason;

            _isRaspberryConnected = connected;
            if (connected || previousConnected || _disconnectReason == "not_connected")
            {
                _disconnectReason = reason;
            }

            if (connected && _lastPongTime == DateTime.MinValue)
            {
                _lastPongTime = DateTime.Now;
            }

            if (previousConnected == _isRaspberryConnected && previousReason == _disconnectReason)
            {
                return;
            }
        }

        await PublishCurrentConnectionStatus();
    }

}
