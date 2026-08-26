using System.Net;
using System.Net.Sockets;
using System.Text;
using BackendServer.Hubs;
using BackendServer.Models;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;

namespace BackendServer.Services;

public class TcpServerService
{
    private TcpListener? _listener;
    private TcpClient? _raspberryClient;
    private StreamWriter? _writer;
    private StreamReader? _reader;
    private DateTime _lastPongTime = DateTime.MinValue;
    private DateTime _lastTelemetryTime = DateTime.MinValue;
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
    private static readonly TimeSpan CommandAckTimeout = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan RaspberryWatchdogTimeout = TimeSpan.FromSeconds(10);
    private IReadOnlyDictionary<string, object>? _lastControlState;

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

                _logger.TelemetryInfo("RX: " + line);
                lock (_connectionStateLock)
                {
                    _lastTelemetryTime = DateTime.Now;
                }
                await _telemetryService.HandleRawTelemetry(line);
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

        _logger.CommandInfo("TX: " + msg);
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
        }
    }

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

                await Task.Delay(500, token);
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
            return new
            {
                raspberryConnected = _isRaspberryConnected,
                lastPongTime = _lastPongTime == DateTime.MinValue ? (DateTime?)null : _lastPongTime,
                lastTelemetryTime = _lastTelemetryTime == DateTime.MinValue ? (DateTime?)null : _lastTelemetryTime,
                reason = _disconnectReason,
                time = DateTime.Now
            };
        }
    }

    public Task PublishCurrentConnectionStatus()
    {
        return _hub.Clients.All.SendAsync("connectionStatus", GetConnectionStatus());
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
