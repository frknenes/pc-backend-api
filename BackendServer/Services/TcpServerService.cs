using System.Net;
using System.Net.Sockets;
using System.Text;
using BackendServer.Hubs;
using Microsoft.AspNetCore.SignalR;

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
                    _lastPongTime = DateTime.Now;
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

                if (line == "PONG")
                {
                    lock (_connectionStateLock)
                    {
                        _lastPongTime = DateTime.Now;
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

            if (IsCurrentConnection(connectionId))
            {
                _heartbeatCts = null;
                _reader = null;
                _writer = null;
                _raspberryClient = null;
                lock (_connectionStateLock)
                {
                    _lastPongTime = DateTime.MinValue;
                }
                await SetConnectionState(false, _disconnectReason == "connected" ? "connection_cleaned" : _disconnectReason);

                _logger.TcpInfo("Bağlantı temizlendi.");
            }
        }
    }

    public async Task<bool> SendCommand(string command)
    {
        if (_raspberryClient == null || _writer == null)
        {
            _logger.CommandError("Raspberry bağlı değil, komut gönderilemedi.");
            return false;
        }

        var msg = ("CMD|" + command).TrimStart('\uFEFF');

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
            return false;
        }
        finally
        {
            _socketSemaphore.Release();
        }

        _logger.CommandInfo("TX: " + msg);
        return true;
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

                if (lastPongTime != DateTime.MinValue && (DateTime.Now - lastPongTime).TotalSeconds > 3)
                {
                    _logger.TcpError("Watchdog timeout! Raspberry cevap vermiyor.");
                    await SetConnectionStateForConnection(connectionId, false, "watchdog_timeout");
                    raspberryClient.Close();
                    break;
                }

                try
                {
                    await _socketSemaphore.WaitAsync(token);
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
