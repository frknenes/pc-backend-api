using System.Net;
using System.Net.Sockets;
using System.Text;

namespace BackendServer.Services;

public class TcpServerService
{
    private TcpListener _listener;
    private TcpClient? _raspberryClient;
    private StreamWriter? _writer;
    private StreamReader? _reader;
    private Timer? _pingTimer;
    private DateTime _lastPongTime = DateTime.MinValue;
    private CancellationTokenSource? _heartbeatCts;

    private readonly TelemetryService _telemetryService;
    private readonly LoggingService _logger;
    private readonly SemaphoreSlim _socketSemaphore = new(1, 1);

    public TcpServerService(TelemetryService telemetryService, LoggingService logger)
    {
        _telemetryService = telemetryService;
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
        while (true)
        {
            _raspberryClient = await _listener.AcceptTcpClientAsync();

            _lastPongTime = DateTime.Now;

            var stream = _raspberryClient.GetStream();
            _reader = new StreamReader(stream, new UTF8Encoding(false));
            _writer = new StreamWriter(stream, new UTF8Encoding(false)) { AutoFlush = true };
            _heartbeatCts = new CancellationTokenSource();
            _ = Task.Run(() => HeartbeatLoop(_heartbeatCts.Token));

            _logger.TcpInfo("Raspberry Pi bağlandı.");

            _ = Task.Run(ListenClientLoop);
        }
    }

    private async Task ListenClientLoop()
    {
        try
        {
            while (true)
            {
                if (_reader == null) break;

                var line = await _reader.ReadLineAsync();
                if (line == null)
                {
                    _logger.TcpError("Raspberry bağlantısı koptu.");
                    break;
                }

                line = line.TrimStart('\uFEFF');

                if (line == "PONG")
                {
                    _lastPongTime = DateTime.Now;
                    continue;
                }

                _logger.TelemetryInfo("RX: " + line);
                await _telemetryService.HandleRawTelemetry(line);
            }
        }
        catch (Exception ex)
        {
            _logger.TcpError("TCP okuma hatası: " + ex.Message);
        }
        finally
        {
            _reader?.Close();
            _writer?.Close();
            _raspberryClient?.Close();

            _heartbeatCts?.Cancel();
            _heartbeatCts = null;

            _reader = null;
            _writer = null;
            _raspberryClient = null;
            _pingTimer = null;
            _lastPongTime = DateTime.MinValue;

            _logger.TcpInfo("Bağlantı temizlendi.");
        }
    }

    public async Task SendCommand(string command)
    {
        if (_raspberryClient == null || _writer == null)
        {
            _logger.CommandError("Raspberry bağlı değil, komut gönderilemedi.");
            return;
        }

        var msg = ("CMD|" + command).TrimStart('\uFEFF');

        await _socketSemaphore.WaitAsync();
        try
        {
            await _writer.WriteLineAsync(msg);
        }
        finally
        {
            _socketSemaphore.Release();
        }

        _logger.CommandInfo("TX: " + msg);
    }

    private async Task HeartbeatLoop(CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                if (_writer == null || _raspberryClient == null)
                    break;

                if (_lastPongTime != DateTime.MinValue && (DateTime.Now - _lastPongTime).TotalSeconds > 3)
                {
                    _logger.TcpError("Watchdog timeout! Raspberry cevap vermiyor.");
                    _raspberryClient?.Close();
                    break;
                }

                try
                {
                    await _socketSemaphore.WaitAsync(token);
                    await _writer.WriteLineAsync("PING");
                }
                catch
                {
                    _logger.TcpError("PING gönderilemedi.");
                    _raspberryClient?.Close();
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

}
