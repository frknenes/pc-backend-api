using System.Globalization;
using System.IO;
using System.Threading.Channels;

namespace BackendServer.Services;

public class LoggingService : IHostedService
{
    private const long DefaultTelemetryMaxFileBytes = 5 * 1024 * 1024;
    private const int DefaultRetentionDays = 7;

    private const string SystemCategory = "system";
    private const string TcpCategory = "tcp";
    private const string TelemetryCategory = "telemetry";
    private const string CommandCategory = "commands";
    private const string SignalRCategory = "signalr";
    private const string ErrorCategory = "errors";

    private readonly string logDirectory;
    private readonly long telemetryMaxFileBytes;
    private readonly int retentionDays;
    private readonly object _lock = new object();
    private string? currentTelemetryLogDirectory;
    private string? currentTelemetryLogFilePath;
    private long currentTelemetryLogFileBytes;
    private DateTime _lastCleanupDate = DateTime.MinValue;
    private readonly Channel<QueuedLog> _telemetryQueue = Channel.CreateBounded<QueuedLog>(
        new BoundedChannelOptions(10_000)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropOldest
        });
    private Task? _telemetryWriterTask;

    private readonly record struct QueuedLog(
        DateTime Timestamp,
        string Category,
        string Level,
        string Message,
        bool WriteToConsole);

    public LoggingService(IConfiguration configuration)
    {
        var basePath = AppContext.BaseDirectory; // Uygulamanın çalıştığı dizin
        logDirectory = Path.Combine(basePath, "Logs");
        telemetryMaxFileBytes = Math.Max(
            1,
            configuration.GetValue<long?>("Logging:TelemetryMaxFileSizeMB") ?? DefaultTelemetryMaxFileBytes / 1024 / 1024) * 1024 * 1024;
        retentionDays = Math.Max(
            1,
            configuration.GetValue<int?>("Logging:FileRetentionDays") ?? DefaultRetentionDays);

        Directory.CreateDirectory(logDirectory); // Yoksa oluşturur
    }

    public void Info(string message)
    {
        WriteLog(SystemCategory, "INFO", message);
    }

    public void Error(string message)
    {
        WriteLog(SystemCategory, "ERROR", message);
    }

    public void TcpInfo(string message)
    {
        WriteLog(TcpCategory, "INFO", message);
    }

    public void TcpError(string message)
    {
        WriteLog(TcpCategory, "ERROR", message);
    }

    public void TelemetryInfo(string message)
    {
        // TCP okuma döngüsünü disk I/O ile bekletme. Kuyruk dolarsa en eski
        // telemetri logu düşer; canlı veri işleme hiçbir zaman durmaz.
        _telemetryQueue.Writer.TryWrite(new QueuedLog(DateTime.Now, TelemetryCategory, "INFO", message, false));
    }

    public void TelemetryError(string message)
    {
        WriteLog(TelemetryCategory, "ERROR", message);
    }

    public void CommandInfo(string message)
    {
        WriteLog(CommandCategory, "INFO", message);
    }

    public void CommandError(string message)
    {
        WriteLog(CommandCategory, "ERROR", message);
    }

    public void SignalRInfo(string message)
    {
        WriteLog(SignalRCategory, "INFO", message);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _telemetryWriterTask = Task.Run(ProcessTelemetryLogsAsync, CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _telemetryQueue.Writer.TryComplete();
        if (_telemetryWriterTask != null)
            await _telemetryWriterTask.WaitAsync(cancellationToken);
    }

    private async Task ProcessTelemetryLogsAsync()
    {
        await foreach (var entry in _telemetryQueue.Reader.ReadAllAsync())
            WriteLog(entry.Category, entry.Level, entry.Message, entry.Timestamp, entry.WriteToConsole);
    }

    private void WriteLog(
        string category,
        string level,
        string message,
        DateTime? timestamp = null,
        bool writeToConsole = true)
    {
        var now = timestamp ?? DateTime.Now;
        var log = $"{now:yyyy-MM-dd HH:mm:ss.fff} [{level}] {message}";

        lock (_lock)
        {
            if (writeToConsole)
                Console.WriteLine(log);

            var dailyLogDirectory = Path.Combine(logDirectory, now.ToString("yyyy-MM-dd"));
            Directory.CreateDirectory(dailyLogDirectory);
            CleanupOldDailyLogDirectories(now);

            var logFilePath = category == TelemetryCategory
                ? GetTelemetryLogFilePath(dailyLogDirectory)
                : GetLogFilePath(dailyLogDirectory, category);

            File.AppendAllText(logFilePath, log + Environment.NewLine);
            if (category == TelemetryCategory)
            {
                currentTelemetryLogDirectory = dailyLogDirectory;
                currentTelemetryLogFilePath = logFilePath;
                currentTelemetryLogFileBytes = new FileInfo(logFilePath).Length;
            }

            if (level == "ERROR" && category != ErrorCategory)
            {
                File.AppendAllText(GetLogFilePath(dailyLogDirectory, ErrorCategory), log + Environment.NewLine);
            }
        }
    }

    private static string GetLogFilePath(string dailyLogDirectory, string category)
    {
        return Path.Combine(dailyLogDirectory, category + ".log");
    }

    private string GetTelemetryLogFilePath(string dailyLogDirectory)
    {
        if (currentTelemetryLogDirectory == dailyLogDirectory &&
            currentTelemetryLogFilePath is not null &&
            currentTelemetryLogFileBytes < telemetryMaxFileBytes)
        {
            return currentTelemetryLogFilePath;
        }

        if (currentTelemetryLogDirectory == dailyLogDirectory &&
            currentTelemetryLogFilePath is not null)
        {
            var currentIndex = ParseTelemetryLogIndex(currentTelemetryLogFilePath);
            if (currentIndex > 0)
            {
                currentTelemetryLogFileBytes = 0;
                return GetTelemetryLogFilePath(dailyLogDirectory, currentIndex + 1);
            }
        }

        var files = Directory
            .EnumerateFiles(dailyLogDirectory, TelemetryCategory + "_*.log")
            .Select(file => new
            {
                Path = file,
                Index = ParseTelemetryLogIndex(file),
                Size = new FileInfo(file).Length
            })
            .Where(file => file.Index > 0)
            .OrderByDescending(file => file.Index)
            .ToList();

        var latestFile = files.FirstOrDefault();
        if (latestFile is null)
        {
            return GetTelemetryLogFilePath(dailyLogDirectory, 1);
        }

        return latestFile.Size < telemetryMaxFileBytes
            ? latestFile.Path
            : GetTelemetryLogFilePath(dailyLogDirectory, latestFile.Index + 1);
    }

    private static string GetTelemetryLogFilePath(string dailyLogDirectory, int index)
    {
        return Path.Combine(dailyLogDirectory, $"{TelemetryCategory}_{index:000}.log");
    }

    private static int ParseTelemetryLogIndex(string filePath)
    {
        var fileName = Path.GetFileNameWithoutExtension(filePath);
        var prefix = TelemetryCategory + "_";

        return fileName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) &&
               int.TryParse(fileName[prefix.Length..], NumberStyles.None, CultureInfo.InvariantCulture, out var index)
            ? index
            : 0;
    }

    private void CleanupOldDailyLogDirectories(DateTime now)
    {
        if (_lastCleanupDate.Date == now.Date)
        {
            return;
        }

        _lastCleanupDate = now.Date;
        var oldestDateToKeep = now.Date.AddDays(-(retentionDays - 1));

        foreach (var directory in Directory.EnumerateDirectories(logDirectory))
        {
            var directoryName = Path.GetFileName(directory);
            if (!DateTime.TryParseExact(directoryName, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var directoryDate) ||
                directoryDate.Date >= oldestDateToKeep)
            {
                continue;
            }

            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{now:yyyy-MM-dd HH:mm:ss.fff} [ERROR] Eski log klasoru silinemedi: {directoryName} - {ex.Message}");
            }
        }
    }
}
