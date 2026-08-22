using System.IO;

namespace BackendServer.Services;

public class LoggingService
{
    private const string SystemCategory = "system";
    private const string TcpCategory = "tcp";
    private const string TelemetryCategory = "telemetry";
    private const string CommandCategory = "commands";
    private const string SignalRCategory = "signalr";
    private const string ErrorCategory = "errors";

    private readonly string logDirectory;
    private readonly object _lock = new object();

    public LoggingService()
    {
        var basePath = AppContext.BaseDirectory; // Uygulamanın çalıştığı dizin
        logDirectory = Path.Combine(basePath, "Logs");

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
        WriteLog(TelemetryCategory, "INFO", message);
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

    private void WriteLog(string category, string level, string message)
    {
        var now = DateTime.Now;
        var log = $"{now:yyyy-MM-dd HH:mm:ss.fff} [{level}] {message}";

        lock (_lock)
        {
            Console.WriteLine(log);

            var dailyLogDirectory = Path.Combine(logDirectory, now.ToString("yyyy-MM-dd"));
            Directory.CreateDirectory(dailyLogDirectory);

            File.AppendAllText(GetLogFilePath(dailyLogDirectory, category), log + Environment.NewLine);

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
}
