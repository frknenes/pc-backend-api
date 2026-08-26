using BackendServer.Models;

namespace BackendServer.Services;

public class CommandService
{
    private readonly TcpServerService _tcpServer;
    private readonly LoggingService _logger;

    public CommandService(TcpServerService tcpServer, LoggingService logger)
    {
        _tcpServer = tcpServer;
        _logger = logger;
    }

    public async Task<CommandExecutionResult> SendCommand(string command)
    {
        _logger.CommandInfo("UI'dan komut alındı: " + command);
        return await _tcpServer.SendCommand(command);
    }
}
