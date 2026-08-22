using BackendServer.Services;
using Microsoft.AspNetCore.SignalR;

namespace BackendServer.Hubs;

public class TelemetryHub : Hub
{
    private readonly LoggingService _logger;
    private readonly TcpServerService _tcpServer;

    public TelemetryHub(LoggingService logger, TcpServerService tcpServer)
    {
        _logger = logger;
        _tcpServer = tcpServer;
    }

    public override async Task OnConnectedAsync()
    {
        _logger.SignalRInfo($"UI bağlandı: {Context.ConnectionId}");
        await base.OnConnectedAsync();
        await Clients.Caller.SendAsync("connectionStatus", _tcpServer.GetConnectionStatus());
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.SignalRInfo($"UI ayrıldı: {Context.ConnectionId}");
        await base.OnDisconnectedAsync(exception);
    }
}
