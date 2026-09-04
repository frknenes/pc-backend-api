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

        // Tarayıcı yenilendiğinde bellekteki eski state yerine Raspberry Pi'nin
        // o andaki GPIO/VFD durumunu iste. GET_STATE donanımı değiştirmez.
        var stateResult = await _tcpServer.SendCommand("GET_STATE");
        if (stateResult.ControlState != null)
            await Clients.Caller.SendAsync("controlState", stateResult.ControlState);
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.SignalRInfo($"UI ayrıldı: {Context.ConnectionId}");
        await base.OnDisconnectedAsync(exception);
    }
}
