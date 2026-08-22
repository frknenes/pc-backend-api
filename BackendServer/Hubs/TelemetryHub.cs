using BackendServer.Services;
using Microsoft.AspNetCore.SignalR;

namespace BackendServer.Hubs;

public class TelemetryHub : Hub
{
    private readonly LoggingService _logger;

    public TelemetryHub(LoggingService logger)
    {
        _logger = logger;
    }

    public override async Task OnConnectedAsync()
    {
        _logger.SignalRInfo($"UI bağlandı: {Context.ConnectionId}");
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.SignalRInfo($"UI ayrıldı: {Context.ConnectionId}");
        await base.OnDisconnectedAsync(exception);
    }
}
