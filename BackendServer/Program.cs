using BackendServer.Hubs;
using BackendServer.Services;

var builder = WebApplication.CreateBuilder(args);

// ------------------- Servisler -------------------
builder.Services.AddControllers();
builder.Services.AddSignalR();

builder.Services.AddSingleton<LoggingService>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<LoggingService>());
builder.Services.AddSingleton<TelemetryService>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<TelemetryService>());
builder.Services.AddSingleton<TcpServerService>();
builder.Services.AddSingleton<CommandService>();

//var logger = new LoggingService();
//logger.Info("Test log yazıldı");

// ------------------- CORS -------------------
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowUi", policy =>
    {
        policy
            // Development UI can run from Expo Go, Metro or Expo Web on the LAN.
            .SetIsOriginAllowed(_ => builder.Environment.IsDevelopment())
            .AllowAnyHeader()
            .AllowAnyMethod()
            .AllowCredentials(); 
    });
});

var app = builder.Build();

// -------------------- Middleware -------------------
app.UseRouting();
app.UseCors("AllowUi");

app.MapControllers();
app.MapHub<TelemetryHub>("/telemetry");

// -------------------- TCP Server --------------------
var tcpServer = app.Services.GetRequiredService<TcpServerService>();
tcpServer.Start();

app.Run();
