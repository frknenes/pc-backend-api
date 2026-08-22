using Microsoft.AspNetCore.Mvc;
using BackendServer.Services;
using BackendServer.Models;

namespace BackendServer.Controllers;

[ApiController]
[Route("api/command")]
public class CommandController : ControllerBase
{
    private readonly CommandService _commandService;
    private readonly LoggingService _logger;

    public CommandController(CommandService commandService, LoggingService logger)
    {
        _commandService = commandService;
        _logger = logger;
    }

    // React UI -> Backend
    // POST: api/command
    [HttpPost]
    public async Task<IActionResult> SendCommand([FromBody] CommandData commandData)
    {
        if (commandData == null || string.IsNullOrWhiteSpace(commandData.Command))
        {
            return BadRequest("Komut boş olamaz.");
        }

        var cmd = commandData.Command.ToUpperInvariant();

        if (!CommandNames.All.Contains(cmd))
        {
            _logger.CommandError($"Bilinmeyen komut: {cmd}");
            return BadRequest("Bilinmeyen komut");
        }
        
        _logger.CommandInfo($"HTTP Command alındı: {cmd}");
        
        var sent = await _commandService.SendCommand(cmd);
        if (!sent)
        {
            return StatusCode(StatusCodes.Status503ServiceUnavailable, new
            {
                status = "ERROR",
                message = "Raspberry bağlı değil veya komut gönderilemedi.",
                sentCommand = cmd,
                time = DateTime.Now
            });
        }
        
        return Ok(new
        {
            status = "OK",
            sentCommand = cmd,
            time = DateTime.Now
        });
    }
}
