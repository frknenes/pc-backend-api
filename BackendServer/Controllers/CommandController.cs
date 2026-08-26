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
        
        var result = await _commandService.SendCommand(cmd);
        if (!result.Success)
        {
            var statusCode = result.Reason switch
            {
                "not_connected" or "send_error" or "ack_timeout" => StatusCodes.Status503ServiceUnavailable,
                "execution_error" => StatusCodes.Status500InternalServerError,
                _ => StatusCodes.Status409Conflict
            };
            return StatusCode(statusCode, new
            {
                status = "ERROR",
                message = result.Reason,
                commandId = result.CommandId,
                sentCommand = cmd,
                controlState = result.ControlState,
                time = DateTime.Now
            });
        }
        
        return Ok(new
        {
            status = "OK",
            commandId = result.CommandId,
            sentCommand = cmd,
            controlState = result.ControlState,
            time = DateTime.Now
        });
    }
}
