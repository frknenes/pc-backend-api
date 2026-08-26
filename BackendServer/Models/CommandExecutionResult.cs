namespace BackendServer.Models;

public record CommandExecutionResult(
    bool Success,
    string CommandId,
    string Command,
    string Reason,
    IReadOnlyDictionary<string, object>? ControlState = null);
