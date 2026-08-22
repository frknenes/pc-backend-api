namespace BackendServer.Models;

public class CommandData
{
    // Komut tipi 
    public string Command { get; set; } = string.Empty;
    
    // Komut kaynağı
    public string Source { get; set; } = "UI";
    
    // Komut gönderim zamanı
    public DateTime Timestamp { get; set; } = DateTime.Now;
    
}
