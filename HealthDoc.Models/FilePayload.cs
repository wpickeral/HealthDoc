namespace HealthDoc.Models;

/// Input to the orchestrator
public class FilePayload
{
    public string ClinicId  { get; set; } = string.Empty;
    public string FileName  { get; set; } = string.Empty;
    public string Content   { get; set; } = string.Empty;
}