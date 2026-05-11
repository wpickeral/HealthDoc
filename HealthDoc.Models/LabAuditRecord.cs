using System.Text.Json.Serialization;

namespace HealthDoc.Models;

/// Written to the AuditLog Cosmos container by EventGridLabResultAuditor
/// whenever a blob lands in lab-results-incoming via an Event Grid system event.
/// Provides an immutable audit trail independent of the processing pipeline.
public class LabAuditRecord
{
    [JsonPropertyName("id")] public string Id        { get; set; } = Guid.NewGuid().ToString();
    public string   ClinicId   { get; set; } = string.Empty;
    public string   FileName   { get; set; } = string.Empty;
    public string   EventType  { get; set; } = string.Empty;
    public string   BlobUrl    { get; set; } = string.Empty;
    public DateTime ReceivedAt { get; set; }
}
