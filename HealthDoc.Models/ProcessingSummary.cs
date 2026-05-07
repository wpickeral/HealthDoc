using System.Text.Json.Serialization;

namespace HealthDoc.Models;

/// Summary written to Cosmos DB after Fan-in
public class ProcessingSummary
{
    [JsonPropertyName("id")] public string Id { get; set; } = Guid.NewGuid().ToString();
    public string ClinicId { get; set; } = string.Empty;
    public string BatchId { get; set; } = string.Empty;
    public int TotalRecords { get; set; }
    public int AbnormalCount { get; set; }
    public DateTime ProcessedAt { get; set; }

    public string Status { get; set; } = string.Empty;

    // Owned by the Monitor pattern — StoreSummary always sets Unknown
    public ConfirmationStatus ConfirmationStatus { get; set; } = ConfirmationStatus.Unknown;

    public List<string> Errors { get; set; } = [];
}