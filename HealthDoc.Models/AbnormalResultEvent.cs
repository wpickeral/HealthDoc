namespace HealthDoc.Models;

/// Custom Event Grid event data published when a batch contains abnormal results.
/// Sent to the custom topic so any subscriber (webhook, another function, Logic App)
/// can react without knowing about the internal Durable Functions pipeline.
public class AbnormalResultEvent
{
    public string   BatchId       { get; set; } = string.Empty;
    public string   ClinicId      { get; set; } = string.Empty;
    public int      AbnormalCount { get; set; }
    public int      TotalRecords  { get; set; }
    public DateTime DetectedAt    { get; set; }
}
