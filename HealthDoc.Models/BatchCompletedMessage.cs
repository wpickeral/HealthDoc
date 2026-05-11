namespace HealthDoc.Models;

public class BatchCompletedMessage
{
    public string   BatchId       { get; set; } = string.Empty;
    public string   ClinicId      { get; set; } = string.Empty;
    public int      TotalRecords  { get; set; }
    public int      AbnormalCount { get; set; }
    public DateTime ProcessedAt   { get; set; }
}
