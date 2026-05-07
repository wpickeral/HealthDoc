using System.Text.Json.Serialization;

namespace HealthDoc.Models;

/// One processed record after ProcessRecord activity
public class ProcessedRecord : LabRecord
{
    [JsonPropertyName("id")] public string Id { get; set; } = string.Empty;
    public bool IsAbnormal { get; set; }
    public DateTime ProcessedAt { get; set; }
    public string Status { get; set; } = string.Empty;

    public static ProcessedRecord From(LabRecord record)
    {
        var parts = record.ReferenceRange.Split('-');
        var min = double.Parse(parts[0]);
        var max = double.Parse(parts[1]);

        return new ProcessedRecord
        {
            Id             = $"{record.ClinicId}-{record.PatientId}-{record.TestCode}-{DateTime.UtcNow:yyyyMMdd}",
            ClinicId       = record.ClinicId,
            PatientId      = record.PatientId,
            TestCode       = record.TestCode,
            Result         = record.Result,
            Unit           = record.Unit,
            ReferenceRange = record.ReferenceRange,
            IsAbnormal     = record.Result < min || record.Result > max,
            CollectedAt    = record.CollectedAt,
            ProcessedAt    = DateTime.UtcNow,
            Status         = "Processed"
        };
    }
}