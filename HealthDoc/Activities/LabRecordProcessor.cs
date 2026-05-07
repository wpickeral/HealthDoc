using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;

namespace HealthDoc.Activities;

public abstract class ProcessRecord
{
    [Function("ProcessRecord")]
    public static ProcessedRecord Process([ActivityTrigger] LabRecord record)
    {
        // Parse reference range e.g. "4.0-5.6"
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