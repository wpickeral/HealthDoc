using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;

namespace HealthDoc.Activities;

public abstract class StoreSummary
{
    [Function("StoreSummary")]
    [CosmosDBOutput("LabResults", "ProcessingSummaries",
        Connection = "CosmosDBConnectionString")]
    public static ProcessingSummary SaveSummary([ActivityTrigger] ProcessedRecord[] records)
    {
        var batchId = Guid.NewGuid().ToString();

        return new ProcessingSummary
        {
            Id = batchId,
            BatchId = batchId,
            ClinicId = records.First().ClinicId,
            TotalRecords = records.Length,
            AbnormalCount = records.Count(r => r.IsAbnormal),
            ProcessedAt = DateTime.UtcNow,
            Status = "Complete",
            ConfirmationStatus = ConfirmationStatus.Unknown  // monitor hasn't run yet
        };
    }
}