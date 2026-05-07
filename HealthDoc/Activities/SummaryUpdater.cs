using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Activities;

public class SummaryUpdater
{
    private readonly ILogger<SummaryUpdater> _logger;

    public SummaryUpdater(ILogger<SummaryUpdater> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Aggregates all processed records into a batch-level summary and persists it to
    /// Cosmos DB via output binding. Records the total record count and how many results
    /// were flagged as abnormal, giving clinic staff a quick overview without querying
    /// individual records. Sets <see cref="ConfirmationStatus.Unknown"/> so the monitor
    /// pattern can verify the write completed before the orchestration finishes.
    /// </summary>
    [Function(AppConfig.Activities.StoreSummary)]
    [CosmosDBOutput(AppConfig.CosmosDb.Database, AppConfig.CosmosDb.SummariesContainer,
        Connection = AppConfig.CosmosDb.Connection)]
    public ProcessingSummary SaveSummary([ActivityTrigger] ProcessedRecord[] records)
    {
        var batchId = Guid.NewGuid().ToString();

        var summary = new ProcessingSummary
        {
            Id                 = batchId,
            BatchId            = batchId,
            ClinicId           = records.First().ClinicId,
            TotalRecords       = records.Length,
            AbnormalCount      = records.Count(r => r.IsAbnormal),
            ProcessedAt        = DateTime.UtcNow,
            Status             = "Complete",
            ConfirmationStatus = ConfirmationStatus.Unknown
        };

        _logger.LogInformation(
            "Batch {BatchId} summary created — {TotalRecords} records, {AbnormalCount} abnormal for clinic {ClinicId}",
            summary.BatchId, summary.TotalRecords, summary.AbnormalCount, summary.ClinicId);

        return summary;
    }
}
