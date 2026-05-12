using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Pipeline;

public class TimeoutSummaryWriter
{
    private readonly ILogger<TimeoutSummaryWriter> _logger;

    public TimeoutSummaryWriter(ILogger<TimeoutSummaryWriter> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Persists a timed-out batch summary back to Cosmos DB when the monitor pattern
    /// exhausts its retry attempts without confirming storage. Separating this write
    /// into an activity keeps the orchestrator free of direct I/O, which is required
    /// for deterministic replay.
    /// </summary>
    [Function(AppConfig.Activities.WriteTimeoutSummary)]
    [CosmosDBOutput(AppConfig.CosmosDb.Database, AppConfig.CosmosDb.SummariesContainer,
        Connection = AppConfig.CosmosDb.Connection)]
    public ProcessingSummary Write([ActivityTrigger] ProcessingSummary summary)
    {
        _logger.LogWarning("Batch {BatchId} — persisting timed-out status to Cosmos", summary.BatchId);
        return summary;
    }
}
