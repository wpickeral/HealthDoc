using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Activities;

public class BatchCompletePublisher
{
    private readonly ILogger<BatchCompletePublisher> _logger;

    public BatchCompletePublisher(ILogger<BatchCompletePublisher> logger)
    {
        _logger = logger;
    }

    // ServiceBusOutput binding delivers the returned message to the notifications queue.
    // The orchestrator calls this as an activity so that all I/O stays outside the
    // orchestrator function, preserving deterministic replay.
    [Function(AppConfig.Activities.PublishBatchComplete)]
    [ServiceBusOutput(AppConfig.ServiceBus.NotificationsQueue,
        Connection = AppConfig.ServiceBus.Connection)]
    public BatchCompletedMessage Publish([ActivityTrigger] ProcessingSummary summary)
    {
        _logger.LogInformation(
            "Publishing batch-complete message for {BatchId} — {TotalRecords} records, {AbnormalCount} abnormal",
            summary.BatchId, summary.TotalRecords, summary.AbnormalCount);

        return new BatchCompletedMessage
        {
            BatchId       = summary.BatchId,
            ClinicId      = summary.ClinicId,
            TotalRecords  = summary.TotalRecords,
            AbnormalCount = summary.AbnormalCount,
            ProcessedAt   = summary.ProcessedAt
        };
    }
}
