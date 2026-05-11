using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Activities;

public class AbnormalAlertPublisher
{
    private readonly ILogger<AbnormalAlertPublisher> _logger;

    public AbnormalAlertPublisher(ILogger<AbnormalAlertPublisher> logger)
    {
        _logger = logger;
    }

    // ServiceBusOutput binding delivers to a topic rather than a queue.
    // Topics fan out to multiple subscriptions — in the portal, lab-results-alerts has two:
    //   clinical-alerts  — receives all messages (no filter)
    //   critical-alerts  — SQL filter: AbnormalCount > 5
    // This demonstrates the queue vs topic distinction: queues deliver to one consumer;
    // topics deliver to every matching subscription simultaneously.
    [Function(AppConfig.Activities.PublishAbnormalAlert)]
    [ServiceBusOutput(AppConfig.ServiceBus.AlertsTopic,
        Connection = AppConfig.ServiceBus.Connection)]
    public BatchCompletedMessage PublishAlert([ActivityTrigger] ProcessingSummary summary)
    {
        _logger.LogInformation(
            "Publishing abnormal-alert to topic for {BatchId} — {AbnormalCount} abnormal result(s)",
            summary.BatchId, summary.AbnormalCount);

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
