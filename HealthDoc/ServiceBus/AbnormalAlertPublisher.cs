using Azure.Messaging.ServiceBus;
using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.ServiceBus;

public class AbnormalAlertPublisher
{
    private readonly ILogger<AbnormalAlertPublisher> _logger;

    public AbnormalAlertPublisher(ILogger<AbnormalAlertPublisher> logger)
    {
        _logger = logger;
    }

    // ServiceBusOutput binding delivers to a topic rather than a queue.
    // Topics fan out to multiple subscriptions — in the portal, lab-results-alerts has two:
    //   clinical-alerts  — receives all messages (no filter / $Default TrueFilter)
    //   critical-alerts  — SQL filter: AbnormalCount > 5
    //
    // SQL filters evaluate message ApplicationProperties (headers), not the JSON body.
    // Returning a POCO from the binding serialises it into the body only — the filter
    // would never match. We return ServiceBusMessage directly so we can set
    // ApplicationProperties["AbnormalCount"] explicitly alongside the JSON body.
    [Function(AppConfig.Activities.PublishAbnormalAlert)]
    [ServiceBusOutput(AppConfig.ServiceBus.AlertsTopic,
        Connection = AppConfig.ServiceBus.Connection)]
    public ServiceBusMessage PublishAlert([ActivityTrigger] ProcessingSummary summary)
    {
        _logger.LogInformation(
            "Publishing abnormal-alert to topic for {BatchId} — {AbnormalCount} abnormal result(s)",
            summary.BatchId, summary.AbnormalCount);

        var payload = new BatchCompletedMessage
        {
            BatchId       = summary.BatchId,
            ClinicId      = summary.ClinicId,
            TotalRecords  = summary.TotalRecords,
            AbnormalCount = summary.AbnormalCount,
            ProcessedAt   = summary.ProcessedAt
        };

        var message = new ServiceBusMessage(BinaryData.FromObjectAsJson(payload));
        message.ApplicationProperties["AbnormalCount"] = summary.AbnormalCount;
        return message;
    }
}
