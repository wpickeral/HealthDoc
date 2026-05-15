using Azure.Messaging.ServiceBus;
using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.ServiceBus.Publishers;

public class AbnormalAlertPublisher
{
    private readonly ServiceBusClient _serviceBusClient;
    private readonly ILogger<AbnormalAlertPublisher> _logger;

    public AbnormalAlertPublisher(ServiceBusClient serviceBusClient, ILogger<AbnormalAlertPublisher> logger)
    {
        _serviceBusClient = serviceBusClient;
        _logger = logger;
    }

    // WHY explicit SDK send instead of [ServiceBusOutput] binding:
    // See BatchCompletePublisher for the full explanation. Short version: in the isolated
    // worker model, [ServiceBusOutput] on a Durable activity is silently swallowed by the
    // Durable runtime before the binding can deliver the message. SDK send is required.
    //
    // WHY ServiceBusMessage instead of a POCO return:
    // Topics fan out to multiple subscriptions — lab-results-alerts has two:
    //   clinical-alerts  — receives all messages (no filter)
    //   critical-alerts  — SQL filter: AbnormalCount > 5
    //
    // SQL filters evaluate message ApplicationProperties (headers), not the JSON body.
    // ApplicationProperties["AbnormalCount"] must be set explicitly for the filter to match.
    // Constructing ServiceBusMessage directly gives us control over both the body and properties.
    //
    // Reference: https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-filters
    [Function(AppConfig.Activities.PublishAbnormalAlert)]
    public async Task PublishAlert([ActivityTrigger] ProcessingSummary summary)
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

        await using var sender = _serviceBusClient.CreateSender(AppConfig.ServiceBus.AlertsTopic);
        await sender.SendMessageAsync(message);
    }
}
