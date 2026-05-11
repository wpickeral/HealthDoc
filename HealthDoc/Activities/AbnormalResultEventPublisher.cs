using Azure.Messaging;
using Azure.Messaging.EventGrid;
using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Activities;

public class AbnormalResultEventPublisher
{
    private readonly EventGridPublisherClient _publisherClient;
    private readonly ILogger<AbnormalResultEventPublisher> _logger;

    public AbnormalResultEventPublisher(
        EventGridPublisherClient publisherClient,
        ILogger<AbnormalResultEventPublisher> logger)
    {
        _publisherClient = publisherClient;
        _logger = logger;
    }

    // Publishes a custom CloudEvent to the Event Grid topic when abnormal lab results are
    // detected. Any subscriber (webhook, another Function, Logic App, Event Hubs) can
    // react without knowing about the Durable Functions pipeline.
    //
    // CloudEvents vs Event Grid schema (AZ-204 exam concept):
    //   CloudEvents  — open standard, vendor-neutral, recommended for new work
    //   Event Grid   — Azure-native schema, required for some built-in connectors
    // This activity uses CloudEvents (the modern choice).
    //
    // The activity pattern is required here: orchestrators must not perform I/O directly
    // because async SDK calls break deterministic replay. All network calls go through activities.
    [Function(AppConfig.Activities.PublishAbnormalEvent)]
    public async Task PublishAsync([ActivityTrigger] ProcessingSummary summary)
    {
        var cloudEvent = new CloudEvent(
            source: "/healthdoc/labs/orchestrator",
            type:   "HealthDoc.Lab.AbnormalResultDetected",
            jsonSerializableData: new AbnormalResultEvent
            {
                BatchId       = summary.BatchId,
                ClinicId      = summary.ClinicId,
                AbnormalCount = summary.AbnormalCount,
                TotalRecords  = summary.TotalRecords,
                DetectedAt    = DateTime.UtcNow
            });

        await _publisherClient.SendEventAsync(cloudEvent);

        _logger.LogInformation(
            "Event Grid: published AbnormalResultDetected for batch {BatchId} — {AbnormalCount} abnormal",
            summary.BatchId, summary.AbnormalCount);
    }
}
