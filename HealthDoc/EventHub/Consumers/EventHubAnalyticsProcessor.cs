namespace HealthDoc.EventHub.Consumers;

using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;


public class EventHubAnalyticsProcessor(ILogger<EventHubAnalyticsProcessor> logger)
{
    // cardinality: many — processes a batch of events per invocation
    [Function(nameof(EventHubAnalyticsProcessor))]
    public void Run(
        [EventHubTrigger(AppConfig.EventHub.Name,
            Connection      = AppConfig.EventHub.Connection,
            ConsumerGroup   = AppConfig.EventHub.ConsumerGroup)]
        string[] events)
    {
        foreach (var e in events)
            logger.LogInformation("Event Hub event received: {Event}", e);
    }
}