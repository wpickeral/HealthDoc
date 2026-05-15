using Azure.Storage.Queues;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Queue.Publishers;

public class FailureQueueNotifierActivity(QueueClient queueClient, ILogger<FailureQueueNotifierActivity> logger)
{
    // WHY explicit SDK send instead of [QueueOutput] binding:
    // [QueueOutput] on a Durable activity is silently swallowed — the Durable runtime
    // captures the return value before the binding can deliver the message.
    // Same root cause as [ServiceBusOutput] on Durable activities (see BatchCompletePublisherActivity).
    [Function(AppConfig.Activities.NotifyFailureQueue)]
    public async Task Run([ActivityTrigger] string fileName)
    {
        var message = $"Validation failed: {fileName} at {DateTimeOffset.UtcNow:O}";
        await queueClient.SendMessageAsync(message);
        logger.LogInformation("Queued failure notification for {FileName}", fileName);
    }
}
