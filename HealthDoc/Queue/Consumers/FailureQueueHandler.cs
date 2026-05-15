using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Queue.Consumers;

public class FailureQueueHandler(ILogger<FailureQueueHandler> logger)
{
    [Function(nameof(FailureQueueHandler))]
    public void Run(
        [QueueTrigger(AppConfig.Queue.FailuresQueue, Connection = AppConfig.Blob.Connection)]
        string message)
    {
        logger.LogWarning("Failure queue message: {Message}", message);
    }
}
