using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.ServiceBus.Consumers;

public class ServiceBusDeadLetterMonitor
{
    private readonly ServiceBusClient _serviceBusClient;
    private readonly ILogger<ServiceBusDeadLetterMonitor> _logger;

    public ServiceBusDeadLetterMonitor(
        ServiceBusClient serviceBusClient,
        ILogger<ServiceBusDeadLetterMonitor> logger)
    {
        _serviceBusClient = serviceBusClient;
        _logger = logger;
    }

    // Runs every 5 minutes and peeks at messages in the dead-letter sub-queue.
    // Messages land in the DLQ when:
    //   - delivery count exceeds MaxDeliveryCount (default 10) after repeated failures
    //   - message TTL expires before it is consumed
    //   - the consumer explicitly dead-letters the message via DeadLetterMessageAsync
    //
    // Peek (not receive) is used here so messages remain in the DLQ for human inspection.
    // A real reprocessing flow would receive-and-delete or complete after fixing the root cause.
    [Function(nameof(ServiceBusDeadLetterMonitor))]
    public async Task Run([TimerTrigger("0 */5 * * * *")] TimerInfo timer)
    {
        // SubQueue.DeadLetter targets the queue's dead-letter sub-queue directly —
        // no separate path construction needed.
        await using var receiver = _serviceBusClient.CreateReceiver(
            AppConfig.ServiceBus.NotificationsQueue,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });

        var messages = await receiver.PeekMessagesAsync(maxMessages: 10);

        if (messages.Count == 0)
        {
            _logger.LogInformation("Dead-letter queue is empty");
            return;
        }

        _logger.LogWarning("{Count} message(s) found in dead-letter queue", messages.Count);

        foreach (var msg in messages)
        {
            _logger.LogWarning(
                "DLQ message: {MessageId} | reason: {Reason} | description: {Description}",
                msg.MessageId,
                msg.DeadLetterReason,
                msg.DeadLetterErrorDescription);
        }
    }
}
