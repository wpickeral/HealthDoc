using Azure.Messaging.ServiceBus;
using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.ServiceBus.Publishers;

public class BatchCompletePublisher
{
    private readonly ServiceBusClient _serviceBusClient;
    private readonly ILogger<BatchCompletePublisher> _logger;

    public BatchCompletePublisher(ServiceBusClient serviceBusClient, ILogger<BatchCompletePublisher> logger)
    {
        _serviceBusClient = serviceBusClient;
        _logger = logger;
    }

    // WHY explicit SDK send instead of [ServiceBusOutput] binding:
    //
    // In the .NET isolated worker model, Durable activity functions return their value to
    // the Durable runtime, which serialises it and sends it back to the orchestrator.
    // When the orchestrator calls CallActivityAsync (no type parameter — return value unused),
    // the runtime still intercepts the return value before any output binding can act on it.
    // The [ServiceBusOutput] binding never fires; messages are silently dropped.
    //
    // This is surfaced by the DURABLE2002 analyzer warning:
    //   "CallActivityAsync is expecting return type 'none' but the activity returns <T>"
    // The fix is to send via the SDK directly so delivery is explicit and testable.
    //
    // Reference: https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-dotnet-isolated-overview
    [Function(AppConfig.Activities.PublishBatchComplete)]
    public async Task Publish([ActivityTrigger] ProcessingSummary summary)
    {
        _logger.LogInformation(
            "Publishing batch-complete message for {BatchId} — {TotalRecords} records, {AbnormalCount} abnormal",
            summary.BatchId, summary.TotalRecords, summary.AbnormalCount);

        var payload = new BatchCompletedMessage
        {
            BatchId       = summary.BatchId,
            ClinicId      = summary.ClinicId,
            TotalRecords  = summary.TotalRecords,
            AbnormalCount = summary.AbnormalCount,
            ProcessedAt   = summary.ProcessedAt
        };

        await using var sender = _serviceBusClient.CreateSender(AppConfig.ServiceBus.NotificationsQueue);
        await sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromObjectAsJson(payload)));
    }
}
