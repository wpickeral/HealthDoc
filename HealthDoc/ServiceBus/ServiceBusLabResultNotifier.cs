using HealthDoc.Models;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.ServiceBus;

public class ServiceBusLabResultNotifier
{
    private readonly ILogger<ServiceBusLabResultNotifier> _logger;
    private readonly TelemetryClient _telemetryClient;

    public ServiceBusLabResultNotifier(
        ILogger<ServiceBusLabResultNotifier> logger,
        TelemetryClient telemetryClient)
    {
        _logger = logger;
        _telemetryClient = telemetryClient;
    }

    // ServiceBusTrigger uses peek-lock by default: the message is locked (invisible to
    // other consumers) while the function runs. On successful return the runtime completes
    // it (removes from queue). On exception the lock is released and the message becomes
    // visible again for redelivery — up to the queue's MaxDeliveryCount, after which it
    // is moved to the dead-letter sub-queue automatically.
    //
    // This runs in parallel with DownstreamSystemNotifier (CosmosDB trigger) — both
    // react to the same completed batch via different event sources, demonstrating that
    // Service Bus and Cosmos Change Feed serve different notification needs.
    [Function(nameof(ServiceBusLabResultNotifier))]
    public void Run(
        [ServiceBusTrigger(AppConfig.ServiceBus.NotificationsQueue,
            Connection = AppConfig.ServiceBus.Connection)]
        BatchCompletedMessage message)
    {
        _telemetryClient.TrackEvent(AppConfig.Analytics.CustomEvents.LabResultsBatchComplete, new Dictionary<string, string>
        {
            ["BatchId"]       = message.BatchId,
            ["ClinicId"]      = message.ClinicId,
            ["TotalRecords"]  = message.TotalRecords.ToString(),
            ["AbnormalCount"] = message.AbnormalCount.ToString()
        });

        _logger.LogInformation(
            "Service Bus: batch {BatchId} — clinic {ClinicId}, {TotalRecords} records, {AbnormalCount} abnormal",
            message.BatchId, message.ClinicId, message.TotalRecords, message.AbnormalCount);
    }
}
