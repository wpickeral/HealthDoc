using HealthDoc.Models;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.ServiceBus.Consumers;

public class CriticalAlertHandler
{
    private readonly ILogger<CriticalAlertHandler> _logger;
    private readonly TelemetryClient _telemetryClient;

    public CriticalAlertHandler(
        ILogger<CriticalAlertHandler> logger,
        TelemetryClient telemetryClient)
    {
        _logger = logger;
        _telemetryClient = telemetryClient;
    }

    // Consumes the critical-alerts subscription on lab-results-alerts topic.
    // Only receives messages where AbnormalCount > 5 — enforced by the SQL filter
    // on the subscription. By the time this function fires, the count is guaranteed
    // to exceed the threshold; no additional check is needed here.
    // Represents an escalation path: high-volume abnormal batches require urgent review.
    [Function(nameof(CriticalAlertHandler))]
    public void Run(
        [ServiceBusTrigger(AppConfig.ServiceBus.AlertsTopic,
            AppConfig.ServiceBus.CriticalAlertsSub,
            Connection = AppConfig.ServiceBus.Connection)]
        BatchCompletedMessage message)
    {
        _telemetryClient.TrackEvent(
            AppConfig.Analytics.CustomEvents.CriticalAlertReceived, new Dictionary<string, string>
        {
            ["BatchId"]       = message.BatchId,
            ["ClinicId"]      = message.ClinicId,
            ["AbnormalCount"] = message.AbnormalCount.ToString()
        });

        _logger.LogWarning(
            "CRITICAL: batch {BatchId} for clinic {ClinicId} has {AbnormalCount} abnormal results — exceeds threshold of 5",
            message.BatchId, message.ClinicId, message.AbnormalCount);
    }
}
