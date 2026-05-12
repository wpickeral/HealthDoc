using HealthDoc.Models;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.ServiceBus;

public class ClinicalAlertHandler
{
    private readonly ILogger<ClinicalAlertHandler> _logger;
    private readonly TelemetryClient _telemetryClient;

    public ClinicalAlertHandler(
        ILogger<ClinicalAlertHandler> logger,
        TelemetryClient telemetryClient)
    {
        _logger = logger;
        _telemetryClient = telemetryClient;
    }

    // Consumes the clinical-alerts subscription on lab-results-alerts topic.
    // Receives all abnormal-result messages regardless of count — no SQL filter.
    // Represents a clinical team notifier: every abnormal batch triggers a review workflow.
    [Function(nameof(ClinicalAlertHandler))]
    public void Run(
        [ServiceBusTrigger(AppConfig.ServiceBus.AlertsTopic,
            AppConfig.ServiceBus.ClinicalAlertsSub,
            Connection = AppConfig.ServiceBus.Connection)]
        BatchCompletedMessage message)
    {
        _telemetryClient.TrackEvent(
            AppConfig.Analytics.CustomEvents.ClinicalAlertReceived, new Dictionary<string, string>
        {
            ["BatchId"]       = message.BatchId,
            ["ClinicId"]      = message.ClinicId,
            ["AbnormalCount"] = message.AbnormalCount.ToString()
        });

        _logger.LogInformation(
            "Clinical alert: batch {BatchId} for clinic {ClinicId} has {AbnormalCount} abnormal result(s)",
            message.BatchId, message.ClinicId, message.AbnormalCount);
    }
}
