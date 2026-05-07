using HealthDoc.Models;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Functions;

public class DownstreamSystemNotifier
{
    private readonly ILogger<DownstreamSystemNotifier> _logger;
    private readonly TelemetryClient _telemetryClient;

    public DownstreamSystemNotifier(ILogger<DownstreamSystemNotifier> logger, TelemetryClient telemetryClient)
    {
        _logger = logger;
        _telemetryClient = telemetryClient;
    }

    [Function("NotifyDownstreamSystems")]
    public async Task Run(
        [CosmosDBTrigger(
            databaseName: AppConfig.CosmosDb.Database,
            containerName: AppConfig.CosmosDb.SummariesContainer,
            Connection = AppConfig.CosmosDb.Connection,
            LeaseContainerName = "leases",
            CreateLeaseContainerIfNotExists =  true)]
        IReadOnlyList<ProcessingSummary> summaries)
    {
        foreach (var summary in summaries)
        {
            // Track business event in App Insights
            _telemetryClient.TrackEvent("LabResultsProcessed", new Dictionary<string, string>
            {
                ["ClinicId"] = summary.ClinicId,
                ["RecordCount"] = summary.TotalRecords.ToString(),
                ["AbnormalCount"] = summary.AbnormalCount.ToString()
            });

            _logger.LogInformation(
                "Batch complete: {ClinicId} — {AbnormalCount} abnormal out of {Total}",
                summary.ClinicId, summary.AbnormalCount, summary.TotalRecords);
        }
    }
}