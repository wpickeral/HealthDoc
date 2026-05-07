using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Abstractions;

namespace HealthDoc;

public class DownstreamSystemNotifier
{
    private readonly ILogger<DownstreamSystemNotifier> _logger;
    private readonly ITelemetryClient _telemetryClient;

    public DownstreamSystemNotifier(ILogger<DownstreamSystemNotifier> logger, ITelemetryClient telemetryClient)
    {
        _logger = logger;
        _telemetryClient = telemetryClient;
    }

    [Function("NotifyDownstreamSystems")]
    public async Task Run(
        [CosmosDBTrigger(
            databaseName: "LabResults",
            containerName: "ProcessingSummaries",
            Connection = "CosmosDBConnection",
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