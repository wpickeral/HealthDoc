using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Functions;

public class LabResultIngestionTrigger
{
    private readonly ILogger<LabResultIngestionTrigger> _logger;

    public LabResultIngestionTrigger(ILogger<LabResultIngestionTrigger> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Entry point for the lab result pipeline. Fires when a partner clinic uploads a CSV
    /// to the <c>lab-results-incoming</c> blob container and kicks off a new orchestration
    /// instance to validate, process, and store the batch.
    /// </summary>
    [Function("IngestLabResult")]
    public async Task Run([BlobTrigger(AppConfig.Blob.IncomingTriggerPath,
            Connection = AppConfig.Blob.Connection)]
        Stream stream,
        string name,
        [DurableClient] DurableTaskClient client)
    {
        using var blobStreamReader = new StreamReader(stream);

        var content = await blobStreamReader.ReadToEndAsync();

        _logger.LogInformation("Lab result file detected: {FileName} — scheduling pipeline", name);

        var payload = new FilePayload() { FileName = name, Content = content };

        var options = new StartOrchestrationOptions { InstanceId = name };

        try
        {
            await client.ScheduleNewOrchestrationInstanceAsync(nameof(LabResultOrchestrator), payload, options);
        }
        catch (InvalidOperationException)
        {
            var existing = await client.GetInstanceAsync(name);

            if (existing?.RuntimeStatus is OrchestrationRuntimeStatus.Running
                                         or OrchestrationRuntimeStatus.Pending)
            {
                _logger.LogWarning(
                    "{FileName} is already being processed — instance {InstanceId} still active, skipping duplicate",
                    name, name);
                return;
            }

            _logger.LogWarning("Prior instance found for {FileName} — purging and rescheduling", name);

            await client.PurgeInstanceAsync(name);

            await client.ScheduleNewOrchestrationInstanceAsync(nameof(LabResultOrchestrator), payload, options);
        }

        _logger.LogInformation("Orchestration started for {FileName} — instance {InstanceId}", name, name);
    }
}
