using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace HealthDoc;

public class LabResultFileDetected
{
    private readonly ILogger<LabResultFileDetected> _logger;

    public LabResultFileDetected(ILogger<LabResultFileDetected> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Entry point for the lab result pipeline. Fires when a partner clinic uploads a CSV
    /// to the <c>lab-results-incoming</c> blob container and kicks off a new orchestration
    /// instance to validate, process, and store the batch.
    /// </summary>
    [Function("LabResultFileDetected")]
    public async Task Run([BlobTrigger("lab-results-incoming/{name}",
            Connection = "StorageConnectionString")]
        Stream stream,
        string name,
        [DurableClient] DurableTaskClient client)
    {
        using var blobStreamReader = new StreamReader(stream);

        var content = await blobStreamReader.ReadToEndAsync();
        _logger.LogInformation("C# Blob trigger function Processed blob\\n Name: {Name} \\n Data: {Content}", name,
            content);

        var payload = new FilePayload() { FileName = name, Content = content };

        await client.ScheduleNewOrchestrationInstanceAsync(nameof(ProcessLabFile), payload);
    }
}