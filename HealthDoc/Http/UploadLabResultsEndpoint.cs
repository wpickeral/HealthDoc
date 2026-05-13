using System.Net;
using Azure.Storage.Blobs;
using HealthDoc.Models;
using HealthDoc.Pipeline;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Http;

public class UploadLabResultsEndpoint
{
    private readonly BlobServiceClient _blobServiceClient;
    private readonly ILogger<UploadLabResultsEndpoint> _logger;

    public UploadLabResultsEndpoint(BlobServiceClient blobServiceClient, ILogger<UploadLabResultsEndpoint> logger)
    {
        _blobServiceClient = blobServiceClient;
        _logger = logger;
    }

    [Function(nameof(UploadLabResultsEndpoint))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "upload")]
        HttpRequestData req,
        [DurableClient] DurableTaskClient client)
    {
        var clinicId = req.Headers.TryGetValues("x-clinic-id", out var values)
            ? values.FirstOrDefault() ?? "UNKNOWN"
            : "UNKNOWN";
        var fileName = $"lab-results-{clinicId}-{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid().ToString("N")[..8]}.csv";

        _logger.LogInformation("Receiving lab results upload — clinic {ClinicId}, assigning filename {FileName}",
            clinicId, fileName);

        var content = await new StreamReader(req.Body).ReadToEndAsync();

        var blob = _blobServiceClient
            .GetBlobContainerClient(AppConfig.Blob.IncomingContainer)
            .GetBlobClient(fileName);

        await blob.UploadAsync(BinaryData.FromString(content), overwrite: false);

        _logger.LogInformation("Blob written to {Container}/{FileName} — starting orchestration",
            AppConfig.Blob.IncomingContainer, fileName);

        var payload = new FilePayload { FileName = fileName, Content = content };
        var options = new StartOrchestrationOptions { InstanceId = fileName };
        await client.ScheduleNewOrchestrationInstanceAsync(nameof(LabResultOrchestrator), payload, options);

        var response = req.CreateResponse(HttpStatusCode.Created);
        await response.WriteAsJsonAsync(new { instanceId = fileName });
        return response;
    }
}
