using System.Net;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Functions;

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
        HttpRequestData req)
    {
        var fileName = $"lab-results-{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid().ToString("N")[..8]}.csv";

        _logger.LogInformation("Receiving lab results upload — assigning filename {FileName}", fileName);

        var blob = _blobServiceClient
            .GetBlobContainerClient(AppConfig.Blob.IncomingContainer)
            .GetBlobClient(fileName);

        await blob.UploadAsync(req.Body, overwrite: false);

        _logger.LogInformation("Blob written to {Container}/{FileName} — pipeline will trigger automatically",
            AppConfig.Blob.IncomingContainer, fileName);

        var response = req.CreateResponse(HttpStatusCode.Created);
        await response.WriteAsJsonAsync(new { instanceId = fileName });
        return response;
    }
}
