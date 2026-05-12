using System.Net;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Http;

public class FailedLabFilesEndpoint
{
    private readonly BlobServiceClient _blobServiceClient;
    private readonly ILogger<FailedLabFilesEndpoint> _logger;

    public FailedLabFilesEndpoint(BlobServiceClient blobServiceClient, ILogger<FailedLabFilesEndpoint> logger)
    {
        _blobServiceClient = blobServiceClient;
        _logger = logger;
    }

    [Function(nameof(FailedLabFilesEndpoint))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "blobs/failed")]
        HttpRequestData req)
    {
        _logger.LogInformation("Listing failed lab files");

        var containerClient = _blobServiceClient.GetBlobContainerClient(AppConfig.Blob.FailedContainer);

        var files = new List<FailedFileInfo>();
        await foreach (var blob in containerClient.GetBlobsAsync())
        {
            var blobClient = containerClient.GetBlobClient(blob.Name);
            var sasUri = blobClient.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddHours(1));
            files.Add(new FailedFileInfo(blob.Name, sasUri.ToString(), blob.Properties.CreatedOn));
        }

        _logger.LogInformation("Found {Count} failed files", files.Count);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(files);
        return response;
    }
}
