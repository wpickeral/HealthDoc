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

        // User delegation SAS: signed by the AAD credential (Managed Identity / az login).
        // No storage account key is needed. Request one key that covers the full window for all
        // blobs in this response; individual blob tokens are scoped to a shorter 1-hour expiry.
        //
        // AZ-204 exam note — two SAS types and revocability:
        //   User delegation SAS  — signed with an AAD credential (this approach); passwordless;
        //                          NOT revocable before expiry; does NOT support stored access policies.
        //   Service SAS          — signed with the storage account key (StorageSharedKeyCredential);
        //                          supports stored access policies (AppConfig.Blob.FailedReadPolicyId),
        //                          which allow instant revocation by deleting the policy without
        //                          rotating the account key. See README.md "Stored Access Policies".
        var delegationKey = await _blobServiceClient.GetUserDelegationKeyAsync(
            startsOn: DateTimeOffset.UtcNow.AddMinutes(-5),  // -5 min buffer for clock skew
            expiresOn: DateTimeOffset.UtcNow.AddHours(2));

        var accountName = _blobServiceClient.AccountName;

        var files = new List<FailedFileInfo>();
        await foreach (var blob in containerClient.GetBlobsAsync())
        {
            var sasBuilder = new BlobSasBuilder
            {
                BlobContainerName = AppConfig.Blob.FailedContainer,
                BlobName          = blob.Name,
                Resource          = "b",   // "b" = single blob; "c" = entire container
                StartsOn          = DateTimeOffset.UtcNow.AddMinutes(-5),
                ExpiresOn         = DateTimeOffset.UtcNow.AddHours(1)
            };
            sasBuilder.SetPermissions(BlobSasPermissions.Read);

            var sasParams = sasBuilder.ToSasQueryParameters(delegationKey, accountName);
            var blobUri   = containerClient.GetBlobClient(blob.Name).Uri;
            var sasUri    = new UriBuilder(blobUri) { Query = sasParams.ToString() }.Uri;

            files.Add(new FailedFileInfo(blob.Name, sasUri.ToString(), blob.Properties.CreatedOn));
        }

        _logger.LogInformation("Found {Count} failed files", files.Count);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(files);
        return response;
    }
}
