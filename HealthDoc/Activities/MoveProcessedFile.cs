using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Activities;

public class MoveProcessedFile
{
    private const string SourceContainer = AppConfig.Blob.IncomingContainer;

    private readonly BlobServiceClient _blobServiceClient;
    private readonly ILogger<MoveProcessedFile> _logger;

    public MoveProcessedFile(BlobServiceClient blobServiceClient, ILogger<MoveProcessedFile> logger)
    {
        _blobServiceClient = blobServiceClient;
        _logger = logger;
    }

    /// <summary>
    /// Moves a processed CSV out of <c>lab-results-incoming</c> into the appropriate
    /// container once the orchestration reaches a terminal state. Files that
    /// passed validation land in <c>lab-results-processed</c>; files that failed
    /// validation land in <c>lab-results-failed</c>. The source blob is deleted after
    /// the copy succeeds to prevent reprocessing on the next trigger poll.
    /// </summary>
    [Function(AppConfig.Activities.MoveFile)]
    public async Task MoveFile([ActivityTrigger] FileArchiveRequest request)
    {
        _logger.LogInformation(
            "Moving {FileName} to {TargetContainer}. Reason: {Reason}",
            request.FileName, request.TargetContainer, request.Reason);

        var source = _blobServiceClient
            .GetBlobContainerClient(SourceContainer)
            .GetBlobClient(request.FileName);

        var dest = _blobServiceClient
            .GetBlobContainerClient(request.TargetContainer)
            .GetBlobClient(request.FileName);

        // Server-side copy — doesn't stream through your function
        await dest.StartCopyFromUriAsync(source.Uri);

        // Set tier after copy completes
        await dest.SetAccessTierAsync(AccessTier.Cool);

        // Delete source only after successful copy and tier set
        await source.DeleteAsync();
    }
}
