using Azure.Messaging;
using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Events;

public class EventGridLabResultAuditor
{
    private readonly ILogger<EventGridLabResultAuditor> _logger;

    public EventGridLabResultAuditor(ILogger<EventGridLabResultAuditor> logger)
    {
        _logger = logger;
    }

    // Receives Microsoft.Storage.BlobCreated system events from an Event Grid subscription
    // on the lab-results-incoming container. Runs independently of LabResultIngestionTrigger —
    // both fire on the same blob upload, demonstrating that Event Grid fan-out delivers the
    // same event to multiple subscribers without either subscriber knowing about the other.
    //
    // AZ-204 distinction:
    //   BlobTrigger    — polls storage internally, lower latency for small workloads
    //   Event Grid     — push delivery, scales better, works across subscriptions/regions
    //
    // The CloudEvent.Subject for blob events follows the path:
    //   /blobServices/default/containers/{container}/blobs/{blobName}
    // We extract the filename from the last segment.
    [Function(nameof(EventGridLabResultAuditor))]
    [CosmosDBOutput(AppConfig.CosmosDb.Database, AppConfig.CosmosDb.AuditLogContainer,
        Connection = AppConfig.CosmosDb.Connection)]
    public LabAuditRecord Run([EventGridTrigger] CloudEvent cloudEvent)
    {
        var fileName  = cloudEvent.Subject?.Split('/').LastOrDefault() ?? "unknown";
        var clinicId  = ExtractClinicId(fileName);
        var blobUrl   = cloudEvent.Data?.ToObjectFromJson<BlobCreatedData>()?.Url ?? string.Empty;

        _logger.LogInformation(
            "Event Grid: {EventType} for blob {FileName} — writing audit record",
            cloudEvent.Type, fileName);

        return new LabAuditRecord
        {
            ClinicId   = clinicId,
            FileName   = fileName,
            EventType  = cloudEvent.Type ?? string.Empty,
            BlobUrl    = blobUrl,
            ReceivedAt = DateTime.UtcNow
        };
    }

    // Blob filenames follow the pattern: lab-results-{timestamp}-{guid}.csv
    // ClinicId is not in the filename — a real system would decode it from blob metadata
    // or use a lookup. For the audit record we store a placeholder to keep the
    // partition key populated (Cosmos DB requires a value for /ClinicId).
    private static string ExtractClinicId(string fileName) =>
        fileName.StartsWith("lab-results-") ? "PENDING" : "UNKNOWN";

    // Minimal projection of the BlobCreated event data — only the fields we need.
    // The full schema is documented at:
    // https://learn.microsoft.com/azure/event-grid/event-schema-blob-storage
    private sealed class BlobCreatedData
    {
        public string? Url { get; set; }
    }
}
