using Azure.Identity;
using Azure.Storage.Blobs;
using HealthDoc.Models;
using Microsoft.Azure.Cosmos;
using System.Text;

var cosmosEndpoint = Environment.GetEnvironmentVariable("COSMOS_ENDPOINT")
    ?? throw new InvalidOperationException("COSMOS_ENDPOINT is not configured");
var storageEndpoint = Environment.GetEnvironmentVariable("STORAGE_ENDPOINT")
    ?? throw new InvalidOperationException("STORAGE_ENDPOINT is not configured");

// DefaultAzureCredential resolves: az login locally → Managed Identity in Azure.
var credential = new DefaultAzureCredential();
var cosmosClient = new CosmosClient(cosmosEndpoint, credential);
var blobServiceClient = new BlobServiceClient(new Uri(storageEndpoint), credential);

Console.WriteLine("Querying processing summaries...");

var container = cosmosClient
    .GetDatabase("LabResults")
    .GetContainer("ProcessingSummaries");

var summaries = new List<ProcessingSummary>();
using var feed = container.GetItemQueryIterator<ProcessingSummary>(
    new QueryDefinition("SELECT * FROM c"));
while (feed.HasMoreResults)
{
    var page = await feed.ReadNextAsync();
    summaries.AddRange(page);
}

Console.WriteLine($"Found {summaries.Count} batch summaries.");

var csv = new StringBuilder();
csv.AppendLine("BatchId,ClinicId,TotalRecords,AbnormalCount,AbnormalRate%,ConfirmationStatus,ProcessedAt");

foreach (var s in summaries.OrderBy(s => s.ProcessedAt))
{
    var rate = s.TotalRecords > 0 ? (double)s.AbnormalCount / s.TotalRecords * 100 : 0;
    csv.AppendLine($"{s.BatchId},{s.ClinicId},{s.TotalRecords},{s.AbnormalCount},{rate:F1},{s.ConfirmationStatus},{s.ProcessedAt:O}");
}

var reportContainer = blobServiceClient.GetBlobContainerClient("lab-results-reports");
await reportContainer.CreateIfNotExistsAsync();

var blobName = $"report-{DateTime.UtcNow:yyyyMMdd-HHmmss}.csv";
await reportContainer.GetBlobClient(blobName).UploadAsync(
    new BinaryData(csv.ToString()), overwrite: true);

Console.WriteLine($"Report written: lab-results-reports/{blobName}");
