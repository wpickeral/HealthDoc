using HealthDoc.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Activities;

public class StorageConfirmationValidator
{
    private readonly CosmosClient _cosmosClient;
    private readonly ILogger<StorageConfirmationValidator> _logger;

    public StorageConfirmationValidator(CosmosClient cosmosClient, ILogger<StorageConfirmationValidator> logger)
    {
        _cosmosClient = cosmosClient;
        _logger = logger;
    }

    /// <summary>
    /// Polls Cosmos DB to confirm that the batch summary written by <c>StoreSummary</c>
    /// is fully readable. Called repeatedly by the orchestrator monitor loop with a
    /// 30-second durable timer between attempts. Returns the confirmed summary so the
    /// output binding can upsert the updated status, or <c>null</c> if the document is
    /// not yet visible — signaling the monitor to wait and try again.
    /// </summary>
    [Function(AppConfig.Activities.CheckStorageConfirmation)]
    [CosmosDBOutput(AppConfig.CosmosDb.Database, AppConfig.CosmosDb.SummariesContainer,
        Connection = AppConfig.CosmosDb.Connection)]
    public async Task<ProcessingSummary?> Run(
        [ActivityTrigger] string batchId)
    {
        var container = _cosmosClient
            .GetDatabase(AppConfig.CosmosDb.Database)
            .GetContainer(AppConfig.CosmosDb.SummariesContainer);

        try
        {
            // Read using CosmosClient — binding expressions don't work reliably with ActivityTrigger
            var response = await container.ReadItemAsync<ProcessingSummary>(
                batchId, new PartitionKey(batchId));

            var summary = response.Resource;
            summary.ConfirmationStatus = ConfirmationStatus.Confirmed;

            _logger.LogInformation("Batch {BatchId} confirmed", batchId);

            return summary; // output binding handles the upsert — no serializer conflict
        }
        catch (CosmosException ex) when
            (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            _logger.LogInformation("Batch {BatchId} not found yet", batchId);
            return null;
        }
    }
}