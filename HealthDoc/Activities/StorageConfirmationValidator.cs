using HealthDoc.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Activities;

public class StorageConfirmation
{
    private readonly CosmosClient _cosmosClient;

    public StorageConfirmation(CosmosClient cosmosClient)
    {
        _cosmosClient = cosmosClient;
    }

    [Function("CheckStorageConfirmation")]
    [CosmosDBOutput("LabResults", "ProcessingSummaries",
        Connection = "CosmosDBConnectionString")]
    public async Task<ProcessingSummary?> Run(
        [ActivityTrigger] string batchId,
        FunctionContext context)
    {
        var logger = context.GetLogger("CheckStorageConfirmation");

        var container = _cosmosClient
            .GetDatabase("LabResults")
            .GetContainer("ProcessingSummaries");

        try
        {
            // Read using CosmosClient — binding expressions don't work reliably with ActivityTrigger
            var response = await container.ReadItemAsync<ProcessingSummary>(
                batchId, new PartitionKey(batchId));

            var summary = response.Resource;
            summary.ConfirmationStatus = ConfirmationStatus.Confirmed;

            logger.LogInformation("Batch {BatchId} confirmed", batchId);

            return summary; // output binding handles the upsert — no serializer conflict
        }
        catch (CosmosException ex) when
            (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            logger.LogInformation("Batch {BatchId} not found yet", batchId);
            return null;
        }
    }
}