using System.Net;
using HealthDoc.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Functions;

public class LabResultsEndpoint
{
    private readonly CosmosClient _cosmosClient;
    private readonly ILogger<LabResultsEndpoint> _logger;

    public LabResultsEndpoint(CosmosClient cosmosClient, ILogger<LabResultsEndpoint> logger)
    {
        _cosmosClient = cosmosClient;
        _logger = logger;
    }

    [Function(nameof(LabResultsEndpoint))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "results/{clinicId}")]
        HttpRequestData req,
        string clinicId)
    {
        _logger.LogInformation("Querying lab results for clinic {ClinicId}", clinicId);

        var container = _cosmosClient
            .GetDatabase(AppConfig.CosmosDb.Database)
            .GetContainer(AppConfig.CosmosDb.LabResultRecordsContainer);

        var query = new QueryDefinition("SELECT * FROM c WHERE c.ClinicId = @clinicId")
            .WithParameter("@clinicId", clinicId);

        var options = new QueryRequestOptions { PartitionKey = new PartitionKey(clinicId) };

        var records = new List<ProcessedRecord>();
        using var feed = container.GetItemQueryIterator<ProcessedRecord>(query, requestOptions: options);
        while (feed.HasMoreResults)
        {
            var page = await feed.ReadNextAsync();
            records.AddRange(page);
        }

        _logger.LogInformation("Found {Count} records for clinic {ClinicId}", records.Count, clinicId);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(records);
        return response;
    }
}
