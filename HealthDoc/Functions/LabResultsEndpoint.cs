using System.Net;
using System.Text.Json;
using HealthDoc.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace HealthDoc.Functions;

public class LabResultsEndpoint
{
    private readonly CosmosClient _cosmosClient;
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<LabResultsEndpoint> _logger;

    public LabResultsEndpoint(
        CosmosClient cosmosClient,
        IConnectionMultiplexer redis,
        ILogger<LabResultsEndpoint> logger)
    {
        _cosmosClient = cosmosClient;
        _redis = redis;
        _logger = logger;
    }

    [Function(nameof(LabResultsEndpoint))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "results/{clinicId}")]
        HttpRequestData req,
        string clinicId)
    {
        var db       = _redis.GetDatabase();
        var cacheKey = AppConfig.Redis.ResultsCacheKey(clinicId);

        // Cache-aside pattern: check Redis before touching Cosmos DB.
        // On a hit the Cosmos query is skipped entirely — no RU charge, lower latency.
        var cached = await db.StringGetAsync(cacheKey);
        if (cached.HasValue)
        {
            _logger.LogInformation("Cache hit for clinic {ClinicId}", clinicId);
            var cachedRecords = JsonSerializer.Deserialize<List<ProcessedRecord>>((string)cached!);
            var cachedResponse = req.CreateResponse(HttpStatusCode.OK);
            await cachedResponse.WriteAsJsonAsync(cachedRecords);
            return cachedResponse;
        }

        _logger.LogInformation("Cache miss for clinic {ClinicId} — querying Cosmos DB", clinicId);

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

        _logger.LogInformation("Found {Count} records for clinic {ClinicId} — caching for {Ttl}s",
            records.Count, clinicId, AppConfig.Redis.DefaultTtl.TotalSeconds);

        // Store serialised JSON in Redis with a TTL. After expiry the next request
        // becomes a cache miss and refreshes from Cosmos — eventual consistency is
        // acceptable here because lab results are append-only once processed.
        await db.StringSetAsync(cacheKey, JsonSerializer.Serialize(records), AppConfig.Redis.DefaultTtl);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(records);
        return response;
    }
}
