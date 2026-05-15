using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace HealthDoc.Pipeline;

public class PatientResultUpdater
{
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<PatientResultUpdater> _logger;

    public PatientResultUpdater(IConnectionMultiplexer redis, ILogger<PatientResultUpdater> logger)
    {
        _redis = redis;
        _logger = logger;
    }

    [Function(AppConfig.Activities.StoreRecords)]
    [CosmosDBOutput(AppConfig.CosmosDb.Database, AppConfig.CosmosDb.LabResultRecordsContainer,
        Connection = AppConfig.CosmosDb.Connection)]
    public async Task<ProcessedRecord[]> StoreRecords(
        [ActivityTrigger] ProcessedRecord[] records)
    {
        var clinicId = records.First().ClinicId;

        _logger.LogInformation("Storing {Count} records for clinic {ClinicId}",
            records.Length, clinicId);

        // Invalidate the cache for this clinic so the next GET fetches fresh data.
        // Write-invalidate (delete on write) rather than write-through (update on write):
        // simpler, avoids serialising here, and the next read will naturally repopulate.
        var db = _redis.GetDatabase();
        var deleted = await db.KeyDeleteAsync(AppConfig.Redis.ResultsCacheKey(clinicId));

        if (deleted)
            _logger.LogInformation("Cache invalidated for clinic {ClinicId}", clinicId);

        // Output binding writes the entire array to Cosmos — one document per record
        return records;
    }
}
