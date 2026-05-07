using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Activities;

public class PatientResultUpdater
{
    private readonly ILogger<PatientResultUpdater> _logger;

    public PatientResultUpdater(ILogger<PatientResultUpdater> logger)
    {
        _logger = logger;
    }

    [Function(AppConfig.Activities.StoreRecords)]
    [CosmosDBOutput(AppConfig.CosmosDb.Database, AppConfig.CosmosDb.LabResultRecordsContainer,
        Connection = AppConfig.CosmosDb.Connection)]
    public ProcessedRecord[] StoreRecords(
        [ActivityTrigger] ProcessedRecord[] records)
    {
        _logger.LogInformation(
            "Storing {Count} records for clinic {ClinicId}",
            records.Length, records.First().ClinicId);

        // Output binding writes the entire array — one document per record
        return records;
    }
}