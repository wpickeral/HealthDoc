using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Pipeline;

public class LabRecordProcessor
{
    private readonly ILogger<LabRecordProcessor> _logger;

    public LabRecordProcessor(ILogger<LabRecordProcessor> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Enriches a single lab record by determining whether the result falls outside the
    /// clinic's reference range and generating a Cosmos DB document ID. Called once per
    /// record by the orchestrator fan-out, so all records in a batch are processed in
    /// parallel rather than sequentially.
    /// </summary>
    [Function(AppConfig.Activities.ProcessRecord)]
    public ProcessedRecord Process([ActivityTrigger] LabRecord record) =>
        ProcessedRecord.From(record);
}
