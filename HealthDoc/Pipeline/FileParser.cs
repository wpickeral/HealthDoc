using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Pipeline;

public class FileParser
{
    private readonly ILogger<FileParser> _logger;

    public FileParser(ILogger<FileParser> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Parses the raw CSV content into a strongly-typed list of lab records, one per
    /// patient result row. Skips the header and any blank lines. Each row is mapped
    /// via <see cref="LabRecord.From"/> so the column-to-field logic is testable in
    /// isolation without Azure Functions infrastructure.
    /// </summary>
    [Function(AppConfig.Activities.ParseFile)]
    public List<LabRecord> Parse([ActivityTrigger] FilePayload payload)
    {
        var records = payload.Content
            .Split('\n')
            .Skip(1)
            .Where(l => !string.IsNullOrWhiteSpace(l))
            .Select(line => LabRecord.From(line.Split(',')))
            .ToList();

        _logger.LogInformation("Parsed {RecordCount} records from {FileName}", records.Count, payload.FileName);

        return records;
    }
}
