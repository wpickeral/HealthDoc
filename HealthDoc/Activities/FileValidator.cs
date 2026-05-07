using System;
using System.IO;
using System.Threading.Tasks;
using HealthDoc.Models;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Activities;

public class FileValidator
{
    private readonly TelemetryClient _telemetryClient;
    private readonly ILogger<FileValidator> _logger;

    public FileValidator(TelemetryClient telemetryClient, ILogger<FileValidator> logger)
    {
        _telemetryClient = telemetryClient;
        _logger = logger;
    }

    /// <summary>
    /// Validates a lab result CSV before any records are processed. Checks that all
    /// required columns are present and that every data row has a non-empty patient ID
    /// and a numeric result value. A failed validation causes the orchestrator to abort
    /// early and move the file to <c>lab-results-failed</c> without touching Cosmos DB.
    /// </summary>
    [Function(AppConfig.Activities.ValidateFile)]
    public ValidationResult Validate([ActivityTrigger] FilePayload payload)
    {
        _logger.LogInformation("Validating {FileName}", payload.FileName);

        ValidationResult validationResult = new ValidationResult();

        try
        {
            var errors = new List<string>();
            var lines = payload.Content.Split('\n');
            var headers = lines[0].Split(',').Select(h => h.Trim()).ToList();

            // Check required headers exist
            var required = new[] { "ClinicId", "PatientId", "TestCode", "Result", "ReferenceRange" };
            foreach (var col in required)
                if (!headers.Contains(col))
                    errors.Add($"Missing required column: {col}");

            // Check records have values
            foreach (var line in lines.Skip(1).Where(l => !string.IsNullOrWhiteSpace(l)))
            {
                var cols = line.Split(',');
                if (string.IsNullOrWhiteSpace(cols[1])) errors.Add("Missing PatientId");
                if (!double.TryParse(cols[3], out _)) errors.Add($"Non-numeric Result: {cols[3]}");
            }

            validationResult.IsValid = errors.Count == 0;
            validationResult.Errors = errors;

            if (validationResult.IsValid)
                _logger.LogInformation("{FileName} passed validation", payload.FileName);
            else
                _logger.LogWarning("{FileName} failed validation — {ErrorCount} error(s): {Errors}",
                    payload.FileName, errors.Count, string.Join("; ", errors));
        }
        catch (Exception e)
        {
            validationResult.Errors.Add(e.Message);
            validationResult.IsValid = false;

            _telemetryClient.TrackEvent("FileValidationFailed", new Dictionary<string, string>
            {
                ["FileName"] = payload.FileName,
                ["ErrorCount"] = validationResult.Errors.Count.ToString(),
                ["Errors"] = string.Join(", ", validationResult.Errors)
            });
        }

        return validationResult;
    }
}