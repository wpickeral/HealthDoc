using System;
using System.IO;
using System.Threading.Tasks;
using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Activities;

public abstract class ValidateFile
{
    /// Validate — check columns, required fields, value ranges
    [Function("ValidateFile")]
    public static ValidationResult Validate([ActivityTrigger] FilePayload payload)
    {
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

            return new ValidationResult { IsValid = !errors.Any(), Errors = errors };
        }
        catch (Exception e)
        {
            return new ValidationResult()
            {
                IsValid = false,
                Errors = [e.Message],
            };
        }
    }
}