using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;

namespace HealthDoc.Activities;

public abstract class ParseFile
{
    // Parse CSV into typed records
    [Function("ParseFile")]
    public static List<LabRecord> Parse([ActivityTrigger] FilePayload payload)
    {
        return payload.Content
            .Split('\n')
            .Skip(1)
            .Where(l => !string.IsNullOrWhiteSpace(l))
            .Select(line =>
            {
                var cols = line.Split(',');
                return new LabRecord
                {
                    ClinicId = cols[0].Trim(),
                    PatientId = cols[1].Trim(),
                    TestCode = cols[2].Trim(),
                    Result = double.Parse(cols[3].Trim()),
                    Unit = cols[4].Trim(),
                    ReferenceRange = cols[5].Trim(),
                    CollectedAt = DateTime.Parse(cols[6].Trim())
                };
            }).ToList();
    }
}