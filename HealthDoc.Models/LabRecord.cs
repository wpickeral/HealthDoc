namespace HealthDoc.Models;

/// One row from the CSV after ParseFile activity
public class LabRecord
{
    public string ClinicId { get; set; } = string.Empty;
    public string PatientId { get; set; } = string.Empty;
    public string TestCode { get; set; } = string.Empty;
    public double Result { get; set; }

    /// <summary>
    /// The normal value range for this test, expressed as "min-max" (e.g. "4.0-5.6").
    /// Used to determine whether the result is abnormal. Values outside this range
    /// set IsAbnormal to true.
    /// </summary>
    public string ReferenceRange { get; set; } = string.Empty;

    public DateTime CollectedAt { get; set; }

    public string Unit { get; set; } = string.Empty;

    public static LabRecord From(string[] cols) => new()
    {
        ClinicId       = cols[0].Trim(),
        PatientId      = cols[1].Trim(),
        TestCode       = cols[2].Trim(),
        Result         = double.Parse(cols[3].Trim()),
        Unit           = cols[4].Trim(),
        ReferenceRange = cols[5].Trim(),
        CollectedAt    = DateTime.Parse(cols[6].Trim())
    };
}