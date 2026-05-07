using HealthDoc.Models;

namespace HealthDoc.Tests;

public class LabRecordTests
{
    [Fact]
    public void From_MapsAllColumns()
    {
        string[] cols = ["C1", "P1", "HGB", "13.5", "g/dL", "12.0-17.5", "2024-05-01"];

        var record = LabRecord.From(cols);

        Assert.Equal("C1", record.ClinicId);
        Assert.Equal("P1", record.PatientId);
        Assert.Equal("HGB", record.TestCode);
        Assert.Equal(13.5, record.Result);
        Assert.Equal("g/dL", record.Unit);
        Assert.Equal("12.0-17.5", record.ReferenceRange);
        Assert.Equal(new DateTime(2024, 5, 1), record.CollectedAt);
    }

    [Fact]
    public void From_TrimsWhitespace()
    {
        string[] cols = [" C1 ", " P1 ", " HGB ", " 13.5 ", " g/dL ", " 12.0-17.5 ", " 2024-05-01 "];

        var record = LabRecord.From(cols);

        Assert.Equal("C1", record.ClinicId);
        Assert.Equal("P1", record.PatientId);
        Assert.Equal("HGB", record.TestCode);
        Assert.Equal(13.5, record.Result);
        Assert.Equal("g/dL", record.Unit);
        Assert.Equal("12.0-17.5", record.ReferenceRange);
    }
}