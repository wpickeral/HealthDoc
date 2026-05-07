using HealthDoc.Models;

namespace HealthDoc.Tests;

public class ProcessedRecordTests
{
    private static LabRecord MakeRecord(double result, string range = "4.0-5.6") => new()
    {
        ClinicId       = "C1",
        PatientId      = "P1",
        TestCode       = "HBA1C",
        Result         = result,
        Unit           = "%",
        ReferenceRange = range,
        CollectedAt    = new DateTime(2024, 5, 1)
    };

    [Fact]
    public void From_MapsBaseFields()
    {
        var record = MakeRecord(5.0);

        var processed = ProcessedRecord.From(record);

        Assert.Equal(record.ClinicId, processed.ClinicId);
        Assert.Equal(record.PatientId, processed.PatientId);
        Assert.Equal(record.TestCode, processed.TestCode);
        Assert.Equal(record.Result, processed.Result);
        Assert.Equal(record.Unit, processed.Unit);
        Assert.Equal(record.ReferenceRange, processed.ReferenceRange);
        Assert.Equal(record.CollectedAt, processed.CollectedAt);
        Assert.Equal("Processed", processed.Status);
    }

    [Fact]
    public void From_SetsIdFromComponents()
    {
        var record = MakeRecord(5.0);
        var today = DateTime.UtcNow.ToString("yyyyMMdd");

        var processed = ProcessedRecord.From(record);

        Assert.Equal($"C1-P1-HBA1C-{today}", processed.Id);
    }

    [Theory]
    [InlineData(5.0, false)]  // within range
    [InlineData(4.0, false)]  // at min boundary
    [InlineData(5.6, false)]  // at max boundary
    [InlineData(3.9, true)]   // below min
    [InlineData(5.7, true)]   // above max
    public void From_SetsIsAbnormal(double result, bool expectedAbnormal)
    {
        var processed = ProcessedRecord.From(MakeRecord(result));

        Assert.Equal(expectedAbnormal, processed.IsAbnormal);
    }

    [Fact]
    public void From_SetsProcessedAtToUtcNow()
    {
        var before = DateTime.UtcNow;
        var processed = ProcessedRecord.From(MakeRecord(5.0));
        var after = DateTime.UtcNow;

        Assert.InRange(processed.ProcessedAt, before, after);
    }
}