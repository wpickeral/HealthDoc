using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging;

namespace HealthDoc;

public static class ProcessLabFile
{
    /// <summary>
    /// Durable orchestrator that coordinates the full lab result processing pipeline.
    /// Validates the CSV, parses it into records, processes each record in parallel,
    /// stores a batch summary to Cosmos DB, polls until storage is confirmed, then
    /// moves the source file to <c>lab-results-processed</c> or <c>lab-results-failed</c>.
    /// </summary>
    [Function(nameof(ProcessLabFile))]
    public static async Task<ProcessingSummary> RunOrchestrator(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var logger = context.CreateReplaySafeLogger(nameof(ProcessLabFile));

        var payload = context.GetInput<FilePayload>();

        // PATTERN 1: Function Chaining — validate then parse sequentially
        var validationResult = await context.CallActivityAsync<ValidationResult>(
            "ValidateFile", payload);

        if (!validationResult.IsValid)
        {
            await context.CallActivityAsync("ArchiveFile", new FileArchiveRequest
            {
                FileName        = payload!.FileName,
                TargetContainer = "lab-results-failed",
                Reason          = "Validation failed — missing required fields"
            });
            return new ProcessingSummary { Status = "Failed", Errors = validationResult.Errors };
        }

        var records = await context.CallActivityAsync<List<LabRecord>>(
            "ParseFile", payload);

        // PATTERN 2: Fan-out / Fan-in — process all records in parallel
        var processingTasks = records
            .Select(record => context.CallActivityAsync<ProcessedRecord>(
                "ProcessRecord", record))
            .ToList();

        var processedRecords = await Task.WhenAll(processingTasks); // <-- Fan-in

        // PATTERN 1 continues: Store summary
        var summary = await context.CallActivityAsync<ProcessingSummary>(
            "StoreSummary", processedRecords);

        // PATTERN 4: Monitor — poll until Cosmos confirms summary is written
        var maxAttempts = 10;
        var attempts = 0;

        while (summary.ConfirmationStatus != ConfirmationStatus.Confirmed
               && attempts < maxAttempts)
        {
            await context.CreateTimer(
                context.CurrentUtcDateTime.AddSeconds(30),
                CancellationToken.None);

            var result = await context.CallActivityAsync<ProcessingSummary?>(
                "CheckStorageConfirmation", summary.BatchId);

            if (result != null)
                summary = result;

            attempts++;
        }

        if (summary.ConfirmationStatus != ConfirmationStatus.Confirmed)
        {
            summary.ConfirmationStatus = ConfirmationStatus.TimedOut;
            await context.CallActivityAsync("WriteTimeoutSummary", summary);
            logger.LogWarning("Batch {BatchId} timed out after {Attempts} confirmation attempts",
                summary.BatchId, attempts);
        }

        await context.CallActivityAsync("MoveFile", new FileArchiveRequest
        {
            FileName        = payload!.FileName,
            TargetContainer = "lab-results-processed",
            Reason          = "Processing completed successfully"
        });

        return summary;
    }
}