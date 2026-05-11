using HealthDoc.Models;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Functions;

public class LabResultOrchestrator
{
    private readonly TelemetryClient _telemetryClient;

    public LabResultOrchestrator(TelemetryClient telemetryClient)
    {
        _telemetryClient = telemetryClient;
    }

    /// <summary>
    /// Durable orchestrator that coordinates the full lab result processing pipeline.
    /// Validates the CSV, parses it into records, processes each record in parallel,
    /// stores a batch summary to Cosmos DB, polls until storage is confirmed, then
    /// moves the source file to <c>lab-results-processed</c> or <c>lab-results-failed</c>.
    /// </summary>
    [Function(nameof(LabResultOrchestrator))]
    public async Task<ProcessingSummary> Run(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var logger = context.CreateReplaySafeLogger(nameof(LabResultOrchestrator));
        var payload = context.GetInput<FilePayload>()!;
        var startedAt = context.CurrentUtcDateTime;

        logger.LogInformation("Pipeline started for {FileName}", payload.FileName);

        // PATTERN 1: Function Chaining — validate then parse sequentially
        var validationResult = await ValidateAsync(context, payload);

        if (!validationResult.IsValid)
        {
            logger.LogWarning("{FileName} failed validation — {ErrorCount} error(s): {Errors}",
                payload.FileName, validationResult.Errors.Count, string.Join("; ", validationResult.Errors));

            await MoveFileAsync(context, payload.FileName, AppConfig.Blob.FailedContainer,
                "Validation failed — missing required fields");

            return new ProcessingSummary { Status = "Failed", Errors = validationResult.Errors };
        }

        var records = await ParseAsync(context, payload);

        logger.LogInformation("{FileName} — {RecordCount} records parsed, fanning out",
            payload.FileName, records.Count);

        // PATTERN 2: Fan-out / Fan-in — process all records in parallel
        var processedRecords = await FanOutFanInProcessAsync(context, records);

        // Store individual records to LabResults container
        await StorePatientRecords(context, processedRecords);

        logger.LogInformation("All {RecordCount} records processed — storing batch summary",
            processedRecords.Length);

        // PATTERN 1 continues: Store summary
        var summary = await StoreSummaryAsync(context, processedRecords);

        logger.LogInformation(
            "Batch {BatchId} stored — {Total} records, {Abnormal} abnormal — starting confirmation monitor",
            summary.BatchId, summary.TotalRecords, summary.AbnormalCount);

        // Event Grid — publish custom event immediately when abnormal results are detected.
        // Fires before the monitor loop so downstream subscribers get early notification
        // without waiting for Cosmos confirmation. Any webhook, Logic App, or function
        // subscribed to the custom topic receives this independently of the Service Bus path.
        if (summary.AbnormalCount > 0)
            await context.CallActivityAsync(AppConfig.Activities.PublishAbnormalEvent, summary);

        // PATTERN 3: Monitor — poll until Cosmos confirms summary is written
        summary = await WaitForConfirmationAsync(context, summary, logger);

        // Service Bus — notify downstream systems via queue (all batches)
        await context.CallActivityAsync(AppConfig.Activities.PublishBatchComplete, summary);

        // Service Bus — publish to alert topic if any abnormal results (topics fan out to
        // multiple subscriptions; a SQL filter on critical-alerts limits to AbnormalCount > 5)
        if (summary.AbnormalCount > 0)
            await context.CallActivityAsync(AppConfig.Activities.PublishAbnormalAlert, summary);

        await MoveFileAsync(context, payload.FileName, AppConfig.Blob.ProcessedContainer,
            "Processing completed successfully");

        var durationSeconds = (context.CurrentUtcDateTime - startedAt).TotalSeconds;

        logger.LogInformation("Pipeline complete for {FileName} — batch {BatchId} — duration {DurationSeconds:F1}s",
            payload.FileName, summary.BatchId, durationSeconds);

        // Guard with IsReplaying so the metric is emitted exactly once, not on every replay pass
        if (!context.IsReplaying)
            _telemetryClient.TrackMetric(AppConfig.Metrics.PipelineDuration, durationSeconds,
                new Dictionary<string, string>
                {
                    [AppConfig.Metrics.Dimensions.FileName] = payload.FileName,
                    [AppConfig.Metrics.Dimensions.BatchId] = summary.BatchId,
                    [AppConfig.Metrics.Dimensions.Status] = summary.ConfirmationStatus.ToString()
                });

        return summary;
    }

    private static async Task StorePatientRecords(TaskOrchestrationContext context, ProcessedRecord[] processedRecords)
    {
        await context.CallActivityAsync(AppConfig.Activities.StoreRecords, processedRecords);
    }

    private static Task<ValidationResult> ValidateAsync(
        TaskOrchestrationContext context, FilePayload payload) =>
        context.CallActivityAsync<ValidationResult>(AppConfig.Activities.ValidateFile, payload);

    private static Task<List<LabRecord>> ParseAsync(
        TaskOrchestrationContext context, FilePayload payload) =>
        context.CallActivityAsync<List<LabRecord>>(AppConfig.Activities.ParseFile, payload);

    private static async Task<ProcessedRecord[]> FanOutFanInProcessAsync(
        TaskOrchestrationContext context, List<LabRecord> records)
    {
        // fan out
        var tasks = records
            .Select(r => context.CallActivityAsync<ProcessedRecord>(AppConfig.Activities.ProcessRecord, r))
            .ToList();

        // fan in
        return await Task.WhenAll(tasks);
    }

    private static Task<ProcessingSummary> StoreSummaryAsync(
        TaskOrchestrationContext context, ProcessedRecord[] records) =>
        context.CallActivityAsync<ProcessingSummary>(AppConfig.Activities.StoreSummary, records);

    private static async Task<ProcessingSummary> WaitForConfirmationAsync(
        TaskOrchestrationContext context, ProcessingSummary summary, ILogger logger)
    {
        const int maxAttempts = 10;
        var attempts = 0;

        while (summary.ConfirmationStatus != ConfirmationStatus.Confirmed
               && attempts < maxAttempts)
        {
            await context.CreateTimer(
                context.CurrentUtcDateTime.AddSeconds(30),
                CancellationToken.None);

            logger.LogInformation("Confirmation check {Attempt}/{MaxAttempts} for batch {BatchId}",
                attempts + 1, maxAttempts, summary.BatchId);

            var result = await context.CallActivityAsync<ProcessingSummary?>(
                AppConfig.Activities.CheckStorageConfirmation, summary.BatchId);

            if (result != null)
                summary = result;

            attempts++;
        }

        if (summary.ConfirmationStatus != ConfirmationStatus.Confirmed)
        {
            summary.ConfirmationStatus = ConfirmationStatus.TimedOut;
            await context.CallActivityAsync<ProcessingSummary>(AppConfig.Activities.WriteTimeoutSummary, summary);
            logger.LogWarning("Batch {BatchId} timed out after {Attempts} confirmation attempts",
                summary.BatchId, attempts);
        }
        else
        {
            logger.LogInformation("Batch {BatchId} confirmed after {Attempts} attempt(s)",
                summary.BatchId, attempts);
        }

        return summary;
    }

    private static Task MoveFileAsync(
        TaskOrchestrationContext context, string fileName, string targetContainer, string reason) =>
        context.CallActivityAsync(AppConfig.Activities.MoveFile, new FileArchiveRequest
        {
            FileName = fileName,
            TargetContainer = targetContainer,
            Reason = reason
        });
}