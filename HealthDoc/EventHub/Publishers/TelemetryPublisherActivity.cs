namespace HealthDoc.EventHub.Publishers;

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using System.Text.Json;

public class TelemetryPublisher(EventHubProducerClient producer)
{
    [Function(AppConfig.Activities.PublishTelemetry)]
    public async Task Run([ActivityTrigger] ProcessingSummary summary)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(new
        {
            summary.BatchId,
            summary.ClinicId,
            summary.TotalRecords,
            summary.AbnormalCount,
            PublishedAt = DateTimeOffset.UtcNow
        });

        var batch = await producer.CreateBatchAsync();
        batch.TryAdd(new EventData(payload));
        await producer.SendAsync(batch);
    }
}