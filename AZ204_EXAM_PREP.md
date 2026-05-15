# AZ-204 Exam Prep — Exercise Guide

Based on the official study guide: https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/az-204

The README now contains a full reference section for each topic below. Use this guide to know **what to focus on and why it's tested**, then read the corresponding README section for the full concept explanation, code examples, and portal setup steps.

---

## Exam Domain Weights (official)

| Domain | Weight |
|---|---|
| Develop Azure compute solutions | 25–30% |
| Connect to and consume Azure services | 20–25% |
| Develop for Azure storage | 15–20% |
| Implement Azure security | 15–20% |
| Monitor and troubleshoot | 5–10% |

---

## What HealthDoc already covers well

| Exam bullet | Where in project |
|---|---|
| Run containers by using ACI | `HealthDoc.ReportGenerator/` |
| Publish image to ACR | `Dockerfile` + `container.yaml.example` |
| Implement Azure Functions app | All of `HealthDoc/` |
| Implement input and output bindings | `[CosmosDBOutput]`, `[BlobTrigger]`, `[ServiceBusTrigger]` throughout |
| Implement function triggers | HTTP, Blob, Timer, EventGrid, CosmosDB, ServiceBus triggers |
| Perform Cosmos DB SDK operations | `StorageConfirmationValidator.cs`, `ReportGenerator/Program.cs` |
| Implement change feed | `DownstreamSystemNotifier.cs` (`[CosmosDBTrigger]`) |
| Blob SDK operations | `MoveProcessedFile.cs`, `FailedLabFilesEndpoint.cs` |
| Create and implement shared access signatures | `FailedLabFilesEndpoint.cs` |
| Key Vault — keys, secrets, certificates | Key Vault references in App Settings, `AppConfig.cs` |
| Managed Identities | `DefaultAzureCredential` in `Program.cs`, RBAC assignments |
| Implement APIM | Two products, policies, named values, rate limiting, JWT validation |
| Implement Event Grid solutions | `EventGridLabResultAuditor.cs`, `AbnormalResultEventPublisher.cs` |
| Implement Service Bus solutions | Publishers + subscribers, queue + topic, SQL filters, DLQ |
| Monitor with Application Insights | `TelemetryClient` custom events and metrics, sampling |

---

## Gaps: what the exam tests that the project doesn't cover

These are the high-value exercises. Do them in order — the first two are in the 20–25% and 25–30% domains.

| Exercise | Time | Format |
|---|---|---|
| 1 — Event Hubs | ~45 min | Code + portal |
| 2 — Queue Storage | ~30 min | Code + portal |
| 3 — Blob lifecycle | ~20 min | Portal only |
| 4 — Cosmos DB consistency | ~20 min | One code change |
| 5 — SAS tokens | ~20 min | CLI only (code already done) |
| 6 — App Insights alerts | ~20 min | Portal only |
| **Total** | **~2.5 hours** | |

---

## Exercise 1 — Azure Event Hubs
**Exam bullet:** "Implement solutions that use Azure Event Hubs"
**Domain:** Connect to and consume Azure services (20–25%)
**Time:** ~45 min

> **README reference:** [§ Azure Event Hubs](README.md#azure-event-hubs) — full concept explanation, code samples, partition/consumer group diagrams, and portal setup steps.

Event Hubs is for high-throughput event streaming (millions of events/sec). It is distinct from Service Bus and Event Grid.

| | Service Bus | Event Grid | Event Hubs |
|---|---|---|---|
| Model | Durable messaging queue/topic | Push-based fan-out | High-throughput stream |
| Consumer | One message → one consumer (queue) | Independent subscribers | Consumer groups read the same stream |
| Replay | No (once consumed, gone) | No | Yes — events retained (default 1 day) |
| Best for | Reliable command/task delivery | Reactive notifications | Telemetry, logs, IoT, clickstreams |

### What to build

Add an Event Hubs producer and consumer to the pipeline. When a batch completes (after `StoreSummary`), publish a telemetry event to Event Hubs. Add a Functions trigger that reads from Event Hubs.

**Step 1 — Create Event Hubs namespace (portal):**
- Standard tier (Basic doesn't support consumer groups)
- Create an event hub named `lab-results-telemetry`
- Create a consumer group named `pipeline-analytics` (in addition to `$Default`)

**Step 2 — Install the SDK:**
```bash
dotnet add HealthDoc/HealthDoc.csproj package Azure.Messaging.EventHubs
```

**Step 3 — Add producer activity** (`HealthDoc/Pipeline/TelemetryPublisher.cs`):
```csharp
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using HealthDoc.Models;
using Microsoft.Azure.Functions.Worker;
using System.Text.Json;

namespace HealthDoc.Pipeline;

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
```

**Step 4 — Add consumer function** (`HealthDoc/Events/EventHubAnalyticsProcessor.cs`):
```csharp
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Events;

public class EventHubAnalyticsProcessor(ILogger<EventHubAnalyticsProcessor> logger)
{
    // cardinality: many — processes a batch of events per invocation
    [Function(nameof(EventHubAnalyticsProcessor))]
    public void Run(
        [EventHubTrigger(AppConfig.EventHub.Name,
                         Connection      = AppConfig.EventHub.Connection,
                         ConsumerGroup   = AppConfig.EventHub.ConsumerGroup)]
        string[] events)
    {
        foreach (var e in events)
            logger.LogInformation("Event Hub event received: {Event}", e);
    }
}
```

**Step 5 — Register `EventHubProducerClient` in `Program.cs`:**
```csharp
builder.Services.AddSingleton(sp =>
{
    var connection = Environment.GetEnvironmentVariable(AppConfig.EventHub.Connection)
        ?? throw new InvalidOperationException($"{AppConfig.EventHub.Connection} is not configured");
    return new EventHubProducerClient(connection, AppConfig.EventHub.Name);
});
```

**Step 6 — Add constants to `AppConfig.cs`:**
```csharp
public static class EventHub
{
    public const string Connection     = "EventHubConnectionString";
    public const string Name           = "lab-results-telemetry";
    public const string ConsumerGroup  = "pipeline-analytics";
}
```

**Step 7 — Wire the activity into `LabResultOrchestrator.cs`** (after `StoreSummaryAsync`):
```csharp
var summary = await StoreSummaryAsync(context, processedRecords);

// Publish a telemetry event to Event Hubs
await context.CallActivityAsync(AppConfig.Activities.PublishTelemetry, summary);
```

**Step 8 — Add to `local.settings.json`:**
```json
"EventHubConnectionString": "<event-hub-namespace-connection-string>"
```

### Key exam concepts

**Partitions and consumer groups:**
- Partitions: events are distributed across N partitions; each partition is an ordered, immutable log
- Consumer group: a logical view of the entire event hub; each consumer group reads independently from all partitions; `$Default` always exists
- Rule: one consumer group = one independent reader (e.g. analytics pipeline, archive pipeline, monitoring pipeline — each gets all events)

**`EventHubTrigger` cardinality:**
```csharp
// cardinality: one — one event per invocation
string eventData

// cardinality: many — batched (more efficient, recommended)
string[] eventData
```

**Event retention:** events are NOT deleted after consumption. They are retained for the configured duration (1–7 days). A new consumer group can read from the beginning (`EventPosition.Earliest`).

**Checkpointing:** the trigger tracks its position in each partition via a checkpoint blob. If the function restarts, it resumes from the last checkpoint — not the beginning. `[EventHubTrigger]` handles this automatically using `AzureWebJobsStorage` — no SDK checkpoint code required. Explicit checkpoint management (`EventProcessorClient`) is only needed for custom processors built outside of Azure Functions.

---

## Exercise 2 — Azure Queue Storage
**Exam bullet:** "Implement solutions that use Azure Queue Storage"
**Domain:** Connect to and consume Azure services (20–25%)
**Time:** ~30 min

> **README reference:** [§ Azure Queue Storage](README.md#azure-queue-storage) — Queue Storage vs Service Bus comparison table, trigger/output binding code, visibility timeout, and poison queue behaviour.

Queue Storage is a simple, durable message queue built into Azure Storage. It doesn't need a separate namespace — it uses the storage account you already have.

| | Service Bus Queue | Azure Queue Storage |
|---|---|---|
| Max message size | 256 KB (Standard) / 100 MB (Premium) | 64 KB |
| Max TTL | 14 days | 7 days |
| Dead-letter queue | Yes | No |
| Peek-lock | Yes | Yes (visibility timeout) |
| Message ordering | Guaranteed (sessions) | Best-effort FIFO |
| Best for | Enterprise messaging, DLQ, sessions | Simple queuing, large volume, low cost |

### What to build

Add a Queue Storage trigger that fires whenever a message is written to a queue. Write a message to the queue after a batch fails validation (currently only a blob move happens).

**Step 1 — Create the queue (portal or CLI):**
```bash
az storage queue create \
  --name lab-results-failures \
  --account-name <storage-account-name>
```

**Step 2 — Add an output binding to `FileValidator.cs`** (when validation fails, write to queue):

In the orchestrator, after the `ValidateFile` activity returns `IsValid = false`, call a new activity:

```csharp
// In LabResultOrchestrator.cs, after the validation failure branch:
await context.CallActivityAsync(AppConfig.Activities.NotifyFailureQueue, payload.FileName);
```

New activity (`HealthDoc/Pipeline/FailureQueueNotifier.cs`):
```csharp
using Microsoft.Azure.Functions.Worker;

namespace HealthDoc.Pipeline;

public class FailureQueueNotifier
{
    [Function(AppConfig.Activities.NotifyFailureQueue)]
    [QueueOutput(AppConfig.Queue.FailuresQueue, Connection = AppConfig.Blob.Connection)]
    public string Run([ActivityTrigger] string fileName)
    {
        return $"Validation failed: {fileName} at {DateTimeOffset.UtcNow:O}";
    }
}
```

**Step 3 — Add a queue trigger consumer** (`HealthDoc/Events/FailureQueueHandler.cs`):
```csharp
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HealthDoc.Events;

public class FailureQueueHandler(ILogger<FailureQueueHandler> logger)
{
    [Function(nameof(FailureQueueHandler))]
    public void Run(
        [QueueTrigger(AppConfig.Queue.FailuresQueue, Connection = AppConfig.Blob.Connection)]
        string message)
    {
        logger.LogWarning("Failure queue message: {Message}", message);
    }
}
```

**Step 4 — Add constants to `AppConfig.cs`:**
```csharp
public static class Queue
{
    public const string FailuresQueue = "lab-results-failures";
}
```

Also add `NotifyFailureQueue` to `AppConfig.Activities`.

### Key exam concepts

**Visibility timeout (the Queue Storage version of peek-lock):**
- When a consumer reads a message, it becomes invisible to other consumers for the visibility timeout period (default: 30 seconds)
- If the consumer doesn't delete the message before the timeout, it reappears and can be read again
- This is how Queue Storage achieves at-least-once delivery without a DLQ

**Poison messages:**
- After a message is dequeued 5 times without deletion, Queue Storage moves it to a `<queue-name>-poison` queue automatically
- `QueueTrigger` handles this automatically — no configuration needed

**Max message size: 64 KB.** For larger payloads, store the data in Blob Storage and put the blob reference in the queue (claim-check pattern).

**`[QueueOutput]` binding:** return the message string from the function (or use an `ICollector<string>` for multiple messages).

---

## Exercise 3 — Blob Storage Lifecycle Management
**Exam bullet:** "Implement storage policies and data lifecycle management"
**Domain:** Develop for Azure storage (15–20%)
**Time:** ~20 min (portal only)

> **README reference:** [§ Azure Blob Storage → Blob Storage Lifecycle Management](README.md#blob-storage-lifecycle-management) — access tiers, policy JSON structure, portal steps, and the "runs once per day" exam rule. Also covers [§ Blob Properties and Metadata](README.md#blob-properties-and-metadata) for the "Set and retrieve properties and metadata" exam bullet.

Lifecycle management policies automatically transition or delete blobs based on age and last-modified time — no code required.

### What to configure (portal)

Storage account → **Data management** → **Lifecycle management** → **Add rule**

**Rule 1 — Archive processed files after 30 days:**
```json
{
  "rules": [
    {
      "name": "archive-processed",
      "enabled": true,
      "type": "Lifecycle",
      "definition": {
        "filters": {
          "blobTypes": ["blockBlob"],
          "prefixMatch": ["lab-results-processed/"]
        },
        "actions": {
          "baseBlob": {
            "tierToCool": { "daysAfterModificationGreaterThan": 30 },
            "tierToArchive": { "daysAfterModificationGreaterThan": 90 },
            "delete": { "daysAfterModificationGreaterThan": 365 }
          }
        }
      }
    }
  ]
}
```

**Rule 2 — Delete failed files after 60 days:**
```json
{
  "name": "delete-failed",
  "definition": {
    "filters": { "blobTypes": ["blockBlob"], "prefixMatch": ["lab-results-failed/"] },
    "actions": { "baseBlob": { "delete": { "daysAfterModificationGreaterThan": 60 } } }
  }
}
```

### Key exam concepts

**Access tiers:**
| Tier | Cost | Retrieval | Use for |
|---|---|---|---|
| Hot | Highest storage | Cheapest | Frequently accessed data |
| Cool | Lower storage | Higher retrieval fee | Infrequently accessed, stored ≥ 30 days |
| Cold | Lower storage | Higher retrieval fee | Rarely accessed, stored ≥ 90 days |
| Archive | Cheapest storage | Hours to rehydrate | Long-term backup, stored ≥ 180 days |

**Rehydration from Archive:** a blob in Archive tier must be rehydrated (moved to Hot/Cool) before it can be read. This takes hours. Rehydration priority: Standard (up to 15 hours) or High (under 1 hour, higher cost).

**Lifecycle policy scope:** rules apply to block blobs. Filters use `prefixMatch` to target specific containers or virtual directories.

**Blob versioning + lifecycle:** if versioning is enabled, lifecycle rules can also target previous versions separately from the current version.

---

## Exercise 4 — Cosmos DB Consistency Levels
**Exam bullet:** "Set the appropriate consistency level for operations"
**Domain:** Develop for Azure storage (15–20%)
**Time:** ~20 min

> **README reference:** [§ Azure Cosmos DB → Consistency Levels](README.md#consistency-levels) — five-level comparison table, per-request SDK override in `StorageConfirmationValidator.cs`, and the multi-region writes restriction.

Cosmos DB offers five consistency levels — a tradeoff between consistency guarantees and latency/availability.

### The five levels (strong → weak)

| Level | Guarantee | Latency | Use case |
|---|---|---|---|
| **Strong** | Always reads the latest write | Highest | Financial transactions; cannot be used with multi-region writes |
| **Bounded Staleness** | Reads lag behind by at most K versions or T seconds | High | Near-real-time with a known lag window |
| **Session** *(default)* | Consistent within a single session (reads your own writes) | Low | Most apps — single-user workflows |
| **Consistent Prefix** | No out-of-order reads; may see stale data | Low | Event sourcing; ordering matters but lag is OK |
| **Eventual** | No ordering guarantee; eventually consistent | Lowest | Counts, aggregations, social "likes" |

### What to add to the project

The project uses the default consistency level (Session) set on the Cosmos DB account. Demonstrate overriding it at the request level in `StorageConfirmationValidator.cs`:

```csharp
// Current code — uses account-level default (Session)
var response = await _cosmosClient
    .GetDatabase(AppConfig.CosmosDb.Database)
    .GetContainer(AppConfig.CosmosDb.SummariesContainer)
    .ReadItemAsync<ProcessingSummary>(batchId, new PartitionKey(batchId));

// Override to Strong for this specific read (ensures we see the write that just happened)
var response = await _cosmosClient
    .GetDatabase(AppConfig.CosmosDb.Database)
    .GetContainer(AppConfig.CosmosDb.SummariesContainer)
    .ReadItemAsync<ProcessingSummary>(
        batchId,
        new PartitionKey(batchId),
        new ItemRequestOptions { ConsistencyLevel = ConsistencyLevel.Strong });
```

### Key exam concepts

**Session is the default.** Most exam questions about "what consistency level should you use for a shopping cart" → Session (reads your own writes).

**Strong consistency cannot be used with multi-region writes.** If your Cosmos account has multiple write regions enabled, Strong is not available.

**Consistency level hierarchy:** you can only *relax* the consistency level at the request level, not strengthen it. If the account default is Session, you can request Eventual on a specific query, but you cannot request Strong if the account default is Bounded Staleness.

**RU cost:** Stronger consistency = higher RU consumption. Eventual = lowest cost.

---

## Exercise 5 — SAS Tokens and Stored Access Policies
**Exam bullet:** "Create and implement shared access signatures"
**Domain:** Implement Azure security (15–20%)
**Time:** ~30 min

> **README reference:** [§ Authentication & Security → Stored Access Policies](README.md#stored-access-policies) — ad-hoc vs stored access policy SAS comparison, user delegation vs service SAS distinction, and CLI commands to create, use, and revoke a policy.

This exercise is already partially done — `FailedLabFilesEndpoint.cs` was updated to use **user delegation key SAS**. The remaining work is understanding and practicing the stored access policy (service SAS) flow via CLI.

### The two SAS types (exam critical)

| | User delegation SAS | Service SAS |
|---|---|---|
| Signed with | AAD credential (Managed Identity / az login) | Storage account key |
| Requires account key? | No | Yes |
| Supports stored access policies? | **No** | **Yes** |
| Revocable before expiry? | No | Yes — delete the policy |

**The most important exam question on SAS:** "How do you immediately revoke a SAS token that was already issued?"
**Answer:** Only possible with a service SAS that references a stored access policy. Delete the policy → 403 immediately, without rotating the account key.

### CLI exercises to run

```bash
# 1. Create a stored access policy on the failed-files container
az storage container policy create \
  --account-name <storage-account-name> \
  --container-name lab-results-failed \
  --name failed-read \
  --permissions r \
  --expiry 2027-01-01T00:00:00Z \
  --connection-string "<storage-connection-string>"

# 2. Generate a service SAS referencing the policy
az storage blob generate-sas \
  --account-name <storage-account-name> \
  --container-name lab-results-failed \
  --name <blob-name> \
  --policy-name failed-read \
  --output tsv \
  --connection-string "<storage-connection-string>"

# 3. Test the SAS URL — it works

# 4. Revoke by deleting the policy
az storage container policy delete \
  --account-name <storage-account-name> \
  --container-name lab-results-failed \
  --name failed-read \
  --connection-string "<storage-connection-string>"

# 5. Try the same SAS URL — 403 Forbidden immediately
```

### Max policies per container: 5

---

## Exercise 6 — App Insights Alerts (lowest priority — 5–10% domain)
**Exam bullet:** "Implement availability tests and alerts"
**Domain:** Monitor and troubleshoot (5–10%)
**Time:** ~20 min

> **README reference:** [§ Application Insights — KQL Queries and Alerts](README.md#application-insights--kql-queries-and-alerts) — key table schema, ready-to-run KQL queries, log alert setup, availability test setup, and log vs metric alert comparison.

### Create a log alert

Application Insights → Alerts → Create → Log search

```kusto
customEvents
| where name == "LabResultsProcessed"
| extend n = toint(customDimensions["AbnormalCount"])
| summarize total = sum(n) by bin(timestamp, 1h)
```

| Field | Value |
|---|---|
| Aggregation | Sum of `total` |
| Condition | Greater than 10 |
| Evaluation frequency | Every 5 minutes |
| Look-back period | 1 hour |

### Create an availability test

Application Insights → Availability → Add test → Standard test

- Test URL: your Function App's `/api/blobs/failed` endpoint (or any HTTP endpoint)
- Test frequency: every 5 minutes
- Locations: 3+ regions
- Success criteria: HTTP 200, response time < 5 seconds

**Availability tests** make HTTP requests to your endpoint from multiple Azure regions on a schedule and alert you if the endpoint goes down or becomes slow.

### Log alert vs metric alert

| | Metric alert | Log alert |
|---|---|---|
| Data source | Pre-aggregated platform metrics | Arbitrary KQL against raw logs |
| Latency | Near-real-time | Depends on ingestion window |
| Cost | Lower | Higher (KQL runs on each evaluation) |
| Use for | CPU, memory, request rate thresholds | Custom business events, complex conditions |

---

## Priority order for tomorrow

If time is short, focus on these in order:

1. **Exercise 1 — Event Hubs** — 20–25% domain, zero coverage in project, explicitly tested
2. **Exercise 2 — Queue Storage** — 20–25% domain, zero coverage, explicitly tested
3. **Exercise 5 — SAS tokens** — 15–20% domain, already half done (code is fixed), just run the CLI
4. **Exercise 3 — Blob lifecycle** — 15–20% domain, portal only, 20 min
5. **Exercise 4 — Cosmos DB consistency** — 15–20% domain, one SDK call to understand
6. **Exercise 6 — App Insights alerts** — 5–10% domain, do last
