# HealthDoc

A regional healthcare network receives lab result CSV files from partner clinics daily. Previously, a staff member manually downloaded each file, validated it, entered results into the system, and emailed a confirmation back to the clinic — a process that was slow, error-prone, and impossible to scale. HealthDoc replaces that workflow with a fully automated ingestion pipeline: a CSV upload to Azure Blob Storage triggers an Azure Durable Functions orchestration that validates, parses, processes, stores, and confirms each batch without human intervention.

Built as an AZ-204 exam study project. Every section of this README maps to an exam topic.

Co-authored with [Claude](https://claude.ai) (Anthropic).

---

## Architecture

```
Partner Clinic
    │
    │  uploads lab_results_*.csv
    ▼
Azure Blob Storage ──── BlobTrigger ────► LabResultIngestionTrigger
(lab-results-incoming)                            │
                                                  │ ScheduleNewOrchestrationInstanceAsync
                                                  ▼
                                         LabResultOrchestrator
                                                  │
                          ┌───────────────────────┤  Function Chaining
                          ▼                       ▼
                     ValidateFile            ParseFile
                    (invalid?)               (List<LabRecord>)
                          │                       │
                          │ MoveFile               │  Fan-out
                          ▼          ┌────────────┼────────────┐
               lab-results-failed    ▼            ▼            ▼
                                ProcessRecord   ...      ProcessRecord
                                          │                    │
                                          └──────────┬─────────┘
                                                     │  Fan-in (Task.WhenAll)
                                                     │
                                          ┌──────────┴──────────┐
                                          ▼                     ▼
                                     StoreRecords          StoreSummary
                                    (LabResultRecords)           │  Cosmos DB Output Binding
                                                                 ▼
                                                          Cosmos DB ◄──── CheckStorageConfirmation
                                                       (LabResults /          (Monitor Pattern —
                                                    ProcessingSummaries)    polls up to 10× / 30s)
                                                     │                    │
                                                     │ MoveFile      (timeout) WriteTimeoutSummary
                                                     ▼
                                          lab-results-processed

                                               Cosmos DB ──── CosmosDBTrigger ────► NotifyDownstreamSystems
                                            (ProcessingSummaries)                   (App Insights telemetry)

HTTP Client  ──── GET /api/status/{instanceId} ────► GetBatchStatus
(caller)                                              (DurableClient — queries instance state)
                                                      202 Accepted  → still running, poll again
                                                      200 OK        → completed
                                                      500           → failed / terminated
```

---

## Azure Services

| Service | Role in This Project | AZ-204 Topic |
|---|---|---|
| **Azure Blob Storage** | Receives CSV uploads; triggers the pipeline; archives processed/failed files | Blob triggers, storage bindings, server-side copy |
| **Azure Durable Functions** | Orchestrates the multi-step pipeline with state | Durable task framework |
| **Azure Cosmos DB** | Persists processing summaries; polled for confirmation; triggers downstream notification | NoSQL output binding, SDK queries, CosmosDB trigger |
| **Application Insights** | Telemetry collection with sampling; business event tracking | Monitoring and diagnostics |

---

## Durable Functions Patterns

Patterns 1–3 are implemented inside `LabResultOrchestrator.cs`. Pattern 4 is a standalone HTTP function in `BatchStatusEndpoint.cs`.

### Pattern 1 — Function Chaining

Activities execute sequentially. The output of each step is the input to the next.

```
ValidateFile(payload)
    └─► ParseFile(payload)          [only if valid]
            └─► StoreSummary(records)
```

The orchestrator short-circuits immediately if `ValidateFile` returns `IsValid = false`, calling `MoveFile` to archive the blob to `lab-results-failed` and returning a failed `ProcessingSummary` without touching Cosmos DB. This is the **early exit** variant of function chaining.

**Exam concept:** Guaranteed ordering, sequential dependency, deterministic replay.

### Pattern 2 — Fan-out / Fan-in

Each `LabRecord` parsed from the CSV is dispatched to a `ProcessRecord` activity independently. All N activities run in parallel. `Task.WhenAll` is the fan-in point that blocks until every record is processed before aggregation.

```csharp
var processingTasks = records.Select(r =>
    context.CallActivityAsync<ProcessedRecord>("ProcessRecord", r));

var results = await Task.WhenAll(processingTasks);  // fan-in
```

**Exam concept:** Parallel activity dispatch, Task.WhenAll, fan-out/fan-in topology.

### Pattern 3 — Monitor

After `StoreSummary` writes to Cosmos DB via output binding, the orchestrator enters a polling loop to confirm the document was fully persisted. It uses a durable timer between checks — not `Thread.Sleep` — so the orchestrator can survive a host restart mid-poll.

```
for attempt in 0..9:
    await context.CreateTimer(30 seconds)     ← durable, replay-safe
    result = await CheckStorageConfirmation(batchId)
    if result.ConfirmationStatus == Confirmed → break

if not Confirmed → set TimedOut, call WriteTimeoutSummary activity
```

On timeout, the orchestrator delegates the final Cosmos write to the `WriteTimeoutSummary` activity rather than performing I/O directly — keeping the orchestrator deterministic and replay-safe.

The `ConfirmationStatus` enum tracks lifecycle: `Unknown → Confirmed | TimedOut`.

**Exam concept:** Monitor pattern, durable timers, external resource polling, status tracking.

### Pattern 4 — Async HTTP API (HTTP Polling Consumer)

`GetBatchStatus` exposes a single HTTP endpoint that lets any caller check on an orchestration instance by its ID. It uses the `[DurableClient]` binding to call `GetInstanceAsync`, then maps the runtime status to an appropriate HTTP response:

| Runtime status | HTTP response | Meaning |
|---|---|---|
| `Completed` | `200 OK` + serialized output | Pipeline finished — result in body |
| `Failed` | `500` + serialized output | Orchestrator threw an unhandled exception |
| `Terminated` | `500 Terminated` | Instance was forcibly stopped |
| Any other | `202 Accepted` | Still running — caller should poll again |

The `202` response is the key exam detail: returning `Accepted` (not `200`) signals to the client that the work is in progress and the same URL should be polled until a terminal status arrives.

**Exam concept:** `[DurableClient]` binding, `GetInstanceAsync`, HTTP polling consumer, `OrchestrationRuntimeStatus` values.

---

## Project Structure

```
HealthDoc/
├── HealthDoc.sln
│
├── HealthDoc/                              # Azure Functions isolated worker app
│   ├── Program.cs                          # DI setup — CosmosClient + BlobServiceClient as singletons
│   ├── AppConfig.cs                        # Centralized const strings for containers, databases, connections
│   ├── host.json                           # Application Insights sampling config
│   ├── Functions/
│   │   ├── LabResultIngestionTrigger.cs    # BlobTrigger entry point → schedules orchestration
│   │   ├── LabResultOrchestrator.cs        # Orchestrator (patterns 1–3)
│   │   ├── BatchStatusEndpoint.cs          # HTTP GET /api/status/{instanceId} (pattern 4)
│   │   └── DownstreamSystemNotifier.cs     # CosmosDBTrigger → App Insights telemetry
│   └── Activities/
│       ├── FileValidator.cs                # ValidateFile — checks headers and data rows
│       ├── FileParser.cs                   # ParseFile — CSV → List<LabRecord>
│       ├── LabRecordProcessor.cs           # ProcessRecord — enriches one record
│       ├── SummaryUpdater.cs               # StoreSummary — Cosmos DB output binding
│       ├── StorageConfirmationValidator.cs # CheckStorageConfirmation — Cosmos SDK query
│       ├── TimeoutSummaryWriter.cs         # WriteTimeoutSummary — persists timed-out status
│       ├── MoveProcessedFile.cs            # MoveFile — server-side blob copy + delete
│       └── PatientResultUpdater.cs         # StoreRecords — writes ProcessedRecords to LabResultRecords
│
├── HealthDoc.Models/                       # Shared models (no Azure dependency)
│   ├── FilePayload.cs
│   ├── FileArchiveRequest.cs
│   ├── LabRecord.cs                        # + static From(string[]) factory
│   ├── ProcessedRecord.cs                  # + static From(LabRecord) factory
│   ├── ProcessingSummary.cs
│   ├── ConfirmationStatus.cs
│   └── ValidationResult.cs
│
├── HealthDoc.Tests/                        # xUnit tests (net10.0)
│   ├── LabRecordTests.cs
│   └── ProcessedRecordTests.cs
│
└── lab_results_2024_05_01.csv             # Sample input file
```

---

## Data Flow

1. **Upload** — A partner clinic uploads a CSV to the `lab-results-incoming` blob container.

2. **Trigger** — `LabResultIngestionTrigger` fires via `BlobTrigger`. It reads the blob content, wraps it in a `FilePayload`, and schedules a new `LabResultOrchestrator` instance.

3. **Validate** — The orchestrator calls `ValidateFile`. If required columns are missing or any `Result` field is non-numeric, it calls `MoveFile` to archive the blob to `lab-results-failed` and returns a failed `ProcessingSummary`. No records are processed.

4. **Parse** — `ParseFile` splits the CSV into rows, skips the header, and maps each row to a `LabRecord` via `LabRecord.From(string[])`.

5. **Process (parallel)** — The orchestrator fans out: one `ProcessRecord` activity per `LabRecord`. Each activity calls `ProcessedRecord.From(record)`, which parses the `ReferenceRange` (e.g., `"4.0-5.6"`), determines `IsAbnormal`, and generates a Cosmos-ready document ID.

6. **Persist records** — `StoreRecords` writes the full `ProcessedRecord[]` array to the `LabResultRecords` Cosmos DB container via output binding, creating one document per patient result.

7. **Aggregate** — `StoreSummary` receives the same `ProcessedRecord[]` array, counts totals and abnormals, and writes a `ProcessingSummary` to the `ProcessingSummaries` container via output binding with `ConfirmationStatus = Unknown`.

8. **Confirm** — The monitor loop calls `CheckStorageConfirmation` up to 10 times (30-second durable timers). Each call queries Cosmos DB directly via `CosmosClient`. On success, `ConfirmationStatus` is set to `Confirmed`. After 10 failed attempts, `ConfirmationStatus` is set to `TimedOut` and the orchestrator calls the `WriteTimeoutSummary` activity to persist the timed-out status to Cosmos.

9. **Archive** — `MoveFile` copies the source blob from `lab-results-incoming` to `lab-results-processed` (server-side copy via `StartCopyFromUriAsync`) and deletes it from the source container.

10. **Notify** — `NotifyDownstreamSystems` is triggered automatically by the `CosmosDBTrigger` on `ProcessingSummaries`. It emits a structured `LabResultsProcessed` event to Application Insights for each completed batch.

---

## Models

| Model | Produced by | Consumed by | Key fields |
|---|---|---|---|
| `FilePayload` | `LabResultIngestionTrigger` | `LabResultOrchestrator` | `FileName`, `Content` |
| `FileArchiveRequest` | Orchestrator | `MoveFile` | `FileName`, `TargetContainer`, `Reason` |
| `ValidationResult` | `ValidateFile` | Orchestrator (gate) | `IsValid`, `Errors` |
| `LabRecord` | `ParseFile` (via `From`) | `ProcessRecord` | `Result`, `ReferenceRange`, `CollectedAt` |
| `ProcessedRecord` | `ProcessRecord` (via `From`) | `StoreRecords`, `StoreSummary` | `IsAbnormal`, `Id`, `ProcessedAt` |
| `ProcessingSummary` | `StoreSummary` | Monitor loop → Cosmos DB → `NotifyDownstreamSystems` | `BatchId`, `AbnormalCount`, `ConfirmationStatus` |

`ProcessedRecord` inherits from `LabRecord`. Both expose a static `From(...)` factory method so the mapping logic can be unit tested independently of Azure Functions.

---

## Sample CSV

```csv
ClinicId,PatientId,TestCode,Result,Unit,ReferenceRange,CollectedAt
CLINIC_001,PAT_123,HBA1C,6.2,%,4.0-5.6,2024-05-01T08:30:00
CLINIC_001,PAT_124,HBA1C,5.1,%,4.0-5.6,2024-05-01T09:00:00
CLINIC_001,PAT_125,GLUCOSE,210,mg/dL,70-100,2024-05-01T09:15:00
```

Rows 1 and 3 will be flagged `IsAbnormal = true` (6.2 > 5.6 and 210 > 100).

---

## Running Locally

**Prerequisites:** .NET 10 SDK, Azure Functions Core Tools v4, Azurite or a real Azure Storage account, a Cosmos DB account.

**`local.settings.json`** (not committed — create manually):

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "<storage-connection-string>",
    "FUNCTIONS_WORKER_RUNTIME": "dotnet-isolated",
    "StorageConnectionString": "<storage-connection-string>",
    "CosmosDBConnectionString": "<cosmos-connection-string>"
  }
}
```

Blob Storage containers required:
- `lab-results-incoming` — drop CSV files here to trigger the pipeline
- `lab-results-processed` — successfully processed files are moved here
- `lab-results-failed` — files that fail validation are moved here

Cosmos DB setup required:
- Database: `LabResults`
- Container: `ProcessingSummaries` with partition key `/id`
- Container: `LabResultRecords` with partition key `/id`

```bash
dotnet restore
dotnet build
cd HealthDoc
func start --port 7220
```

Upload `lab_results_2024_05_01.csv` to the `lab-results-incoming` blob container to trigger a run.

---

## Running Tests

```bash
dotnet test HealthDoc.Tests/HealthDoc.Tests.csproj
```

10 tests across two classes:

- `LabRecordTests` — `From` maps all CSV columns; `From` trims whitespace
- `ProcessedRecordTests` — base field mapping, composite ID format, `IsAbnormal` boundary cases (in-range, at boundary, below min, above max), `ProcessedAt` timestamp precision

---

## AZ-204 Concepts Checklist

- [ ] **Isolated worker model** — `Program.cs`, `HealthDoc.csproj` (`dotnet-isolated` runtime)
- [ ] **Blob trigger** — `LabResultIngestionTrigger.cs` (`BlobTrigger` on `lab-results-incoming/{name}`)
- [ ] **Durable orchestration** — `LabResultOrchestrator.cs` (`[OrchestrationTrigger]`, deterministic replay rules)
- [ ] **Activity functions** — 8 activities, each decorated with `[Function]` + `[ActivityTrigger]`
- [ ] **Function chaining** — sequential `ValidateFile → ParseFile → StoreSummary`
- [ ] **Fan-out / Fan-in** — parallel `ProcessRecord` × N, `Task.WhenAll` fan-in
- [ ] **Monitor pattern** — polling loop with `context.CreateTimer()` (durable, replay-safe)
- [ ] **Async HTTP API pattern** — `BatchStatusEndpoint.cs`, `[DurableClient]` binding, `202 Accepted` polling response
- [ ] **Cosmos DB output binding** — `SummaryUpdater.cs` + `TimeoutSummaryWriter.cs` (`[CosmosDBOutput]` attribute)
- [ ] **Cosmos DB SDK** — direct `CosmosClient` query in `StorageConfirmationValidator.cs`
- [ ] **Cosmos DB trigger** — `DownstreamSystemNotifier.cs` (`[CosmosDBTrigger]` fires on new documents in `ProcessingSummaries`)
- [ ] **Dependency injection** — singleton `CosmosClient` and `BlobServiceClient` registered in `Program.cs`
- [ ] **Application Insights** — sampling config in `host.json`; `TelemetryClient` for custom business events in `FileValidator.cs` and `DownstreamSystemNotifier.cs`
- [ ] **Centralized configuration** — `AppConfig.cs` (`const` strings required for C# attribute parameters at compile time; `Metrics` nested class centralizes metric names and dimension keys)
- [ ] **Structured logging** — `ILogger<T>` injected throughout all activities and orchestrator; 8+ log points across the pipeline
