# HealthDoc

A regional healthcare network receives lab result CSV files from partner clinics daily. Previously, a staff member manually downloaded each file, validated it, entered results into the system, and emailed a confirmation back to the clinic — a process that was slow, error-prone, and impossible to scale.

HealthDoc replaces that workflow with a fully automated ingestion pipeline. Partner clinics POST CSV files through Azure API Management, which triggers an Azure Durable Functions orchestration that validates, parses, processes, stores, and confirms each batch without human intervention. Processed results are cached in Redis, abnormal findings are broadcast via Service Bus and Event Grid, and every upload is recorded in an audit log. An internal React dashboard lets staff review failed files and query results, authenticated through Azure AD.

Built as an AZ-204 exam study project — every service and pattern maps directly to an exam domain.

Co-authored with [Claude](https://claude.ai) (Anthropic).

---

## Table of Contents

1. [Azure Services](#azure-services)
2. [Architecture](#architecture) — [Ingestion Pipeline](#ingestion-pipeline) · [Partner Clinic Queries](#partner-clinic-queries) · [Internal Dashboard](#internal-dashboard)
3. [Project Structure](#project-structure)
4. [The Pipeline](#the-pipeline)
5. [Durable Functions Patterns](#durable-functions-patterns)
6. [Local Development](#local-development)
7. [Azure Cosmos DB](#azure-cosmos-db)
8. [Azure Blob Storage](#azure-blob-storage)
9. [Azure API Management](#azure-api-management)
10. [Authentication & Security](#authentication--security)
11. [Azure Service Bus](#azure-service-bus)
12. [Azure Queue Storage](#azure-queue-storage)
13. [Azure Event Grid](#azure-event-grid)
14. [Azure Event Hubs](#azure-event-hubs)
15. [Azure Managed Redis](#azure-managed-redis)
16. [Azure Container Instances](#azure-container-instances)
17. [End-to-End Testing](#end-to-end-testing)
18. [Application Insights — KQL Queries and Alerts](#application-insights--kql-queries-and-alerts)
19. [AZ-204 Coverage Map](#az-204-coverage-map)

---

## Azure Services

| Service | Role in This Project | AZ-204 Topic |
|---|---|---|
| **Azure Functions** | HTTP upload endpoint, blob trigger, orchestrator, activity functions, Cosmos DB trigger, Event Grid trigger, Service Bus trigger, failed file listing | Isolated worker model, triggers, output bindings |
| **Azure Durable Functions** | Orchestrates the multi-step pipeline with durable state | Function chaining, fan-out/fan-in, monitor, async HTTP API |
| **Azure Blob Storage** | Receives uploaded CSVs; archives processed/failed files; SAS token generation | Blob triggers, storage bindings, server-side copy, SAS tokens |
| **Azure Cosmos DB** | Persists lab records, processing summaries, and audit logs | NoSQL output binding, SDK queries, CosmosDB trigger |
| **Azure API Management** | Gateway for all HTTP endpoints: two products (subscription key, JWT), rate limiting, named values, response caching | APIM policies, products, subscriptions, named values, validate-jwt |
| **Azure Active Directory** | Issues JWT tokens for internal users; two app registrations with delegated scope `LabResults.Read` | App registrations, OAuth 2.0 scopes, OIDC |
| **Azure Key Vault** | Stores connection strings as secrets; app settings reference them at runtime | Key Vault secrets, Key Vault references, soft-delete |
| **Managed Identity** | Function App authenticates to Cosmos, Storage, and Key Vault without connection strings | System-assigned identity, DefaultAzureCredential, RBAC |
| **Azure Service Bus** | Delivers batch-complete notifications (queue) and abnormal-result alerts (topic with subscriptions) | Queues, topics, subscriptions, DLQ, peek-lock |
| **Azure Queue Storage** | Simple durable queue for validation failure notifications; uses existing storage account | Queue trigger, output binding, visibility timeout, poison queue |
| **Azure Event Grid** | Push-based fan-out: blob created audit events (system) and abnormal result detected events (custom) | System events, custom events, CloudEvents, subscription filters |
| **Azure Event Hubs** | High-throughput telemetry stream; pipeline analytics consumer group reads batch completion events | Event hub trigger, producer client, partitions, consumer groups, checkpointing |
| **Azure Managed Redis** | Cache-aside on lab results queries; write-invalidation on new record writes | Cache-aside pattern, TTL, eviction, IConnectionMultiplexer |
| **Application Insights** | Telemetry collection with sampling; custom business events and pipeline metrics | Monitoring, custom events, structured logging |
| **MSAL (React SPA)** | Internal dashboard authenticates via authorization code + PKCE; silent token renewal | MSAL auth flows, token acquisition, cache strategy |
| **Azure Container Registry** | Stores the report generator Docker image; pulled by ACI on demand | Registry tiers, image push/pull, admin credentials vs RBAC |
| **Azure Container Instances** | Runs the report generator as a one-shot batch job: queries Cosmos, writes CSV to Blob, exits | Restart policies, secure env vars, scale to zero, batch workloads |

---

## Architecture

### Ingestion Pipeline

When a partner clinic uploads a CSV, the file flows through APIM into an automated processing pipeline. The upload endpoint writes the blob and starts the Durable orchestration directly. The EventGrid trigger independently receives the `BlobCreated` event from the same write and records an audit log — neither path knows about the other. The orchestrator runs validation, parsing, parallel record processing, persistence, and downstream notification as a durable, replay-safe workflow.

```
Partner Clinic  ──── POST /labs/upload ────► Azure API Management
                     Content-Type: text/csv    (subscription key auth, rate limit,
                     Ocp-Apim-Subscription-Key  x-functions-key injected)
                                                        │
                                                        ▼
                                             UploadLabResultsEndpoint
                                             (generates unique filename,
                                              writes blob to lab-results-incoming,
                                              starts orchestration directly)
                                                        │
                                               ┌────────┴────────┐
                                               │ StartOrchestration  │ blob write
                                               ▼                     ▼
                                      LabResultOrchestrator    EventGridLabResultAuditor
                                                               (EventGridTrigger — BlobCreated)
                                                               writes AuditLog → Cosmos DB
                                    │
                 ┌──────────────────┤  Function Chaining
                 ▼                  ▼
            ValidateFile        ParseFile
           (invalid?)           (List<LabRecord>)
                 │                  │
                 │ MoveFile         │  Fan-out
                 ▼      ┌───────────┼───────────┐
        lab-results-    ▼           ▼           ▼
           failed   ProcessRecord  ...     ProcessRecord
                             │                  │
                             └────────┬─────────┘
                                      │  Fan-in (Task.WhenAll)
                                      │
                           ┌──────────┴──────────┐
                           ▼                     ▼
                      StoreRecords           StoreSummary
                    (LabResultRecords)    (ProcessingSummaries)
                    + Redis invalidate     + CosmosDB Output
                                               │
                              ┌────────────────┤ (if AbnormalCount > 0)
                              ▼                ▼
                   CheckStorageConfirmation  PublishAbnormalEvent
                   (Monitor — polls up to    (Event Grid custom topic)
                    10× / 30s durable timer)
                              │
                   PublishBatchComplete ──► Service Bus queue ──► ServiceBusLabResultNotifier
                   PublishAbnormalAlert ──► Service Bus topic ──► clinical-alerts subscription
                                                                ► critical-alerts (AbnormalCount > 5)
                              │
                              ▼
                   MoveFile → lab-results-processed
                              │
                   CosmosDBTrigger ──► DownstreamSystemNotifier (App Insights telemetry)
```

### Partner Clinic Queries

After uploading, clinics poll for status using the instance ID returned at upload time, and can query processed results by clinic ID. Both endpoints sit behind the Clinic Standard APIM product, using the same subscription key as the upload endpoint.

```
Partner Clinic  ──── GET /labs/status/{instanceId} ────► Azure API Management
                     GET /labs/results/{clinicId}         (Clinic Standard product —
                     Ocp-Apim-Subscription-Key             subscription key auth)
                                                                    │
                                                       ┌────────────┴────────────┐
                                                       ▼                         ▼
                                                GetBatchStatus          LabResultsEndpoint
                                           (DurableClient —            (checks Redis first,
                                            GetInstanceAsync)           falls back to Cosmos)
                                                       │
                                           202 Accepted  → still running
                                           200 OK        → completed
                                           500           → failed / terminated
```

### Internal Dashboard

Internal staff sign in through the React SPA using MSAL. The acquired access token is passed to APIM, where the Internal Dashboard product validates it against Azure AD before forwarding the request to the Function App. No subscription key is required.

```
Internal User  ──── Sign In (MSAL) ────► Azure Active Directory
(HealthDoc.Dashboard)                        │ access token (LabResults.Read scope)
                                             ▼
               GET /labs/failed-files ───► Azure API Management
               GET /labs/results/{id}      (Internal Dashboard product —
               Authorization: Bearer        validate-jwt, no subscription key)
                                                       │
                                          ┌────────────┴────────────┐
                                          ▼                         ▼
                                FailedLabFilesEndpoint     LabResultsEndpoint
                                (blob list + SAS URLs)     (Redis → Cosmos)
```

---

## Project Structure

```
HealthDoc/
├── HealthDoc.sln
│
├── HealthDoc/                                  # Azure Functions isolated worker app
│   ├── Program.cs                              # DI — all SDK clients registered as singletons
│   ├── AppConfig.cs                            # Centralized const strings for all services
│   ├── host.json                               # Application Insights sampling config
│   ├── Http/                                   # HTTP triggers
│   │   ├── UploadLabResultsEndpoint.cs         # POST /api/upload → blob write + starts orchestration → instanceId
│   │   ├── BatchStatusEndpoint.cs              # GET /api/status/{instanceId} — async HTTP API
│   │   ├── LabResultsEndpoint.cs               # GET /api/results/{clinicId} — Redis → Cosmos
│   │   └── FailedLabFilesEndpoint.cs           # GET /api/blobs/failed → blob list + SAS URLs
│   ├── Pipeline/                               # Orchestrator + activities (core processing)
│   │   ├── LabResultOrchestrator.cs            # Orchestrator — all four Durable patterns
│   │   ├── FileValidator.cs                    # ValidateFile — checks headers and data rows
│   │   ├── FileParser.cs                       # ParseFile — CSV → List<LabRecord>
│   │   ├── LabRecordProcessor.cs               # ProcessRecord — enriches one record
│   │   ├── SummaryUpdater.cs                   # StoreSummary — Cosmos DB output binding
│   │   ├── StorageConfirmationValidator.cs     # CheckStorageConfirmation — Cosmos SDK query
│   │   ├── TimeoutSummaryWriter.cs             # WriteTimeoutSummary — persists timed-out status
│   │   ├── MoveProcessedFile.cs                # MoveFile — server-side blob copy + delete
│   │   └── PatientResultUpdater.cs             # StoreRecords — Cosmos write + Redis invalidation
│   ├── ServiceBus/                             # Service Bus publishers and subscribers
│   │   ├── BatchCompletePublisher.cs           # PublishBatchComplete — queue output binding
│   │   ├── AbnormalAlertPublisher.cs           # PublishAbnormalAlert — topic output binding
│   │   ├── ServiceBusLabResultNotifier.cs      # ServiceBusTrigger (queue) → App Insights event
│   │   ├── ClinicalAlertHandler.cs             # ServiceBusTrigger (clinical-alerts sub) → App Insights event
│   │   ├── CriticalAlertHandler.cs             # ServiceBusTrigger (critical-alerts sub, AbnormalCount > 5) → LogWarning
│   │   └── ServiceBusDeadLetterMonitor.cs      # TimerTrigger → peeks DLQ every 5 minutes
│   └── Events/                                 # Blob, EventGrid, and Cosmos triggers + Event Grid publisher
│       ├── LabResultIngestionTrigger.cs        # BlobTrigger → schedules orchestration (inactive on Flex Consumption)
│       ├── EventGridLabResultAuditor.cs        # EventGridTrigger (BlobCreated) → AuditLog
│       ├── AbnormalResultEventPublisher.cs     # PublishAbnormalEvent — Event Grid custom event
│       └── DownstreamSystemNotifier.cs         # CosmosDBTrigger → App Insights telemetry
│
├── HealthDoc.Models/                           # Shared models — no Azure dependency
│   ├── LabRecord.cs                            # CSV row + static From(string[]) factory
│   ├── ProcessedRecord.cs                      # Enriched record + static From(LabRecord) factory
│   ├── ProcessingSummary.cs                    # Batch-level summary written to Cosmos
│   ├── ConfirmationStatus.cs                   # Unknown → Confirmed | TimedOut enum
│   ├── ValidationResult.cs                     # IsValid + Errors list
│   ├── FilePayload.cs                          # FileName + Content passed to orchestrator
│   ├── FileArchiveRequest.cs                   # FileName + TargetContainer + Reason for MoveFile
│   ├── FailedFileInfo.cs                       # Blob name + SAS URL + created timestamp
│   ├── BatchCompletedMessage.cs                # Service Bus message payload
│   ├── AbnormalResultEvent.cs                  # Event Grid custom event data
│   └── LabAuditRecord.cs                       # Audit log document written to Cosmos
│
├── HealthDoc.Tests/                            # xUnit tests (net10.0)
│   ├── LabRecordTests.cs                       # From factory — column mapping, whitespace
│   └── ProcessedRecordTests.cs                 # IsAbnormal boundary cases, ID format, timestamp
│
├── HealthDoc.Dashboard/                        # Internal React/TypeScript SPA (Vite + MSAL)
│   ├── src/
│   │   ├── main.tsx                            # MsalProvider wrapper
│   │   ├── App.tsx                             # Auth gate — login or dashboard
│   │   ├── authConfig.ts                       # MSAL config, API scope, APIM base URL
│   │   ├── hooks/useApiToken.ts                # Silent token acquisition with popup fallback
│   │   └── components/
│   │       ├── Dashboard.tsx                   # Tab shell — shows logged-in user
│   │       ├── FailedFilesPanel.tsx            # Failed CSVs with SAS download links
│   │       └── ResultsPanel.tsx                # Clinic ID search → processed records table
│   └── .env.example                            # Required env vars (tenant ID, client IDs, APIM URL)
│
├── HealthDoc.ReportGenerator/                  # .NET 10 console app — ACI batch job
│   ├── Program.cs                              # Query Cosmos → build CSV → write to blob → exit
│   ├── Dockerfile                              # Multi-stage: dotnet/sdk build → dotnet/runtime serve
│   ├── .dockerignore                           # Excludes bin/ and obj/
│   ├── container.yaml.example                  # ACI deployment YAML template (values redacted)
│   └── HealthDoc.ReportGenerator.csproj
│
└── lab_results_2024_05_01.csv                  # Sample input file
```

---

## The Pipeline

A single CSV upload flows through these steps end-to-end:

1. **Upload**: A partner clinic POSTs a CSV body to `POST /labs/upload` through APIM. The APIM inbound policy injects an `X-Clinic-Id` header set to `context.Subscription.Id` — callers never supply it themselves. `UploadLabResultsEndpoint` reads this header, generates a unique filename (`lab-results-{clinicId}-{timestamp}-{shortGuid}.csv`), writes it to `lab-results-incoming`, and passes `ClinicId` explicitly in `FilePayload` so the authoritative value flows through the entire orchestration. The endpoint returns `{ "instanceId": "<filename>" }` for status polling.

2. **Orchestration trigger**: `UploadLabResultsEndpoint` starts the orchestration directly after writing the blob, passing the file content and generated filename as a `FilePayload`. The filename is the deterministic instance ID, so the caller can begin polling immediately.

   > **Note — `LabResultIngestionTrigger`:** The project also contains a `BlobTrigger`-based function (`LabResultIngestionTrigger.cs`) originally designed as the orchestration entry point. The **Flex Consumption SKU** does not support the polling-based `BlobTrigger`; it requires an EventGrid-based blob trigger instead. Rather than setting up an EventGrid event subscription for the blob container, the orchestration start was consolidated into the HTTP upload endpoint. `LabResultIngestionTrigger` remains in the project as a reference and could be activated on a Consumption, Premium, or Dedicated plan, or by migrating it to use `Source = BlobTriggerSource.EventGrid` on Flex Consumption.

3. **Audit trigger**: `EventGridLabResultAuditor` (EventGridTrigger) independently receives the `Microsoft.Storage.BlobCreated` system event for the same upload and writes a `LabAuditRecord` to the `AuditLog` Cosmos container. Neither trigger knows about the other; the audit trail is fully decoupled from the processing pipeline.

4. **Validate**: The orchestrator calls `ValidateFile`. If required columns are missing or any `Result` field is non-numeric, it calls `MoveFile` to archive the blob to `lab-results-failed` and returns a failed `ProcessingSummary`. No records are processed.

5. **Parse**: `ParseFile` splits the CSV into rows, skips the header, and maps each row to a `LabRecord` via `LabRecord.From(string[])`. The `ClinicId` column in the CSV is parsed for schema validation but immediately overridden with `FilePayload.ClinicId` — the APIM-injected value is the authority, not the caller-supplied CSV column.

6. **Process (parallel)**: The orchestrator fans out, dispatching one `ProcessRecord` activity per `LabRecord`. Each activity calls `ProcessedRecord.From(record)`, which parses the `ReferenceRange` (e.g. `"4.0-5.6"`), determines `IsAbnormal`, and generates a Cosmos-ready document ID.

7. **Persist**: `StoreRecords` writes all `ProcessedRecord` documents to `LabResultRecords` via output binding and invalidates the Redis cache key for the affected clinic, so the next results query fetches fresh data. `StoreSummary` aggregates totals and abnormal counts into a `ProcessingSummary`, writing it to `ProcessingSummaries` with `ConfirmationStatus = Unknown`.

8. **Publish events**: If the batch contains abnormal results, `AbnormalResultEventPublisher` immediately publishes a `HealthDoc.Lab.AbnormalResultDetected` CloudEvent to the Event Grid custom topic, giving any subscriber early notification before the confirmation monitor runs.

9. **Confirm**: The monitor loop calls `CheckStorageConfirmation` up to 10 times with 30-second durable timers between attempts, querying Cosmos directly via the SDK. On success it sets `ConfirmationStatus = Confirmed`; after 10 failures it sets `TimedOut` and delegates the final Cosmos write to `WriteTimeoutSummary`.

10. **Notify**: `BatchCompletePublisher` sends a `BatchCompletedMessage` to the `lab-results-notifications` Service Bus queue, consumed by `ServiceBusLabResultNotifier`. If abnormal results exist, `AbnormalAlertPublisher` sends the same message to the `lab-results-alerts` topic, which fans it out to the `clinical-alerts` subscription (all messages) and `critical-alerts` (SQL filter: `AbnormalCount > 5`). Separately, `DownstreamSystemNotifier` fires from the Cosmos DB trigger on `ProcessingSummaries` and emits a structured event to Application Insights.

   > **Why publishers use explicit SDK sends instead of `[ServiceBusOutput]` bindings:** In the .NET isolated worker model, a Durable activity function's return value is intercepted by the Durable runtime to pass the result back to the orchestrator. When the orchestrator calls `CallActivityAsync` without a type parameter (return value not needed), the runtime still consumes the return value — the `[ServiceBusOutput]` binding never sees it and messages are silently dropped. The `DURABLE2002` analyzer warning ("CallActivityAsync is expecting return type 'none'") is the signal that this conflict exists. The fix is to inject `ServiceBusClient` and call `sender.SendMessageAsync` directly, making delivery explicit rather than relying on a binding that cannot fire. See [Durable Functions .NET isolated worker overview](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-dotnet-isolated-overview).

11. **Archive**: `MoveFile` copies the blob from `lab-results-incoming` to `lab-results-processed` via server-side copy (`StartCopyFromUriAsync`) and deletes the source.

### Data Models

| Model | Produced by | Consumed by | Key fields |
|---|---|---|---|
| `FilePayload` | `UploadLabResultsEndpoint` | `LabResultOrchestrator` | `FileName`, `Content` |
| `FileArchiveRequest` | Orchestrator | `MoveFile` | `FileName`, `TargetContainer`, `Reason` |
| `ValidationResult` | `ValidateFile` | Orchestrator (gate) | `IsValid`, `Errors` |
| `LabRecord` | `ParseFile` (via `From`) | `ProcessRecord` | `Result`, `ReferenceRange`, `CollectedAt` |
| `ProcessedRecord` | `ProcessRecord` (via `From`) | `StoreRecords`, `StoreSummary` | `IsAbnormal`, `Id`, `ProcessedAt` |
| `ProcessingSummary` | `StoreSummary` | Monitor loop, Service Bus, App Insights | `BatchId`, `AbnormalCount`, `ConfirmationStatus` |
| `BatchCompletedMessage` | `BatchCompletePublisher` | `ServiceBusLabResultNotifier` | `BatchId`, `ClinicId`, `AbnormalCount` |
| `AbnormalResultEvent` | `AbnormalResultEventPublisher` | Event Grid subscribers | `BatchId`, `ClinicId`, `AbnormalCount` |
| `LabAuditRecord` | `EventGridLabResultAuditor` | `AuditLog` Cosmos container | `ClinicId`, `FileName`, `EventType`, `BlobUrl`, `ReceivedAt` |

`ProcessedRecord` inherits from `LabRecord`. Both expose a static `From(...)` factory so the mapping logic can be unit tested without any Azure dependency.

---

## Durable Functions Patterns

All four patterns are implemented in this project. Patterns 1–3 are inside `LabResultOrchestrator.cs`; Pattern 4 is `BatchStatusEndpoint.cs`.

### Pattern 1 — Function Chaining

Activities execute sequentially: the output of each step is the input to the next.

```
ValidateFile(payload)
    └─► ParseFile(payload)          [only if valid]
            └─► StoreSummary(records)
```

The orchestrator short-circuits if `ValidateFile` returns `IsValid = false`: it calls `MoveFile` to archive the blob and returns a failed summary without touching Cosmos DB. This is the **early exit** variant of function chaining.

**Exam concept:** Guaranteed ordering, sequential dependency, deterministic replay.

### Pattern 2 — Fan-out / Fan-in

Each `LabRecord` is dispatched to a `ProcessRecord` activity independently. All N activities run in parallel. `Task.WhenAll` is the fan-in point; the orchestrator blocks here until every record is processed.

```csharp
var tasks = records.Select(r =>
    context.CallActivityAsync<ProcessedRecord>(AppConfig.Activities.ProcessRecord, r));

var results = await Task.WhenAll(tasks);  // fan-in
```

**Exam concept:** Parallel activity dispatch, `Task.WhenAll`, fan-out/fan-in topology.

### Pattern 3 — Monitor

After `StoreSummary` writes to Cosmos, the orchestrator polls until the document is confirmed persisted. It uses durable timers (not `Thread.Sleep`), so the orchestrator survives a host restart mid-poll.

```
for attempt in 0..9:
    await context.CreateTimer(30 seconds)     ← durable, replay-safe
    result = await CheckStorageConfirmation(batchId)
    if result.ConfirmationStatus == Confirmed → break

if not Confirmed → set TimedOut, call WriteTimeoutSummary activity
```

On timeout, the final Cosmos write is delegated to the `WriteTimeoutSummary` activity (not performed by the orchestrator directly), keeping it deterministic and replay-safe.

**Exam concept:** Monitor pattern, durable timers, external resource polling, status tracking.

### Pattern 4 — Async HTTP API

`BatchStatusEndpoint` lets any caller check orchestration status by instance ID. The instance ID is the blob filename returned by the upload endpoint; the client holds it and polls until a terminal status arrives.

| Runtime status | HTTP response | Meaning |
|---|---|---|
| `Completed` | `200 OK` + `{ "status": "Completed", "instanceId": "..." }` | Pipeline finished |
| `Failed` | `500` + `{ "status": "Failed", "instanceId": "..." }` | Orchestrator threw an exception |
| `Terminated` | `500` + `{ "status": "Terminated", "instanceId": "..." }` | Instance was forcibly stopped |
| Not found (`null`) | `404 Not Found` | Unknown instance ID |
| Any other | `202 Accepted` | Still running — poll again |

The `202 Accepted` response is the key exam detail: it signals the client to keep polling the same URL. Returning `404` for an unknown instance ID is important — without this guard, a missing orchestration is indistinguishable from one that is still running, causing the client to poll forever.

`SerializedOutput` (the orchestrator's return value) is intentionally not returned here. Fetching it requires `getInputsAndOutputs: true` on `GetInstanceAsync`, which is expensive for large payloads. Callers that need the processed records use `GET /labs/results/{clinicId}` instead.

**Exam concept:** `[DurableClient]` binding, `GetInstanceAsync`, HTTP polling consumer, `OrchestrationRuntimeStatus` values.

---

## Local Development

### Prerequisites

- .NET 10 SDK
- Azure Functions Core Tools v4
- Azurite (local storage emulator) or a real Azure Storage account
- Cosmos DB account
- Redis (Docker: `docker run -d -p 6379:6379 redis`)

### Configuration

Create `HealthDoc/local.settings.json` (not committed):

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "<storage-connection-string>",
    "FUNCTIONS_WORKER_RUNTIME": "dotnet-isolated",
    "APPLICATIONINSIGHTS_CONNECTION_STRING": "<app-insights-connection-string>",

    "CosmosDBConnectionString": "<cosmos-connection-string>",
    "CosmosDBEndpoint": "https://<account>.documents.azure.com:443/",

    "StorageConnectionString": "<storage-connection-string>",
    "StorageAccountEndpoint": "https://<account>.blob.core.windows.net/",

    "KeyVaultEndpoint": "https://kv-health-doc-dev.vault.azure.net/",

    "ServiceBusConnectionString": "<service-bus-connection-string>",

    "EventGridTopicEndpoint": "https://<topic>.eventgrid.azure.net/api/events",
    "EventGridTopicKey": "<topic-access-key>",

    "RedisConnectionString": "localhost:6379"
  }
}
```

There are two separate consumers of these settings, and they authenticate differently:

| Setting | Consumer | How it authenticates |
|---|---|---|
| `CosmosDBEndpoint`, `StorageAccountEndpoint` | SDK clients (`CosmosClient`, `BlobServiceClient`) | `DefaultAzureCredential` — `az login` locally, Managed Identity in Azure. No secret needed. |
| `CosmosDBConnectionString`, `StorageConnectionString` | Binding attributes (`[CosmosDBOutput]`, `[BlobTrigger]`, etc.) | Connection string resolved by the Functions runtime. Still needed locally. |

**Why Key Vault doesn't eliminate local connection strings:** `@Microsoft.KeyVault(...)` references are resolved by the Azure Functions host reading live App Settings from the Azure portal. `local.settings.json` is a flat file read directly by the local host; there is no Key Vault resolution. So locally, the actual connection string values are always required for binding attributes.

In Azure, the connection string app settings are replaced with Key Vault references and the runtime resolves them transparently. The SDK clients don't use connection strings in either environment; they use `DefaultAzureCredential` throughout.

> Run `az login` before starting the host so `DefaultAzureCredential` can resolve the developer credential for the SDK clients.

### Azure Resource Setup

**Blob Storage containers:**
- `lab-results-incoming` — upload endpoint writes here; BlobTrigger fires automatically
- `lab-results-processed` — successfully processed files moved here
- `lab-results-failed` — validation failures moved here

**Cosmos DB — database `LabResults`:**
- `ProcessingSummaries` — partition key `/id`
- `LabResultRecords` — partition key `/ClinicId`
- `AuditLog` — partition key `/ClinicId`

**Service Bus namespace (Standard tier):**
- Queue: `lab-results-notifications`
- Topic: `lab-results-alerts` with subscriptions `clinical-alerts` (no filter) and `critical-alerts` (SQL filter: `AbnormalCount > 5`)

### Running

```bash
dotnet restore
dotnet build
cd HealthDoc
func start --port 7220
```

Upload a CSV to trigger the full pipeline:

```bash
curl -X POST http://localhost:7220/api/upload \
  -H "Content-Type: text/csv" \
  -H "x-functions-key: <your-local-function-key>" \
  -H "x-clinic-id: CLINIC-01" \
  --data-binary @lab_results_2024_05_01.csv
```

In production APIM injects `X-Clinic-Id` automatically from the subscription. For local testing, pass it manually.

Response `201 Created`:

```json
{ "instanceId": "lab-results-CLINIC-01-20260508213642-8582e72b.csv" }
```

Poll status using the returned `instanceId`:

```bash
curl http://localhost:7220/api/status/lab-results-CLINIC-01-20260508213642-8582e72b.csv \
  -H "x-functions-key: <your-local-function-key>"
```

Returns `202 Accepted` while running, `200 OK` with a `ProcessingSummary` JSON body when complete.

### Sample CSV

```csv
ClinicId,PatientId,TestCode,Result,Unit,ReferenceRange,CollectedAt
CLINIC_001,PAT_123,HBA1C,6.2,%,4.0-5.6,2024-05-01T08:30:00
CLINIC_001,PAT_124,HBA1C,5.1,%,4.0-5.6,2024-05-01T09:00:00
CLINIC_001,PAT_125,GLUCOSE,210,mg/dL,70-100,2024-05-01T09:15:00
```

Rows 1 and 3 will be flagged `IsAbnormal = true` (6.2 > 5.6 and 210 > 100), triggering the Service Bus alert topic and Event Grid custom event.

### Tests

```bash
dotnet test HealthDoc.Tests/HealthDoc.Tests.csproj
```

10 tests across two classes:
- `LabRecordTests` — `From` maps all CSV columns; `From` trims whitespace
- `ProcessedRecordTests` — base field mapping, composite ID format, `IsAbnormal` boundary cases (in-range, at boundary, below min, above max), `ProcessedAt` timestamp precision

---

## Azure Cosmos DB

Cosmos DB is the primary data store for the pipeline. Three containers live inside a single `LabResults` database, each with a partition key chosen to make the most common query a single-partition operation.

### Portal Setup

**Create the Cosmos DB account** — Portal → **Create a resource** → search **Azure Cosmos DB** → select **Azure Cosmos DB for NoSQL**:

| Field | Value |
|---|---|
| Account name | `health-doc-database-account` (globally unique) |
| Region | Same as your Function App |
| Capacity mode | **Serverless** |

Serverless is ideal for a study project — you pay per request unit consumed with no minimum, and there is no hourly charge when idle.

**AZ-204 capacity mode comparison:**

| Mode | Best for | Billing |
|---|---|---|
| **Serverless** | Sporadic or unpredictable traffic, dev/test | Per RU consumed |
| **Provisioned throughput** | Steady, predictable workloads | Per RU/s provisioned (whether used or not) |
| **Autoscale** | Variable workloads with a known ceiling | Scales between 10% and 100% of max RU/s |

**Create the database and containers** — once the account is provisioned, open **Data Explorer** → **New Container**:

| Container | Database | Partition key | Notes |
|---|---|---|---|
| `ProcessingSummaries` | `LabResults` (create new) | `/id` | Each summary is looked up by its own ID |
| `LabResultRecords` | `LabResults` (use existing) | `/ClinicId` | Queries filter by clinic — all records for one clinic stay in one partition |
| `AuditLog` | `LabResults` (use existing) | `/ClinicId` | Written by `EventGridLabResultAuditor` on every blob upload |

> **Partition key design:** `/ClinicId` on `LabResultRecords` means `SELECT * WHERE ClinicId = 'X'` is a single-partition query with no cross-partition fan-out. `/id` on `ProcessingSummaries` distributes summaries evenly — they are always looked up by their own ID, so no partition affinity is needed.

**RBAC:** The Function App's managed identity needs the `Cosmos DB Built-in Data Contributor` data plane role to read and write documents. This is covered in [Authentication & Security](#authentication--security) — the role must be assigned via CLI and does not appear in the portal IAM blade.

### Consistency Levels

Cosmos DB offers five consistency levels — a sliding scale between strong consistency guarantees and low latency. The level is configured on the account and can be relaxed (but not strengthened) per individual request.

| Level | Guarantee | Latency | RU cost | Use case |
|---|---|---|---|---|
| **Strong** | Always reads the latest committed write | Highest | Highest | Financial transactions; single-region or no multi-region writes |
| **Bounded Staleness** | Reads lag by at most K versions or T seconds | High | High | Near-real-time with a known staleness window |
| **Session** *(default)* | Reads your own writes within a session | Low | Low | Most apps — single-user or single-session flows |
| **Consistent Prefix** | No out-of-order reads; may see stale data | Low | Low | Event sourcing where ordering matters but lag is acceptable |
| **Eventual** | No ordering or recency guarantee | Lowest | Lowest | Counts, likes, aggregations where temporary stale data is fine |

The account-level default is **Session**. Override per request in `StorageConfirmationValidator.cs` to ensure the monitor loop reads the write that just happened:

```csharp
var response = await _cosmosClient
    .GetDatabase(AppConfig.CosmosDb.Database)
    .GetContainer(AppConfig.CosmosDb.SummariesContainer)
    .ReadItemAsync<ProcessingSummary>(
        batchId,
        new PartitionKey(batchId),
        new ItemRequestOptions { ConsistencyLevel = ConsistencyLevel.Strong });
```

**AZ-204 exam rule:** You can only *weaken* consistency at the request level — if the account default is Session, you can request Eventual on a specific read, but you cannot request Strong. If the account is configured for Strong, you cannot use multi-region writes. Session is the correct answer for most exam scenarios involving a single user or single application session.

---

## Azure Blob Storage

### How It Fits Into the Pipeline

Blob Storage is the entry and exit point for every lab result file. Four containers handle the full lifecycle of a CSV from upload to archival:

```
lab-results-incoming   ← UploadLabResultsEndpoint writes the raw CSV here
                          EventGridLabResultAuditor fires on BlobCreated (audit trail)
                          LabResultIngestionTrigger fires on new blob (inactive on Flex Consumption)

lab-results-processed  ← MoveProcessedFile copies here after successful pipeline run
lab-results-failed     ← MoveProcessedFile copies here after validation failure
lab-results-reports    ← ReportGenerator (ACI) writes CSV reports here on demand
```

`MoveProcessedFile.cs` uses a **server-side copy** — `StartCopyFromUriAsync` followed by `DeleteAsync` on the source. This moves the file within the same storage account without transferring bytes through the Function host.

### Access Tiers

Blobs in Azure Storage are assigned an access tier that controls cost. Tiers can be set at the storage account level (default) or overridden per blob.

| Tier | Storage cost | Retrieval cost | Minimum duration | Best for |
|---|---|---|---|---|
| **Hot** | Highest | Lowest | None | Frequently accessed files |
| **Cool** | Lower | Higher | 30 days | Infrequently accessed; early deletion fee applies |
| **Cold** | Lower | Higher | 90 days | Rarely accessed; lower cost than Cool |
| **Archive** | Lowest | Highest + rehydration | 180 days | Long-term retention; not directly readable |

**Rehydration from Archive:** a blob in Archive tier is offline and cannot be read until rehydrated. Rehydration copies the blob to Hot or Cool tier. Two priorities:
- **Standard** — up to 15 hours
- **High** — under 1 hour (higher cost)

**AZ-204 exam rule:** Archive blobs cannot be read directly — you must rehydrate first. Attempting to download an Archive blob returns `409 Conflict`. Rehydration is a copy operation; the original Archive blob remains until explicitly deleted.

### Blob Storage Lifecycle Management

A lifecycle management policy automatically transitions or deletes blobs based on age, removing the need for manual cleanup or scheduled jobs. Policies run once per day.

Policy structure: a rule has a **filter** (which blobs it applies to) and **actions** (what to do at each age threshold).

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
            "tierToCool":    { "daysAfterModificationGreaterThan": 30 },
            "tierToArchive": { "daysAfterModificationGreaterThan": 90 },
            "delete":        { "daysAfterModificationGreaterThan": 365 }
          }
        }
      }
    },
    {
      "name": "delete-failed",
      "enabled": true,
      "type": "Lifecycle",
      "definition": {
        "filters": {
          "blobTypes": ["blockBlob"],
          "prefixMatch": ["lab-results-failed/"]
        },
        "actions": {
          "baseBlob": {
            "delete": { "daysAfterModificationGreaterThan": 60 }
          }
        }
      }
    }
  ]
}
```

**AZ-204 exam rule:** Lifecycle policies run once per day. A blob that already passed an age threshold when the policy was first created is processed on the next evaluation cycle — the policy is not applied retroactively to all existing blobs on creation.

### Blob Properties and Metadata

Every blob has two kinds of descriptive data, both retrievable without downloading the blob body.

**System properties** — set and maintained by Azure Storage:

| Property | Description |
|---|---|
| `ContentType` | MIME type (e.g. `text/csv`) |
| `ContentLength` | Blob size in bytes |
| `ETag` | Opaque string; changes on every write; use for optimistic concurrency |
| `LastModified` | UTC timestamp of the last write |

**User-defined metadata** — arbitrary key-value string pairs set by your application:

```csharp
// Write metadata
var blobClient = containerClient.GetBlobClient(blobName);
await blobClient.SetMetadataAsync(new Dictionary<string, string>
{
    { "ClinicId", clinicId },
    { "BatchId",  batchId }
});

// Read metadata (without downloading the blob body)
var props = await blobClient.GetPropertiesAsync();
var clinicId = props.Value.Metadata["ClinicId"];
```

**AZ-204 exam rule:** Metadata values are always strings. Metadata is not indexed — you cannot use `GetBlobsAsync` to filter by metadata values. To find blobs by metadata, you must list all blobs and filter client-side, or use Azure Blob Index tags (a separate feature that supports server-side filtering).

### Portal Setup

Container setup is covered in [Local Development](#local-development). The additional setup for this section is the lifecycle management policy:

1. Storage account → **Data management** → **Lifecycle management** → **Add rule**
2. Enter rule name `archive-processed`, click **Next**
3. Filter: **Blob type** = Block blobs, **Prefix** = `lab-results-processed/`
4. Actions: set **Tier to cool** at 30 days, **Tier to archive** at 90 days, **Delete blob** at 365 days
5. Save, then repeat for `delete-failed` with only a **Delete blob** action at 60 days

---

## Azure API Management

APIM sits in front of all HTTP endpoints. External partner clinics use one product; internal staff use another. The Function App URL is never exposed to either. APIM is the only entry point.

### Why APIM

| Problem without APIM | Solution with APIM |
|---|---|
| Revoking one clinic's access means rotating the Function key, breaking all other clinics | Each clinic gets its own subscription key — revoke one without touching others |
| A misbehaving clinic can flood the system | Per-subscription rate limits enforced at the gateway |
| No visibility into which clinic calls what, how often | Every request logged with subscription context |
| External partners can see internal Azure hostnames and routes | The Function App URL is an internal detail — clinics only see the APIM gateway URL |

### Public vs Internal URL

```
Client calls:    https://apim-healthdoc-dev.azure-api.net/labs/upload
APIM forwards:   https://<func-app>.azurewebsites.net/api/upload
```

The `/labs` prefix is a domain concept visible to clients. The `/api` prefix is an Azure Functions implementation detail. Setting the **Web service URL** to `https://<func-app>.azurewebsites.net/api` absorbs the Functions prefix once; operation URL overrides stay clean (`/upload`, `/status/{id}`, `/results/{clinicId}`).

### Portal Setup

#### Step 1 — Create the APIM Instance

Search **API Management** → **Create**.

| Field | Value |
|---|---|
| Resource name | `apim-healthdoc-dev` (globally unique) |
| Region | same as your Function App |
| Pricing tier | **Consumption** |
| Organization name | HealthDoc |

> Consumption tier is pay-per-call with no hourly charge, ideal for study. Provisioning takes ~5 minutes.

**AZ-204 SKU comparison:**

| Tier | Cold starts | VNet support | Scale | Best for |
|---|---|---|---|---|
| Consumption | Yes (~2 s) | No | Auto | Dev, testing |
| Developer | No | Yes | Manual | Non-production exploration |
| Basic | No | No | Manual | Low-traffic production |
| Standard | No | No | Manual | Medium production |
| Premium | No | Yes | Manual + zone redundancy | Enterprise production |

#### Step 2 — Create the Named Value for the Function Key

Named values are APIM's encrypted key-value store. Policies reference them as `{{Name}}`; the value is never visible to callers.

**Named values** → **Add**:

| Field | Value |
|---|---|
| Name | `FunctionAppKey` |
| Type | **Secret** |
| Value | Function App host key (Function App → **App keys** → `default`) |

#### Step 3 — Create the Lab Results API

**APIs** → **Add API** → **HTTP**:

| Field | Value |
|---|---|
| Display name | `Lab Results API` |
| Web service URL | `https://<your-func-app>.azurewebsites.net/api` |
| API URL suffix | `labs` |

Add three operations:

| Operation | Method | URL |
|---|---|---|
| Upload Lab Results | `POST` | `/upload` |
| Get Processing Status | `GET` | `/status/{instanceId}` |
| Get Lab Results | `GET` | `/results/{clinicId}` |

#### Step 4 — Apply Policies

**API-level policy** (all operations):

```xml
<policies>
    <inbound>
        <base />
        <set-header name="x-functions-key" exists-action="override">
            <value>{{FunctionAppKey}}</value>
        </set-header>
    </inbound>
    <outbound>
        <base />
        <set-header name="x-functions-key" exists-action="delete" />
        <set-header name="x-powered-by" exists-action="delete" />
    </outbound>
</policies>
```

**Operation-level: Upload** (rate limit + Content-Type guard):

```xml
<inbound>
    <base />
    <!-- rate-limit-by-key requires Developer tier or above; included here as a study reference -->
    <rate-limit-by-key calls="10" renewal-period="60"
        counter-key="@(context.Subscription.Id)"
        increment-condition="@(context.Response.StatusCode == 201)" />
    <choose>
        <when condition='@(!context.Request.Headers.GetValueOrDefault("Content-Type", "").Contains("text/csv"))'>
            <return-response>
                <set-status code="415" reason="Unsupported Media Type" />
                <set-body>Content-Type must be text/csv</set-body>
            </return-response>
        </when>
    </choose>
</inbound>
```

**Operation-level: Get Lab Results** (response caching):

```xml
<inbound>
    <base />
    <!-- cache-lookup is a no-op on Consumption without an External Cache linked -->
    <!-- link Redis under APIM → External cache to activate on Consumption tier  -->
    <cache-lookup vary-by-developer="false" vary-by-developer-groups="false"
                  allow-private-response-caching="true" />
</inbound>
<outbound>
    <base />
    <cache-store duration="60" />
</outbound>
```

#### Step 5 — Create Products and Subscriptions

**Clinic Standard product** (external clinics):

| Field | Value |
|---|---|
| Display name | `Clinic Standard` |
| Requires subscription | ✅ |
| APIs | Lab Results API |

Create one subscription per clinic (`clinic-001-test`, scope: Clinic Standard). Each clinic receives a unique key — revoke one without affecting others.

> **Subscription name as clinic identifier:** The subscription name becomes the `ClinicId` on every document written by that clinic — Cosmos records, audit logs, Redis cache keys, and Service Bus messages all use it. This project assumes the name is agreed upon at provisioning time and communicated to the clinic alongside their subscription key. A name like `clinic-001` is more meaningful than the subscription ID (a GUID) and stable for the lifetime of the subscription.

**Clinic Standard product-level policy:**

```xml
<policies>
    <inbound>
        <base />
        <set-header name="x-clinic-id" exists-action="override">
            <value>@(context.Subscription.Name)</value>
        </set-header>
    </inbound>
    <outbound>
        <base />
        <set-header name="x-clinic-id" exists-action="delete" />
    </outbound>
</policies>
```

Both the injection and the strip are scoped to the Clinic Standard product: only subscribers of this product send uploads, so only their requests need the header set. The Internal Dashboard product has no `x-clinic-id` concern at all. Stripping on the way out keeps the header from leaking back to callers — it exists solely for the Function App's benefit.

---

## Authentication & Security

HealthDoc uses three distinct authentication models depending on who is calling and what they need.

### External Clinics: Subscription Keys

Partner clinics authenticate with an `Ocp-Apim-Subscription-Key` header. Subscription keys are the right choice here because the unit of identity is the clinic as a whole — one key per clinic, provisioned and revoked by the platform team, with no requirement for clinics to adopt any identity provider.

**When JWT would be better than subscription keys:**

| Scenario | Why JWT wins |
|---|---|
| **Multiple users per clinic** | JWT claims/roles allow per-user permissions; subscription keys give all clinic staff identical access |
| **Clinic has an Azure AD tenant** | B2B federation lets clinics log in with their own org credentials |
| **Audit requirements** | JWT carries `sub`/`oid` claims — log exactly who uploaded, not just which clinic |
| **Short-lived credentials** | Tokens expire (typically 1 hour) and refresh automatically; subscription keys require manual rotation if compromised |

**The key distinction: subscription keys authenticate a system; JWT tokens authenticate a person.** In this project, clinics are the unit of trust — subscription keys are correct and simpler.

### Internal Users: Azure AD & MSAL

Internal staff access the dashboard through a React SPA that authenticates with Azure AD via MSAL, then passes the access token to APIM, where a `validate-jwt` policy verifies it before the request reaches the Function App. See the [Internal Dashboard](#internal-dashboard) diagram for the full flow.

#### App Registrations

**Register `HealthDoc-API`:**
1. Azure Active Directory → App registrations → New registration (single tenant)
2. Expose an API → Add a scope: `LabResults.Read`, who can consent: Admins and users
3. Note the `api://<api-client-id>` Application ID URI

**Register `HealthDoc-Dashboard`:**
1. New registration (single tenant)
2. Add platform: Single-page application, redirect URI: `http://localhost:5173`
3. API permissions → `HealthDoc-API` → `LabResults.Read` → Grant admin consent

#### APIM Internal Dashboard Product

**Products** → **Add**:

| Field | Value |
|---|---|
| Display name | `Internal Dashboard` |
| Requires subscription | **off** |
| Published | on |

Add two operations: `GET /labs/failed-files` and `GET /labs/results/{clinicId}`.

Apply the `validate-jwt` policy **at the product level**, not the API level:

```xml
<validate-jwt header-name="Authorization"
              failed-validation-httpcode="401"
              failed-validation-error-message="Unauthorized. Valid Azure AD token required.">
    <openid-config url="https://login.microsoftonline.com/{{TenantId}}/v2.0/.well-known/openid-configuration" />
    <audiences>
        <audience>api://{{ApiClientId}}</audience>
    </audiences>
</validate-jwt>
```

Add named values `TenantId` and `ApiClientId` for the policy to reference.

> **Why product level, not API level?** Policies stack — every request passes through `Product → API → Operation` in sequence. If `validate-jwt` were placed at the API level, it would run for both products, forcing external clinics to present a JWT token on top of their subscription key. At the product level it only runs for requests that arrive through the Internal Dashboard product.

**Policy execution order for each product:**

| Layer | Clinic Standard | Internal Dashboard |
|---|---|---|
| Product | Subscription key validated | `validate-jwt` checks Azure AD token |
| API | `x-functions-key` injected, headers cleaned | `x-functions-key` injected, headers cleaned |
| Operation | Rate limit + Content-Type guard (upload only) | — |

#### Frontend Setup

```bash
cd HealthDoc.Dashboard
cp .env.example .env
# Fill in VITE_TENANT_ID, VITE_SPA_CLIENT_ID, VITE_API_CLIENT_ID, VITE_APIM_BASE_URL
npm install
npm run dev
```

Navigate to `http://localhost:5173`. After signing in, the dashboard shows two tabs:
- **Failed Files**: lists CSVs that failed validation, each with a one-hour SAS download link
- **Lab Results**: enter a Clinic ID to query processed records

**Verify end-to-end:**
1. Upload an invalid CSV — it lands in `lab-results-failed`
2. Sign in to the dashboard, open **Failed Files** — the file appears with a working download link
3. Call `GET /labs/failed-files` without a token → `401 Unauthorized`
4. Call the same endpoint with a valid token (copy from browser DevTools) → `200 OK`

### Application Identity: Key Vault & Managed Identity

The Function App itself authenticates to Cosmos DB, Blob Storage, and Key Vault using its Managed Identity — no connection strings or shared keys anywhere in deployed code.

**The problem with plaintext credentials:** Connection strings stored in app settings, environment variables, or accidentally committed config files give full storage account and database access to anyone who reads them. Key Vault and Managed Identity eliminate the secret entirely from the deployed environment.

| Layer | Problem | Solution |
|---|---|---|
| **At rest** | Secrets in config files and app settings | Secrets stored in Key Vault; app settings hold a reference, not the value |
| **In transit** | App authenticates with a shared key anyone can copy | App authenticates using its Azure identity — no secret to steal or rotate |

#### Authentication in Code

`Program.cs` registers both SDK clients using `DefaultAzureCredential` and a service endpoint URI instead of a connection string:

```csharp
var credential = new DefaultAzureCredential();

// No connection string — authenticates via Managed Identity (Azure) or az login (local)
new CosmosClient(endpoint, credential);
new BlobServiceClient(new Uri(endpoint), credential);
```

Binding attributes (`[CosmosDBOutput]`, `[BlobTrigger]`, etc.) still reference named connection string settings because the Functions runtime resolves these — not the SDK. In Azure, those app settings use **Key Vault references** instead of storing the secret value directly.

A Key Vault reference is a special app setting value in the format:
```
@Microsoft.KeyVault(SecretUri=https://<vault>.vault.azure.net/secrets/<secret-name>/)
```
The Functions runtime detects this format, fetches the real value from Key Vault at startup using the Function App's Managed Identity, and passes it to the binding provider. The binding code never sees the reference string, only the resolved secret. The secret lives in one place (Key Vault) and is never stored in app settings or config files.

#### DefaultAzureCredential Chain

`DefaultAzureCredential` tries credential sources in order, using the first that succeeds:

| Order | Source | When it applies |
|---|---|---|
| 1 | Environment | `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID` set |
| 2 | Workload Identity | AKS with federated credentials |
| 3 | **Managed Identity** | Running inside Azure (Functions, App Service, VM) |
| 4 | Visual Studio | Signed in to Visual Studio |
| 5 | **Azure CLI** | `az login` has been run |
| 6 | Azure PowerShell | `Connect-AzAccount` has been run |
| 7 | Interactive browser | Fallback |

In this project: locally → #5 (`az login`). In Azure → #3 (Managed Identity). No code change between environments.

#### Portal Setup

**Create Key Vault** (`kv-health-doc-dev`, Standard tier, soft-delete and purge protection enabled).

**Grant yourself the `Key Vault Secrets Officer` role** on the vault before adding secrets. This is required when RBAC is enabled on the vault — without it, the portal will return a 403 when you try to create or view secrets. Key Vault Secrets Officer allows read, write, list, and delete on secrets. Key Vault Administrator is broader (covers keys and certificates too) and more than needed for this task.

**Add secrets:**

| Secret name | Value |
|---|---|
| `CosmosDBConnectionString` | Full Cosmos connection string |
| `StorageConnectionString` | Full storage account connection string |
| `EventGridTopicKey` | Key 1 from the Event Grid topic Access keys |
| `EventGridTopicEndpoint` | Topic endpoint URL from the Event Grid topic overview |

**Enable system-assigned Managed Identity:** Function App → **Identity** → **System assigned** → On.

**Grant RBAC roles:**

| Resource | Role | Assignee | How to assign |
|---|---|---|---|
| Key Vault | `Key Vault Secrets User` | Function App identity | Portal IAM blade or CLI |
| Storage account | `Storage Blob Data Contributor` | Function App identity | Portal IAM blade or CLI |
| Cosmos DB account | `Cosmos DB Built-in Data Contributor` | Function App identity | Azure CLI only (see below) |

The Cosmos DB role is a **data plane** role, not a control plane role. It does not appear in the portal IAM blade and must be assigned via CLI.

The control plane roles available in the IAM blade (such as `Contributor` or `Cosmos DB Account Reader`) govern account management: creating databases, adjusting throughput, viewing connection strings. They grant no access to read or write documents. When `CosmosClient` authenticates with `DefaultAzureCredential` and calls `GetItemQueryIterator` or `ReadItemAsync`, the Cosmos DB service checks data plane RBAC for those requests, not control plane RBAC. An identity with `Contributor` on the account but no data plane role will still receive a 403 on every SDK call. `Cosmos DB Built-in Data Contributor` is the role that grants permission to read and write documents.

This is the same two-layer model as Azure SQL: granting a service `Contributor` on the SQL Server resource does not allow it to query tables. The service also needs a database user with the appropriate SQL-level permissions (`db_datareader`, `db_datawriter`). Control plane and data plane are independent in both services — both layers must be configured.

Get the Function App's principal ID from **Function App → Identity → System assigned → Object (principal) ID**, then run:

```bash
PRINCIPAL_ID=<object-id-of-function-app-identity>

# Cosmos DB — data plane role; CLI only
az cosmosdb sql role assignment create \
  --account-name <cosmos-account-name> \
  --resource-group <rg> \
  --role-definition-name "Cosmos DB Built-in Data Contributor" \
  --principal-id $PRINCIPAL_ID \
  --scope "/"

# Storage — data plane role; also available via portal IAM blade
STORAGE_ID=$(az storage account show --name <storage-account-name> --resource-group <rg> --query id -o tsv)
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee $PRINCIPAL_ID \
  --scope $STORAGE_ID

# Key Vault — also available via portal IAM blade
KV_ID=$(az keyvault show --name <keyvault-name> --resource-group <rg> --query id -o tsv)
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee $PRINCIPAL_ID \
  --scope $KV_ID
```

> **RBAC vs Access Policies:** Key Vault supports both models. Access policies are vault-level and older; Azure RBAC is consistent with all other Azure resources and the recommended approach. Know both for the exam.

> **System-assigned vs user-assigned identity:** System-assigned is tied to the resource and deleted with it, making it best for single-resource use. User-assigned is independent and can be shared across multiple resources, making it best for shared credentials or pre-provisioned scenarios.

**Replace App Settings with Key Vault references:** Function App → **Configuration** → replace each connection string value with:

```
@Microsoft.KeyVault(VaultName=kv-health-doc-dev;SecretName=CosmosDBConnectionString)
```

Also add the endpoint settings. These are not connection strings — they are the service URLs used by the SDK clients (`CosmosClient` and `BlobServiceClient`) in `Program.cs`, which authenticate with `DefaultAzureCredential` and connect directly to the service endpoint. The binding attributes use the Key Vault-referenced connection strings above; the SDK clients use these endpoint URLs. Both are required.

| Name | Value | Where to find it |
|---|---|---|
| `CosmosDBEndpoint` | `https://<account>.documents.azure.com:443/` | Cosmos DB account → **Overview** → URI |
| `StorageAccountEndpoint` | `https://<account>.blob.core.windows.net/` | Storage account → **Endpoints** → Blob service |

### Stored Access Policies

A **stored access policy** is a named permission set stored on a Blob container (or Queue/Table). A service SAS that references the policy by identifier can be revoked instantly by deleting the policy — without rotating the storage account key.

**AZ-204 exam rule:** Ad-hoc SAS tokens (permissions embedded inline) cannot be revoked before their expiry. The only way to invalidate them early is to rotate the storage account key, which breaks every other SAS and SDK client using that key. Stored access policies solve this — delete the policy and every SAS referencing it returns `403` immediately.

| | Ad-hoc SAS | Stored Access Policy SAS |
|---|---|---|
| **Permissions** | Embedded in the token | Defined in the policy; token holds only the policy ID |
| **Revocability** | Cannot be revoked before expiry | Revoke by deleting the policy |
| **Max policies per container** | N/A | 5 |
| **SAS type required** | Service SAS or user delegation SAS | Service SAS only (account key required to sign) |

> **User delegation SAS vs service SAS:** `FailedLabFilesEndpoint.cs` uses user delegation SAS — signed with the Function App's AAD credential (Managed Identity) via `GetUserDelegationKeyAsync`. This is the passwordless approach and is preferred when no revocability is needed. Stored access policies only work with service SAS (signed with `StorageSharedKeyCredential` — the account key). They cannot be combined with user delegation SAS.

**CLI: create a stored access policy on the failed-files container:**

```bash
az storage container policy create \
  --account-name <storage-account-name> \
  --container-name lab-results-failed \
  --name failed-read \
  --permissions r \
  --expiry 2027-01-01T00:00:00Z \
  --connection-string "<storage-connection-string>"
```

**CLI: generate a service SAS that references the policy (revocable):**

```bash
az storage blob generate-sas \
  --account-name <storage-account-name> \
  --container-name lab-results-failed \
  --name <blob-name> \
  --policy-name failed-read \
  --output tsv \
  --connection-string "<storage-connection-string>"
```

**CLI: revoke all SAS tokens that reference the policy (instant, no key rotation):**

```bash
az storage container policy delete \
  --account-name <storage-account-name> \
  --container-name lab-results-failed \
  --name failed-read \
  --connection-string "<storage-connection-string>"
```

After the policy is deleted, any SAS token containing `si=failed-read` returns `403 Forbidden` immediately, regardless of the token's `se` (expiry) parameter.

---

## Azure Service Bus

### Queues, Topics, and the Cosmos DB Trigger

The existing `DownstreamSystemNotifier` handles post-processing notifications via a Cosmos DB trigger. Service Bus is a separate exam topic covering durable messaging. It solves a different class of problem:

| Concern | Cosmos DB trigger | Service Bus |
|---|---|---|
| **Consumer model** | All trigger instances receive all changes | Queue: one consumer per message; topic: each subscription gets its own copy |
| **Filtering** | None | SQL expressions on subscriptions |
| **Delivery guarantee** | At-least-once, tied to change feed | At-least-once with configurable retry and DLQ |
| **Cross-system** | Cosmos-native | Any AMQP or HTTP client |

Both patterns are kept in the project; they are complementary, not competing.

### How It Fits Into the Pipeline

After the monitor loop confirms a batch, the orchestrator calls two publishing activities:

- **`BatchCompletePublisher`** → `lab-results-notifications` queue via `[ServiceBusOutput]`: every completed batch, consumed by `ServiceBusLabResultNotifier`
- **`AbnormalAlertPublisher`** → `lab-results-alerts` topic via `[ServiceBusOutput]`: only when `AbnormalCount > 0`

```
Queue (lab-results-notifications)
  └─ One message → one consumer
     Used for: guaranteed delivery of every batch to exactly one notifier

Topic (lab-results-alerts)
  ├─ Subscription: clinical-alerts    (no filter — receives all messages)
  └─ Subscription: critical-alerts    (SQL filter: AbnormalCount > 5)
     Used for: fan-out — each subscription gets its own independent delivery
```

**AZ-204 exam rule:** Use a queue when exactly one consumer should process each message. Use a topic when multiple independent consumers each need their own copy, optionally filtered by content.

### SQL Filters

SQL filters evaluate **message application properties** (headers set by the sender), not the message body. This is an important distinction: if `AbnormalCount` were only in the JSON body, the filter `AbnormalCount > 5` would never match because Service Bus cannot inspect the body.

`AbnormalAlertPublisher` sets the property explicitly alongside the JSON body:

```csharp
var message = new ServiceBusMessage(BinaryData.FromObjectAsJson(payload));
message.ApplicationProperties["AbnormalCount"] = summary.AbnormalCount;
```

Service Bus evaluates the filter against `ApplicationProperties` at delivery time and routes the message to `critical-alerts` only when the value exceeds 5. `clinical-alerts` uses the default `$Default` (TrueFilter) and receives every message regardless.

### Correlation Filters

A correlation filter matches on a fixed set of well-known properties (`CorrelationId`, `MessageId`, `Subject`, `To`, and application properties) using exact string equality only — no expressions or operators. Service Bus evaluates them via an optimised hash lookup rather than expression parsing, making them significantly faster at high message volumes. Microsoft recommends using correlation filters wherever possible.

A good use case is routing by clinic — set `CorrelationId` to the clinic ID on the message and create one correlation filter per subscription:

```csharp
message.CorrelationId = summary.ClinicId;
```

```
Subscription: clinic-001  →  CorrelationId = 'CLINIC_001'
Subscription: clinic-002  →  CorrelationId = 'CLINIC_002'
```

Each subscription receives only its own messages with no expression evaluation.

**Rule of thumb:** use a correlation filter for known, fixed equality matches. Use SQL when you need an expression (`>`, `<`, `LIKE`, `IN`, compound `AND`/`OR`). In this project `AbnormalCount > 5` requires SQL; if the filter were `ClinicId = 'CLINIC_001'`, a correlation filter would be the better choice.

### Peek-Lock vs Receive-and-Delete

`[ServiceBusTrigger]` uses **peek-lock** by default:

| | Peek-lock | Receive-and-delete |
|---|---|---|
| **How it works** | Message locked while processing; completed on success, released on failure | Message deleted immediately on receipt |
| **On exception** | Lock expires → message reappears → redelivered | Message gone — no retry possible |
| **After N failures** | Moved to dead-letter queue | N/A |
| **Best for** | Any processing where data loss is unacceptable | Idempotent operations where duplicate processing is worse than loss |

### Dead-Letter Queue

Messages land in the DLQ when:
- Delivery count exceeds `MaxDeliveryCount` (default: 10)
- Message TTL expires before consumption
- A consumer explicitly calls `DeadLetterMessageAsync`

`ServiceBusDeadLetterMonitor` runs every 5 minutes, peeks (not receives) the DLQ via `SubQueue.DeadLetter`, and logs any messages found, leaving them in place for human inspection.

```csharp
_serviceBusClient.CreateReceiver(
    AppConfig.ServiceBus.NotificationsQueue,
    new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
```

### Message TTL and Duplicate Detection

**Message TTL**: set `TimeToLive` on `ServiceBusMessage` or at the queue/topic level. Expired messages are dead-lettered (if configured) or silently discarded.

**Duplicate detection**: enable on the queue/topic and set a `MessageId` on each message. Service Bus discards messages with an ID seen within the detection window (default: 10 minutes).

### Portal Setup

**Create Service Bus namespace** (`sb-healthdoc-dev`, **Standard** tier; Standard is required for topics, Basic is queues only).

**Create queue** `lab-results-notifications`: Max delivery count 10, TTL 14 days, dead-lettering on expiration enabled.

**Create topic** `lab-results-alerts` with two subscriptions:

| Subscription | Filter type | Filter name | Filter expression |
|---|---|---|---|
| `clinical-alerts` | None (`$Default`) | — | Receives all messages |
| `critical-alerts` | SQL | `high-abnormal-count` | `AbnormalCount > 5` |

When creating the `critical-alerts` subscription, delete the default `$Default` filter first, then add a new SQL filter. The portal requires a name for each filter rule — use something descriptive like `high-abnormal-count`.

Copy the connection string from **Shared access policies** → `RootManageSharedAccessKey`. Add to `local.settings.json` as `ServiceBusConnectionString` and to Function App configuration (or as a Key Vault secret).

---

## Azure Queue Storage

### Queue Storage vs Service Bus

Both services deliver messages from a producer to a consumer. The right choice depends on the features you need.

| | Azure Queue Storage | Azure Service Bus |
|---|---|---|
| **Max message size** | 64 KB | 256 KB (Standard) / 100 MB (Premium) |
| **Max TTL** | 7 days | 14 days |
| **Dead-letter queue** | No (poison queue after 5 dequeues) | Yes — configurable DLQ with reason |
| **Message ordering** | Best-effort FIFO | Guaranteed with sessions |
| **Topics / subscriptions** | No | Yes |
| **Delivery semantics** | At-least-once | At-least-once (peek-lock) |
| **Best for** | Simple high-volume queuing, low cost | Enterprise messaging — DLQ, sessions, topics, transactions |

**AZ-204 exam rule:** Use Queue Storage when you need a simple durable queue built into your existing storage account, at the lowest cost and with no need for topics, DLQ, or ordering. Use Service Bus when you need any of those enterprise features.

### How It Fits Into the Pipeline

When `FileValidator` determines that a CSV is invalid, the orchestrator calls `FailureQueueNotifier` to write a notification to the `lab-results-failures` queue. `FailureQueueHandler` triggers on new messages and logs the failure for downstream alerting.

```
LabResultOrchestrator
  └─ ValidateFile returns IsValid = false
       └─ FailureQueueNotifier activity
            └─ [QueueOutput] → lab-results-failures queue
                                    │
                              FailureQueueHandler
                              [QueueTrigger] → LogWarning
```

The `[QueueOutput]` binding on an activity function returns the message as a string. The `[QueueTrigger]` function fires once per message.

```csharp
// Producer — activity function with output binding
[Function(AppConfig.Activities.NotifyFailureQueue)]
[QueueOutput(AppConfig.Queue.FailuresQueue, Connection = AppConfig.Blob.Connection)]
public string Run([ActivityTrigger] string fileName)
    => $"Validation failed: {fileName} at {DateTimeOffset.UtcNow:O}";

// Consumer — queue trigger function
[Function(nameof(FailureQueueHandler))]
public void Run(
    [QueueTrigger(AppConfig.Queue.FailuresQueue, Connection = AppConfig.Blob.Connection)]
    string message)
    => _logger.LogWarning("Failure queue: {Message}", message);
```

> **Note:** `[QueueTrigger]` uses the same `StorageConnectionString` as the blob bindings — no separate namespace or connection string is needed. Queue Storage is part of the storage account.

### Visibility Timeout

Queue Storage delivers messages using a **visibility timeout** — its equivalent of Service Bus peek-lock.

1. A consumer reads a message; the message becomes **invisible** to all other consumers for the visibility timeout period (default: 30 seconds)
2. If the consumer deletes the message before the timeout expires, it is gone permanently
3. If the consumer crashes or the timeout expires before deletion, the message **reappears** and is available for another consumer to pick up
4. After a message is dequeued 5 times without being deleted, Queue Storage automatically moves it to a `<queue-name>-poison` queue

`[QueueTrigger]` handles the delete and the poison queue automatically — no explicit `DeleteMessageAsync` or DLQ configuration needed.

**AZ-204 exam rule:** The poison queue name is always `<queue-name>-poison`. After 5 failed dequeues the message moves there; it never returns to the original queue. Service Bus has configurable `MaxDeliveryCount` and a DLQ with a failure reason; Queue Storage's approach is simpler and automatic.

### Portal Setup

No new Azure resource is needed — Queue Storage is built into the storage account you already have.

**Create the queue:**

```bash
az storage queue create \
  --name lab-results-failures \
  --account-name <storage-account-name> \
  --connection-string "<storage-connection-string>"
```

Or via portal: Storage account → **Queues** → **+ Queue** → name: `lab-results-failures`.

The `[QueueTrigger]` and `[QueueOutput]` bindings use `Connection = AppConfig.Blob.Connection`, which resolves to `StorageConnectionString` — the same setting already in `local.settings.json`. No additional configuration is required.

---

## Azure Event Grid

### Push-Based Events vs Polling and Queuing

The blob trigger already starts the pipeline when a CSV lands. Event Grid is a distinct exam topic covering push-based event delivery to multiple independent subscribers:

| | BlobTrigger | Event Grid | Service Bus |
|---|---|---|---|
| **Model** | Polling | Push | Durable queue / topic |
| **Fan-out** | All instances compete for one invocation | Every subscriber gets independent delivery | Queue = one; topic = multiple subscriptions |
| **Filtering** | None | Subject prefix/suffix, event type, advanced field filters | SQL expressions |
| **Scope** | Azure Functions | Any HTTPS endpoint or Azure service | Any AMQP or HTTP client |
| **Best for** | Simple per-file processing | Reactive fan-out with multiple independent subscribers | Guaranteed delivery, retry, ordering |

**AZ-204 exam rule:** Use Event Grid for push-based fan-out with filtering. Use Service Bus for guaranteed delivery with retry and dead-lettering.

### BlobTrigger and Event Grid Are Independent

`BlobTrigger` works out of the box with no Event Grid resource — by default it uses **polling**: the Functions runtime periodically scans the container and compares against an internal receipt store to detect new blobs. No Azure Event Grid subscription is required for the pipeline to run.

There is an opt-in mode called **Event Grid-based BlobTrigger** (available since Functions v2) where the runtime automatically subscribes to `BlobCreated` events for lower latency and better scale at high volumes. Same `[BlobTrigger]` attribute in code — the delivery mechanism is swapped out transparently via configuration. This project uses the default polling mode.

`EventGridLabResultAuditor`, by contrast, uses an explicit `[EventGridTrigger]` and will **not fire until you create an Event Grid subscription** in Azure pointing to it (see Portal Setup below). The two triggers are fully independent — one polling, one push — and both fire on the same blob upload without knowing about each other.

### How It Fits Into the Pipeline

**System event path**: an Event Grid subscription on the `lab-results-incoming` container sends `Microsoft.Storage.BlobCreated` events to `EventGridLabResultAuditor`. It writes a `LabAuditRecord` to the `AuditLog` Cosmos container. This runs independently of `LabResultIngestionTrigger`; both fire on the same upload with neither knowing about the other.

**Custom event path**: the orchestrator calls `AbnormalResultEventPublisher` immediately after `StoreSummary` when abnormal results are present. The activity publishes a `HealthDoc.Lab.AbnormalResultDetected` CloudEvent to a custom topic via `EventGridPublisherClient`.

### System Events vs Custom Events

```
System events                          Custom events
─────────────────────────────          ──────────────────────────────────
Published by Azure services            Published by your application code
(Blob Storage, Cosmos DB, etc.)        via EventGridPublisherClient

Source: the Azure resource itself      Source: a custom topic you create

Schema: predefined by the service      Schema: you define the event type,
(e.g. Microsoft.Storage.BlobCreated)   subject, and data payload

Subscription created on the resource   Subscription created on the topic
```

Both use the same delivery, retry, and dead-lettering infrastructure.

### CloudEvents vs Event Grid Schema

| | CloudEvents (recommended) | Event Grid schema |
|---|---|---|
| **Standard** | Open (CNCF) | Azure-specific |
| **Portability** | Any CloudEvents-compatible system | Azure only |
| **`type` field** | `HealthDoc.Lab.AbnormalResultDetected` | `eventType` |

This project uses CloudEvents throughout. `[EventGridTrigger]` accepts both schemas automatically.

### Subscription Filters

**Subject filters** match on the event subject string:
- `SubjectBeginsWith: /blobServices/default/containers/lab-results-incoming/` — limits the auditor to uploads only, not writes to processed or failed containers

**Advanced filters** match on event data fields:
```json
{ "operatorType": "NumberGreaterThan", "key": "data.AbnormalCount", "value": 5 }
```

### Retry Policy and Dead-Lettering

If a subscriber returns non-2xx or times out, Event Grid retries with exponential backoff, up to 24 hours and 30 attempts by default. After exhausting retries, events can be dead-lettered to a blob container for inspection. Configure `MaxDeliveryAttempts` and `EventTimeToLive` per subscription.

### Portal Setup

**Create custom Event Grid topic** — Portal → **Create a resource** → search **Event Grid Topic**:

| Field | Value | Why |
|---|---|---|
| **Name** | `evgt-healthdoc-abnormal-alerts` | Identifies the custom topic |
| **Topic type** | Custom topic | System topics are created automatically by Azure services (Storage, Cosmos DB, etc.). A custom topic is for events your own application publishes — this is what `AbnormalResultEventPublisher` writes to. |
| **Event schema** | Cloud Event Schema v1.0 | Open CNCF standard, portable across non-Azure systems. `[EventGridTrigger]` accepts both CloudEvents and Event Grid schema — CloudEvents is the recommended choice for new work. |
| **Access tier** | Leave default (Basic) | Controls ingestion throughput; Basic is sufficient for a study project. |

Once created, go to the topic → **Access keys** → copy **Key 1** as `EventGridTopicKey` in `local.settings.json`. Copy the **Topic Endpoint** URL as `EventGridTopicEndpoint`.

To verify events are being delivered, add a test subscription on the topic: **+ Event Subscription** → endpoint type **Web Hook**. For the endpoint URL, use [webhook.site](https://webhook.site): the site generates a unique HTTPS URL the moment you open it. Paste that URL as the webhook endpoint. When `AbnormalResultEventPublisher` fires, Event Grid delivers the CloudEvent as an HTTP POST to that URL and webhook.site displays the full request in real time — headers, body, and the exact JSON payload — in the browser. No account or setup required.

**Create system event subscription** — Storage account → **Events** → **+ Event Subscription**:

| Field | Value |
|---|---|
| **Name** | `sub-healthdoc-blob-created-audit` |
| **Event schema** | Cloud Event Schema v1.0 |
| **Filter to event types** | `Microsoft.Storage.BlobCreated` |
| **Endpoint type** | Azure Function → `EventGridLabResultAuditor` |
| **Subject begins with** | `/blobServices/default/containers/lab-results-incoming/` |

---

## Azure Event Hubs

### Event Hubs vs Event Grid vs Service Bus

All three services route events or messages, but they solve different problems:

| | Azure Event Grid | Azure Event Hubs | Azure Service Bus |
|---|---|---|---|
| **Model** | Push-based fan-out | High-throughput stream | Durable queue / topic |
| **Consumer model** | Independent subscribers per event | Consumer groups read the full stream | Queue = one consumer; topic = per subscription |
| **Replay** | No | Yes — retained for 1–7 days | No (once consumed, gone) |
| **Throughput** | Moderate | Millions of events/sec | Moderate |
| **Best for** | Reactive notifications, webhooks | Telemetry, logs, IoT, click streams | Reliable command delivery, ordering, DLQ |

**AZ-204 exam rule:** Use Event Hubs for high-throughput streaming where events need to be retained and replayed by multiple independent reader systems. Use Event Grid for push-based reactive fan-out. Use Service Bus for guaranteed delivery with retry and dead-lettering.

### How It Fits Into the Pipeline

After `StoreSummary` writes a batch result to Cosmos, `TelemetryPublisher` (activity) sends a telemetry event to the `lab-results-telemetry` event hub. `EventHubAnalyticsProcessor` (trigger, consumer group `pipeline-analytics`) reads from the hub independently of any other consumer.

```
StoreSummary activity
  └─ TelemetryPublisher activity
       └─ EventHubProducerClient.SendAsync
            └─ lab-results-telemetry event hub
                    │
                    ├─ $Default consumer group  (available for other readers)
                    └─ pipeline-analytics consumer group
                            └─ EventHubAnalyticsProcessor [EventHubTrigger]
                                 └─ LogInformation per event
```

```csharp
// Producer — activity function
public async Task Run([ActivityTrigger] ProcessingSummary summary)
{
    var batch = await _producer.CreateBatchAsync();
    batch.TryAdd(new EventData(JsonSerializer.SerializeToUtf8Bytes(new
    {
        summary.BatchId, summary.ClinicId, summary.AbnormalCount,
        PublishedAt = DateTimeOffset.UtcNow
    })));
    await _producer.SendAsync(batch);
}

// Consumer — event hub trigger (cardinality: many — processes a batch per invocation)
[Function(nameof(EventHubAnalyticsProcessor))]
public void Run(
    [EventHubTrigger(AppConfig.EventHub.Name,
                     Connection    = AppConfig.EventHub.Connection,
                     ConsumerGroup = AppConfig.EventHub.ConsumerGroup)]
    string[] events)
{
    foreach (var e in events)
        _logger.LogInformation("Event Hub event: {Event}", e);
}
```

### Partitions and Consumer Groups

**Partitions** divide the event stream into N parallel ordered logs. Events are distributed across partitions by a partition key (or round-robin if no key is specified). Each partition is an independent, ordered, immutable sequence of events.

**Consumer groups** are independent logical readers of the entire event hub. Each consumer group maintains its own offset (position) per partition and reads the full stream independently. `$Default` is always created automatically.

```
lab-results-telemetry event hub  (2 partitions)
├─ Partition 0  → events: e1, e3, e5 ...
└─ Partition 1  → events: e2, e4, e6 ...

$Default consumer group        → reads all partitions from its own offset
pipeline-analytics consumer group → reads all partitions from its own offset (independently)
```

**AZ-204 exam rule:** Each consumer group reads the full stream independently — adding a new consumer group does not affect other consumer groups' positions. One consumer group = one independent downstream system (e.g. one for analytics, one for archiving, one for alerting).

### Checkpointing and Event Retention

The `[EventHubTrigger]` automatically checkpoints its read position per partition in a blob storage container. If the Function host restarts, it resumes from the last checkpoint rather than the beginning.

Event data is **not deleted on consumption** — it is retained for the configured retention window (default: 1 day; up to 7 days on Standard tier, up to 90 days on Premium/Dedicated). A new consumer group added after events were written can read from the beginning using `EventPosition.Earliest`.

**AZ-204 exam rule:** This is the fundamental difference from Service Bus — Event Hubs retains all events for the retention window regardless of whether they have been consumed. Service Bus deletes a message once a consumer acknowledges it.

### Portal Setup

**Create Event Hubs namespace** — Portal → **Create a resource** → search **Event Hubs**:

| Field | Value |
|---|---|
| Namespace name | `evhns-healthdoc-dev` (globally unique) |
| Pricing tier | **Standard** (Basic supports only 1 consumer group; Standard supports multiple) |
| Throughput units | 1 |

**Create the event hub** inside the namespace → **+ Event Hub**:

| Field | Value |
|---|---|
| Name | `lab-results-telemetry` |
| Partition count | 2 |
| Message retention | 1 day |

**Create a consumer group** inside the event hub → **+ Consumer Group**:

| Field | Value |
|---|---|
| Name | `pipeline-analytics` |

Copy the connection string from **Shared access policies** → `RootManageSharedAccessKey`. Add to `local.settings.json` as `EventHubConnectionString` and to Function App configuration.

Add the constant to `AppConfig.cs`:

```csharp
public static class EventHub
{
    public const string Connection    = "EventHubConnectionString";
    public const string Name          = "lab-results-telemetry";
    public const string ConsumerGroup = "pipeline-analytics";
}
```

---

## Azure Managed Redis

> **Note:** Azure Cache for Redis is being replaced by Azure Managed Redis. New instance creation is blocked from October 1, 2026; existing instances are retired September 30, 2028. This project uses Azure Managed Redis throughout.

### Cache-Aside in the Application Layer

The APIM `cache-lookup`/`cache-store` policies are present but have no effect on the Consumption tier without an external cache linked. Redis provides real cache-aside behaviour directly in application code and is the dedicated AZ-204 caching topic.

| | APIM cache policy | Redis in application code |
|---|---|---|
| **Where it sits** | Gateway — before the Function is invoked | Inside the Function |
| **What it caches** | Full HTTP responses | Any data — JSON, strings, binary |
| **Invalidation** | TTL only | Your code calls `KeyDeleteAsync` on write |
| **Tier support** | Consumption: no-op without external cache | Works everywhere |

Both layers are in place: the APIM policy stubs remain as documentation, and Redis provides the actual caching. Linking Redis as an APIM External Cache on the Developer tier or above would activate both simultaneously.

### Cache-Aside Pattern

Cache-aside (lazy loading) is the primary caching pattern on the AZ-204 exam:

```
Read path                              Write path
──────────────────────────────         ──────────────────────────────
1. Check Redis for cache key           1. Write new records to Cosmos DB
2. Hit  → return cached data           2. Delete the cache key for clinicId
3. Miss → query Cosmos DB             3. Next read repopulates from Cosmos
4. Store result in Redis (60s TTL)
5. Return result
```

**Write-invalidate (delete) rather than write-through (update):** the activity only needs the `clinicId` to delete the key, with no need to serialise the full result set, which would duplicate work the read path already does. The cost is one extra Cosmos query on the next read after a write, acceptable for append-only lab data.

### Key Implementation Details

**`LabResultsEndpoint.cs`** — checks Redis before every Cosmos query:

```csharp
var cached = await db.StringGetAsync(cacheKey);
if (cached.HasValue)
    return deserialise and respond;   // Cosmos not touched

// cache miss — query Cosmos, store result
await db.StringSetAsync(cacheKey, JsonSerializer.Serialize(records), AppConfig.Redis.DefaultTtl);
```

**`PatientResultUpdater.cs`** — invalidates on write:

```csharp
await db.KeyDeleteAsync(AppConfig.Redis.ResultsCacheKey(clinicId));
```

**`Program.cs`** — `IConnectionMultiplexer` registered as a singleton. This is mandatory: the multiplexer manages a connection pool, and creating one per request would exhaust TCP connections immediately.

### Redis Data Types

This project uses the **string** type (any byte sequence, including JSON). Other types worth knowing:

| Type | Use case |
|---|---|
| **String** | Key-value, JSON blobs, counters (`INCR`) |
| **Hash** | Object with named fields — cache partial objects without full serialisation |
| **List** | Ordered sequences, simple queues (`LPUSH`/`RPOP`) |
| **Set** | Unique members, membership tests (`SADD`/`SISMEMBER`) |
| **Sorted set** | Ranked leaderboards, time-series by score |

### TTL and Eviction

**TTL** is set per key at write time. After expiry the key is deleted automatically (60 seconds in this project). A GET within the window is a cache hit; after expiry it's a miss and triggers a fresh Cosmos query.

**Eviction policy** kicks in when the cache reaches its memory limit:

| Policy | Behaviour |
|---|---|
| `noeviction` | Returns errors on write when full |
| `allkeys-lru` | Evicts least-recently-used across all keys |
| `volatile-lru` | Evicts LRU keys that have a TTL (leaves TTL-less keys) |
| `allkeys-lfu` | Evicts least-frequently-used |

Azure Managed Redis defaults to `volatile-lru`. Since every key in this project has a TTL, `volatile-lru` and `allkeys-lru` behave identically here.

### Portal Setup

**Create Azure Managed Redis** — Portal → **Create a resource** → search **Azure Managed Redis**:

| Field | Value |
|---|---|
| **Name** | `redis-healthdoc-dev` |
| **Region** | Same as your Function App |
| **Data tier** | **Flash** |
| **Cache size** | **F0** (cheapest, ideal for study) |

Wait for **Status: Running** on the Overview page before connecting.

**Tier comparison:**

| Tier | Best for |
|---|---|
| Flash (F0–F700) | Dev/test; RAM + NVMe storage |
| Memory Optimized (M10–M90) | Memory-intensive, low-vCPU workloads |
| Balanced (B0–B10) | General production |
| Compute Optimized (X3–X20) | High-throughput production |

**Authentication:** Microsoft Entra ID is enabled by default and access keys are disabled by default. This is the recommended posture — no shared secret to leak or rotate. To authenticate with Managed Identity, add the `Microsoft.Azure.StackExchangeRedis` NuGet package to the Functions project and assign the Function App identity the **Redis Cache Contributor** role on the instance.

For this study project, access keys are used for simplicity. To enable them: **Authentication** → **Access keys** tab → enable access key authentication. Copy **Primary access key** and the endpoint from **Overview**.

**Public network access:** Azure Managed Redis creates instances with public network access **disabled** by default. This project does not use VNet integration, so public access must be enabled for the Function App to reach the cache. In the portal: Redis instance → **Networking** → **Public access** tab → enable. Without this, every Redis call from the deployed Function App will time out with a `ConnectTimeout` error, causing the `StoreRecords` activity to fail and the orchestration to get stuck in a retry loop.

> For production, use VNet integration (supported on Flex Consumption) with a private endpoint on the Redis instance, or restrict public access to specific IPs via firewall rules on the same tab.

Azure Managed Redis uses **port 10000** and a different endpoint format from the legacy Azure Cache for Redis. Add to `local.settings.json`:

```json
"RedisConnectionString": "<name>.<region>.redis.azure.net:10000,password=<key>,ssl=True,abortConnect=False"
```

For local development with Docker:
```bash
docker run -d -p 6379:6379 redis
```
```json
"RedisConnectionString": "localhost:6379"
```

**Optional: link to APIM as External Cache** — APIM → **External cache** → **Add** → select the Redis instance. Once linked, the existing `cache-lookup`/`cache-store` policy stubs become active on Consumption tier. The two cache layers operate independently: an APIM cache hit never reaches the Function; an APIM miss that hits the application cache skips the Cosmos query.

---

## Azure Container Instances

`HealthDoc.ReportGenerator` is a .NET 10 console app that queries `ProcessingSummaries` from Cosmos DB, generates a CSV report, writes it to a `lab-results-reports` blob container, and exits. It runs as a one-shot ACI batch job: triggered on demand, runs to completion, stops.

ACI is used here because containerised batch workloads, restart policies, and scale-to-zero are AZ-204 exam topics, and a backend console app is a genuinely appropriate use of the service.

### What the Report Generator Does

```
az container create --file container.yaml
  └─ ACI pulls image from ACR
       └─ Container starts, env vars injected from secureEnvironmentVariables
            └─ Program.cs runs:
                 1. Connect to Cosmos DB (DefaultAzureCredential)
                 2. Query all ProcessingSummaries
                 3. Build CSV: BatchId, ClinicId, TotalRecords, AbnormalCount, AbnormalRate%, Status
                 4. Write to lab-results-reports/report-{timestamp}.csv in Blob Storage
                 5. Exit 0
       └─ restartPolicy: Never — container stops, billing ends
```

### Azure Container Registry

Create the registry first — you need the ACR name to tag and push the image. ACR names must be globally unique, lowercase alphanumeric only (no dashes or underscores), 5–50 characters.

```bash
az acr create \
  --name acrhealthdocdev \
  --resource-group <rg> \
  --sku Basic \
  --admin-enabled true
```

`--admin-enabled true` allows ACI to authenticate with a username and password. For production, assign the ACI managed identity the `AcrPull` RBAC role on the registry instead.

**ACR SKU comparison:**

| Tier | Storage | Webhooks | Geo-replication | Best for |
|---|---|---|---|---|
| Basic | 10 GB | No | No | Dev/test |
| Standard | 100 GB | Yes | No | Most production |
| Premium | 500 GB | Yes | Yes | Global, high-throughput |

### Multi-Stage Dockerfile

The Dockerfile lives in `HealthDoc.ReportGenerator/` but requires the repo root as its build context because it copies both `HealthDoc.Models/` and `HealthDoc.ReportGenerator/`:

```
Stage 1 (dotnet/sdk:10.0)          Stage 2 (dotnet/runtime:10.0)
  COPY HealthDoc.Models/             COPY published output
  COPY HealthDoc.ReportGenerator/    ENTRYPOINT dotnet HealthDoc.ReportGenerator.dll
  dotnet publish -c Release
```

The final image contains only the .NET runtime and the published binary, with no SDK.

### Build, Test Locally, and Push

```bash
# Build from the repo root
docker build -f HealthDoc.ReportGenerator/Dockerfile -t healthdoc-report-generator:latest .

# Tag and push to ACR
az acr login --name acrhealthdocdev
docker tag healthdoc-report-generator:latest acrhealthdocdev.azurecr.io/healthdoc-report-generator:latest
docker push acrhealthdocdev.azurecr.io/healthdoc-report-generator:latest
```
**Testing locally without Docker** — the simplest approach; runs on the host where `az login` credentials are available via `DefaultAzureCredential`. One-time role assignment required:

```bash
PRINCIPAL_ID=$(az ad signed-in-user show --query id -o tsv)

# Cosmos DB data plane role — does NOT appear in portal IAM blade (see Authentication & Security)
az cosmosdb sql role assignment create \
  --account-name <cosmos-account-name> \
  --resource-group <rg> \
  --role-definition-name "Cosmos DB Built-in Data Contributor" \
  --principal-id $PRINCIPAL_ID \
  --scope "/"

# Blob Storage data plane role — also available via portal IAM blade
STORAGE_ID=$(az storage account show --name <storage-account-name> --resource-group <rg> --query id -o tsv)
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee $PRINCIPAL_ID \
  --scope $STORAGE_ID
```

Your object ID can also be found in the portal: **Azure Active Directory → Users → your account → Object ID**.

```bash
cd HealthDoc.ReportGenerator
COSMOS_ENDPOINT=https://<account>.documents.azure.com:443/ \
STORAGE_ENDPOINT=https://<account>.blob.core.windows.net/ \
dotnet run
```

**Testing the Docker image locally** — the container is isolated from the host's `az login` session. Pass a service principal via environment variables so `EnvironmentCredential` (first in the `DefaultAzureCredential` chain) can authenticate:

```bash
docker run \
  -e COSMOS_ENDPOINT=https://<account>.documents.azure.com:443/ \
  -e STORAGE_ENDPOINT=https://<account>.blob.core.windows.net/ \
  -e AZURE_CLIENT_ID=<sp-client-id> \
  -e AZURE_CLIENT_SECRET=<sp-client-secret> \
  -e AZURE_TENANT_ID=<tenant-id> \
  healthdoc-report-generator:latest
```

### Deploy to ACI

In Azure, `DefaultAzureCredential` resolves to the container group's Managed Identity — no credential environment variables needed.

#### Step 1 — Create a user-assigned managed identity

A **user-assigned identity** persists independently of the container group. This is required here because `restartPolicy: Never` means each run is a delete-and-recreate cycle — a system-assigned identity would be destroyed with the container group, losing all role assignments.

```bash
az identity create --name id-healthdoc-report-generator --resource-group <rg>

PRINCIPAL_ID=$(az identity show \
  --name id-healthdoc-report-generator \
  --resource-group <rg> \
  --query principalId -o tsv)

IDENTITY_ID=$(az identity show \
  --name id-healthdoc-report-generator \
  --resource-group <rg> \
  --query id -o tsv)
```

#### Step 2 — Assign data plane roles

```bash
# Cosmos DB data plane role
az cosmosdb sql role assignment create \
  --account-name <cosmos-account-name> \
  --resource-group <rg> \
  --role-definition-name "Cosmos DB Built-in Data Contributor" \
  --principal-id $PRINCIPAL_ID \
  --scope "/"

# Blob Storage data plane role
STORAGE_ID=$(az storage account show --name <storage-account-name> --resource-group <rg> --query id -o tsv)
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee $PRINCIPAL_ID \
  --scope $STORAGE_ID
```

These are one-time assignments. Because the identity is user-assigned, they survive every subsequent delete-and-recreate cycle.

#### Step 3 — Configure and deploy

Copy `HealthDoc.ReportGenerator/container.yaml.example` to `container.yaml` (gitignored) and fill in your values. Set the `userAssignedIdentities` key to the full resource ID from `$IDENTITY_ID`.

**`secureEnvironmentVariables` vs `environmentVariables`:**

| | `environmentVariables` | `secureEnvironmentVariables` |
|---|---|---|
| Visible in portal | Yes | No |
| Returned by `az container show` | Yes | No |
| Use for | Non-sensitive config | Secrets, credentials, connection strings |

```bash
ACR_PASSWORD=$(az acr credential show --name acrhealthdocdev --query passwords[0].value -o tsv)
az container create --resource-group <rg> --file container.yaml
```

To re-run the report generator, delete the container group and recreate it — `restartPolicy: Never` means a terminated container group will not restart on its own:

```bash
az container delete --resource-group <rg> --name aci-healthdoc-report-generator --yes
az container create --resource-group <rg> --file container.yaml
```

### Restart Policies

```yaml
restartPolicy: Never
```

| Policy | Behaviour | Use case |
|---|---|---|
| `Always` | Restart on any exit, including clean exit (code 0) | Long-running services — web servers, APIs |
| `OnFailure` | Restart only on non-zero exit code | Batch jobs that should stop cleanly on success |
| `Never` | Never restart | One-shot tasks — run once and stop |

This project uses `Never`; the report generator runs once and exits cleanly. `Always` is the policy for a persistent web server; `OnFailure` is for jobs that should retry on error but stop on success.

### Scale to Zero

ACI bills per second of CPU and memory consumption. With `restartPolicy: Never`, the container stops as soon as `Program.cs` exits — billing ends automatically, no manual intervention needed. There are no idle charges between runs.

```bash
# Check the result of the last run
az container show --resource-group <rg> --name aci-healthdoc-report-generator \
  --query "containers[0].instanceView.currentState"

# View output logs
az container logs --resource-group <rg> --name aci-healthdoc-report-generator
```

---

## End-to-End Testing

This section walks through a complete pipeline run and explains where to validate each stage. The goal is to confirm that every service fired, data landed in the right place, and no silent failures occurred.

### Prerequisites

- Function App is running (locally or deployed to Azure)
- All App Settings / `local.settings.json` values are set: Cosmos, Storage, Service Bus, Event Grid, Redis, Key Vault
- Application Insights is connected (`APPLICATIONINSIGHTS_CONNECTION_STRING` is set)

---

### Step 1 — Upload a CSV via APIM

POST a small CSV through the APIM gateway. Use the Clinic Standard subscription key from **APIM → Subscriptions**. APIM injects the `X-Clinic-Id` header automatically from the subscription — do not pass it manually.

```bash
curl -X POST https://<apim-name>.azure-api.net/labs/upload \
  -H "Ocp-Apim-Subscription-Key: <subscription-key>" \
  -H "Content-Type: text/csv" \
  --data-binary @sample.csv
```

A minimal valid CSV (two records, one abnormal):

```csv
ClinicId,PatientId,TestCode,Result,Unit,ReferenceRange,CollectedAt
CLINIC-01,P001,HBA1C,5.1,%,4.0-5.6,2024-05-01T09:00:00
CLINIC-01,P002,GLUCOSE,210,mg/dL,70-100,2024-05-01T09:15:00
```

Row 2 will be flagged `IsAbnormal = true` (210 > 100), triggering the Service Bus alert topic and Event Grid custom event.

**Expected response:** `201 Created` with a JSON body containing `instanceId`. The filename encodes the clinic ID derived from the APIM subscription.

```json
{ "instanceId": "lab-results-CLINIC-01-20260513132410-4bed7a8a.csv" }
```

Save the `instanceId` — you will use it in the next step.

---

### Step 2 — Poll orchestration status

```bash
curl https://<apim-name>.azure-api.net/labs/status/<instanceId> \
  -H "Ocp-Apim-Subscription-Key: <subscription-key>"
```

| Response | Meaning |
|---|---|
| `202 Accepted` | Orchestration still running — poll again |
| `200 OK` with `{ "status": "Completed", "instanceId": "..." }` | Pipeline finished successfully |
| `404 Not Found` | Unknown instance ID |
| `500` with `{ "status": "Failed" \| "Terminated", "instanceId": "..." }` | Orchestration faulted — check App Insights |

---

### Step 3 — Validate in Azure Storage

Open **Storage Account → Containers**:

| Container | Expected |
|---|---|
| `lab-results-incoming` | File removed (moved after processing) |
| `lab-results-processed` | File present with original name |
| `lab-results-failed` | Empty (file was valid) |

To test the failure path, upload a CSV missing required columns. The file should appear in `lab-results-failed` and `lab-results-incoming` should be empty.

---

### Step 4 — Validate in Cosmos DB

Open **Cosmos DB → Data Explorer**:

| Container | Expected |
|---|---|
| `LabResultRecords` | One document per CSV row (`ClinicId`, `PatientId`, `TestCode`, `Result`, `Unit`, `ReferenceRange`, `IsAbnormal`) |
| `ProcessingSummaries` | One document with `TotalRecords: 2`, `AbnormalCount: 1`, `ClinicId` matching the APIM subscription ID |
| `AuditLog` | One document from `EventGridLabResultAuditor` with `ClinicId`, `FileName`, `EventType: Microsoft.Storage.BlobCreated`, and `BlobUrl` populated |

---

### Step 5 — Validate Service Bus messages

Messages are consumed immediately by the subscriber functions — do not expect them to be visible in Service Bus Explorer. Validate by checking that the handlers fired in App Insights:

```kql
traces
| where cloud_RoleName == "health-doc"
| where message has "Service Bus: batch" or message has "Clinical alert" or message has "CRITICAL"
| order by timestamp desc
| take 10
```

| Log message | Function | Condition |
|---|---|---|
| `Service Bus: batch {BatchId} — clinic {ClinicId}, {TotalRecords} records, {AbnormalCount} abnormal` | `ServiceBusLabResultNotifier` | Every successful batch |
| `Clinical alert: batch {BatchId} for clinic {ClinicId} has {AbnormalCount} abnormal result(s)` | `ClinicalAlertHandler` | Any abnormal count > 0 |
| `CRITICAL: batch {BatchId} for clinic {ClinicId} has {AbnormalCount} abnormal results — exceeds threshold of 5` | `CriticalAlertHandler` | AbnormalCount > 5 only |

To trigger `CriticalAlertHandler`, upload a CSV with more than 5 abnormal rows.

Each handler also emits a custom event to Application Insights (`customEvents` table) for structured querying:

```kql
customEvents
| where cloud_RoleName == "health-doc"
| where name in ("LabResultsBatchComplete", "ClinicalAlertReceived", "CriticalAlertReceived")
| order by timestamp desc
| take 10
```

---

### Step 6 — Validate Event Grid

If you uploaded abnormal records, the custom Event Grid topic should have fired `AbnormalResultDetected`. To verify delivery, check the metric on the topic:

**Event Grid Topic → Metrics → Published Events / Delivered Events**

Both counts should be non-zero. A gap between Published and Delivered indicates a delivery failure — check the subscription's **Dead Letter** storage if enabled.

---

### Step 7 — Validate Redis cache

After a successful pipeline run, the `results:{clinicId}` key is invalidated by `StoreRecords`. Query the results endpoint to prime the cache:

```bash
curl https://<apim-name>.azure-api.net/labs/results/CLINIC-01 \
  -H "Ocp-Apim-Subscription-Key: <subscription-key>"
```

Call it twice. The second call should return faster — the first populates Redis, the second hits the cache and skips the Cosmos query. To confirm:

**Application Insights → Logs:**

```kusto
traces
| where message has "Cache hit" or message has "Cache miss"
| order by timestamp desc
| take 10
```

The first call logs `Cache miss: results:CLINIC-01`; the second logs `Cache hit: results:CLINIC-01`.

---

### Step 8 — Validate in Application Insights

Application Insights is the primary observability tool for the pipeline. All custom business events and structured logs are captured there.

#### Live Metrics

During an upload, open **Application Insights → Live Metrics**. You will see function invocations, dependency calls (Cosmos, Service Bus, Redis), and any exceptions in real time.

#### Custom Events

```kusto
customEvents
| where timestamp > ago(10m)
| project timestamp, name, customDimensions
| order by timestamp desc
```

Expected events in order:

| Event name | Source | Key properties |
|---|---|---|
| `LabResultsProcessed` | `DownstreamSystemNotifier` (Cosmos trigger) | `ClinicId`, `RecordCount`, `AbnormalCount` |
| `LabResultsBatchComplete` | `ServiceBusLabResultNotifier` (SB queue) | `BatchId`, `ClinicId`, `TotalRecords`, `AbnormalCount` |
| `ClinicalAlertReceived` | `ClinicalAlertHandler` (SB topic) | `BatchId`, `ClinicId`, `AbnormalCount` |
| `CriticalAlertReceived` | `CriticalAlertHandler` (SB topic, > 5 only) | `BatchId`, `ClinicId`, `AbnormalCount` |
| `FileValidationFailed` | `FileValidator` (on invalid upload) | `FileName`, `Reason` |

#### Tracing the full pipeline

To see all telemetry for a single upload as a distributed trace:

**Application Insights → Transaction Search → filter by Operation ID**

Or use the end-to-end transaction view:

```kusto
requests
| where timestamp > ago(10m)
| where name == "UploadLabResultsEndpoint"
| project operation_Id, timestamp, duration, resultCode
| order by timestamp desc
| take 5
```

Take the `operation_Id` from a row, then:

```kusto
union requests, dependencies, traces, exceptions, customEvents
| where operation_Id == "<paste-id>"
| project timestamp, itemType, name, message, duration
| order by timestamp asc
```

This shows every span and log line from the HTTP upload through to the Cosmos trigger and Service Bus consumers — the full pipeline in one query.

#### Checking for exceptions

```kusto
exceptions
| where timestamp > ago(1h)
| project timestamp, problemId, outerMessage, operation_Name
| order by timestamp desc
```

An empty result means the pipeline completed without unhandled exceptions.

---

### Step 9 — Run the report generator

After the pipeline has processed at least one batch, run the report generator to confirm it can read from Cosmos and write to Blob Storage:

```bash
cd HealthDoc.ReportGenerator
dotnet run
```

Expected output:

```
Fetching summaries from Cosmos DB...
Found 1 summaries. Writing report...
Report written to: reports/report-20260512T143022Z.csv
Done.
```

Verify the CSV appeared in **Storage Account → Containers → reports**.

---

## AZ-204 Coverage Map

This project covers a significant portion of the AZ-204 exam domains. Each item links to the file where the concept is implemented.

### Compute — Azure Functions

- **Isolated worker model** — `Program.cs`, `HealthDoc.csproj` (`dotnet-isolated` runtime)
- **HTTP trigger** — `Http/UploadLabResultsEndpoint.cs`, `Http/BatchStatusEndpoint.cs`, `Http/LabResultsEndpoint.cs`, `Http/FailedLabFilesEndpoint.cs`
- **Blob trigger** — `LabResultIngestionTrigger.cs` (`BlobTrigger` on `lab-results-incoming/{name}`); note: Flex Consumption only supports EventGrid-based blob triggers — on this SKU the orchestration is started by the HTTP upload endpoint instead
- **CosmosDB trigger** — `DownstreamSystemNotifier.cs` (fires on new documents in `ProcessingSummaries`)
- **Timer trigger** — `ServiceBusDeadLetterMonitor.cs` (every 5 minutes)
- **EventGrid trigger** — `EventGridLabResultAuditor.cs` (receives `BlobCreated` system events)
- **ServiceBus trigger** — `ServiceBusLabResultNotifier.cs` (consumes `lab-results-notifications` queue)
- **Activity functions** — 11 activities, each decorated with `[Function]` + `[ActivityTrigger]`
- **Durable orchestrator** — `LabResultOrchestrator.cs` (`[OrchestrationTrigger]`, deterministic replay)
- **Function chaining** — sequential `ValidateFile → ParseFile → StoreSummary` with early exit
- **Fan-out / Fan-in** — parallel `ProcessRecord` × N, `Task.WhenAll` fan-in
- **Monitor pattern** — `context.CreateTimer()` polling loop (durable, replay-safe)
- **Async HTTP API** — `BatchStatusEndpoint.cs`, `[DurableClient]`, `202 Accepted` polling response
- **CosmosDB output binding** — `SummaryUpdater.cs`, `TimeoutSummaryWriter.cs`, `StorageConfirmationValidator.cs`, `PatientResultUpdater.cs`
- **ServiceBus output binding** — `BatchCompletePublisher.cs` (queue), `AbnormalAlertPublisher.cs` (topic)
- **CosmosDB output binding on EventGrid trigger** — `EventGridLabResultAuditor.cs` writes `LabAuditRecord`
- **Dependency injection** — all SDK clients registered as singletons in `Program.cs`
- **Centralized configuration** — `AppConfig.cs` (`const` strings for C# attribute parameters; nested classes per service)
- **Structured logging** — `ILogger<T>` throughout; cache hit/miss, DLQ findings, pipeline milestones
- **Application Insights** — sampling in `host.json`; `TelemetryClient` custom events and metrics; pipeline duration metric with dimensions

### Storage

- **Blob containers** — `lab-results-incoming`, `lab-results-processed`, `lab-results-failed`
- **Server-side blob copy** — `MoveProcessedFile.cs` (`StartCopyFromUriAsync` + delete source)
- **SAS token generation** — `FailedLabFilesEndpoint.cs` (user delegation key SAS via `GetUserDelegationKeyAsync`; 1-hour read-only)
- **Cosmos DB partition key design** — `LabResultRecords` uses `/ClinicId` (single-partition queries by clinic); `ProcessingSummaries` uses `/id`
- **Cosmos DB SDK query** — `StorageConfirmationValidator.cs` (`ReadItemAsync`, `CosmosException` not-found handling)
- **Cosmos DB output binding** — declarative writes via `[CosmosDBOutput]` attribute; no SDK call needed
- **Cosmos DB consistency levels** — account default Session; per-request override via `ItemRequestOptions { ConsistencyLevel }`; Strong not available with multi-region writes
- **Blob lifecycle management** — tier-to-cool/archive/delete rules by container prefix and age; runs once per day
- **Blob properties and metadata** — `GetPropertiesAsync()` for system properties; `SetMetadataAsync()` for user-defined key-value pairs; metadata is not indexed
- **Blob access tiers** — Hot / Cool / Cold / Archive; Archive requires rehydration (Standard ≤15 h, High ≤1 h) before read
- **Queue Storage trigger and output** — `[QueueTrigger]` / `[QueueOutput]` reusing `StorageConnectionString`; visibility timeout; automatic poison queue after 5 failed dequeues
- **Queue Storage vs Service Bus** — Queue Storage for simple low-cost queuing; Service Bus for DLQ, sessions, topics, ordering
- **Event Hubs trigger** — `[EventHubTrigger]` with `string[] events` (cardinality: many); consumer group; checkpointing to blob
- **Event Hubs producer** — `EventHubProducerClient.CreateBatchAsync` + `SendAsync`; registered as singleton
- **Partitions and consumer groups** — partitions = parallel ordered logs; each consumer group reads the full stream independently
- **Event Hubs vs Event Grid vs Service Bus** — Event Hubs for high-throughput streaming with replay; Event Grid for push fan-out; Service Bus for durable delivery with DLQ

### Security

- **Azure AD app registration** — `HealthDoc-API` exposes `LabResults.Read` scope; `HealthDoc-Dashboard` consumes it
- **MSAL — authorization code + PKCE** — `HealthDoc.Dashboard` (`@azure/msal-react`, silent acquisition, popup fallback)
- **APIM validate-jwt** — product-level policy on Internal Dashboard; `openid-config` from Azure AD OIDC endpoint
- **APIM policy execution order** — Product → API → Operation stacking; why validate-jwt lives at product level
- **Subscription keys vs JWT** — system identity vs person identity; when to use each
- **SAS tokens — user delegation SAS** — `FailedLabFilesEndpoint.cs`; signed with AAD credential via `GetUserDelegationKeyAsync`; passwordless; no account key needed
- **SAS tokens — service SAS vs user delegation SAS** — service SAS supports stored access policies; user delegation SAS does not
- **Stored access policies** — policy stored on the container; service SAS references it by ID; revoke instantly by deleting the policy without rotating the account key
- **Azure Key Vault secrets** — `CosmosDBConnectionString` and `StorageConnectionString` stored as secrets
- **Key Vault references in App Settings** — `@Microsoft.KeyVault(VaultName=...;SecretName=...)` resolved transparently by the runtime
- **Key Vault soft-delete and purge protection** — accidental deletion safeguards
- **RBAC vs access policies** — RBAC is the modern approach; access policies are vault-level and legacy
- **System-assigned Managed Identity** — enabled on Function App; tied to the resource lifecycle
- **System-assigned vs user-assigned** — Function App uses system-assigned (tied to its lifecycle); ACI report generator uses user-assigned (`id-healthdoc-report-generator`) so role assignments survive the delete/recreate cycle required by `restartPolicy: Never`
- **DefaultAzureCredential** — `az login` locally → Managed Identity in Azure; no code change between environments
- **Passwordless SDK clients** — `CosmosClient(endpoint, credential)`, `BlobServiceClient(uri, credential)`
- **RBAC role assignments** — `Key Vault Secrets User`, `Cosmos DB Built-in Data Contributor`, `Storage Blob Data Contributor`, `EventGrid Data Sender`

### Monitor & Optimize

- **Application Insights sampling** — `host.json` sampling config; `excludedTypes: Request` keeps request telemetry unsampled
- **Custom events** — `TelemetryClient.TrackEvent` in `DownstreamSystemNotifier.cs` and `ServiceBusLabResultNotifier.cs`
- **Custom metrics** — `TelemetryClient.TrackMetric` for pipeline duration with `FileName`, `BatchId`, `Status` dimensions
- **KQL queries** — `customEvents`, `customMetrics`, `traces`, `exceptions`, `dependencies` table schema; `summarize`, `extend`, `bin`, `percentile`
- **Alert rules — log vs metric alerts** — log alerts run arbitrary KQL; metric alerts use pre-aggregated platform metrics; log alerts have higher cost and latency
- **Cache-aside pattern** — `LabResultsEndpoint.cs`: Redis check → Cosmos fallback → cache store; `PatientResultUpdater.cs`: write-invalidate
- **IConnectionMultiplexer singleton** — connection pool reuse; one instance per application lifetime
- **Redis TTL** — 60s per key via `StringSetAsync(key, value, TimeSpan)`
- **Redis eviction policies** — `volatile-lru` default; `allkeys-lru`, `allkeys-lfu`, `noeviction` variants
- **Azure Managed Redis SKU tiers** — Memory Optimized (high throughput), Balanced (general purpose), Compute Optimized (CPU-intensive), Flash Optimized (large datasets, cost-sensitive)
- **APIM external cache** — links Redis to APIM so `cache-lookup`/`cache-store` policies work on Consumption tier

### API Management

- **Consumption SKU** — pay-per-call; cold starts; no built-in cache; no VNet
- **APIM SKU comparison** — Consumption / Developer / Basic / Standard / Premium tiers
- **Named values** — encrypted key-value store; referenced as `{{Name}}` in policy XML
- **Products and subscriptions** — Clinic Standard (subscription required) and Internal Dashboard (JWT, no key)
- **API-level policies** — `set-header` for key injection and clinic-id tagging; outbound header cleanup
- **Operation-level policies** — `rate-limit-by-key` (Developer+ tier), `choose`/`return-response` Content-Type guard, `cache-lookup`/`cache-store`
- **Public vs internal URL decoupling** — `/labs` public prefix maps to `/api` Functions prefix via Web service URL

### Messaging & Events

- **Service Bus queue** — `BatchCompletePublisher.cs` (`[ServiceBusOutput]`); `ServiceBusLabResultNotifier.cs` (`[ServiceBusTrigger]`, peek-lock)
- **Service Bus topic + SQL subscriptions** — `AbnormalAlertPublisher.cs` → `lab-results-alerts`; `clinical-alerts` (all) and `critical-alerts` (`AbnormalCount > 5`)
- **Dead-letter queue** — `ServiceBusDeadLetterMonitor.cs` peeks via `SubQueue.DeadLetter` option
- **Peek-lock vs receive-and-delete** — peek-lock re-delivers on failure; receive-and-delete deletes immediately
- **Message TTL and duplicate detection** — queue/topic-level config; `MessageId`-based dedup window
- **Queues vs topics** — queue = one consumer per message; topic = each subscription gets independent delivery
- **Event Grid system events** — `EventGridLabResultAuditor.cs` (`[EventGridTrigger]`); `Microsoft.Storage.BlobCreated` subscription on blob container
- **Event Grid custom events** — `AbnormalResultEventPublisher.cs` publishes via `EventGridPublisherClient`; `EventGridPublisherClient` registered as singleton
- **CloudEvents vs Event Grid schema** — CloudEvents is the open standard; `[EventGridTrigger]` accepts both
- **Subscription filters** — subject-begins-with limits auditor to `lab-results-incoming` only; advanced filters for field-level matching
- **Event Grid retry and dead-lettering** — exponential backoff up to 30 attempts / 24 hours; undelivered events to blob storage
- **Event Grid vs Service Bus vs BlobTrigger** — push fan-out vs durable queuing vs polling
- **Queue Storage trigger** — `[QueueTrigger]` on `lab-results-failures`; visibility timeout; automatic poison queue
- **Queue Storage output binding** — `[QueueOutput]` returns string message from activity function
- **Event Hubs trigger** — `[EventHubTrigger]` on `lab-results-telemetry`; `pipeline-analytics` consumer group; batch processing
- **Event Hubs producer** — `EventHubProducerClient` singleton; `CreateBatchAsync` + `SendAsync`
- **Partitions, consumer groups, checkpointing** — partitions distribute load; consumer groups read independently; checkpoint stored in blob

### Containers

- **Multi-stage Dockerfile** — `dotnet/sdk` build stage compiles and publishes; `dotnet/runtime` serve stage carries only the output — no SDK in the final image
- **Repo-root build context** — required when the Dockerfile COPYs from sibling projects; `docker build -f SubProject/Dockerfile .` from the repo root
- **Azure Container Registry** — `az acr create`, `docker push`, admin credentials vs `AcrPull` RBAC role
- **ACR SKU tiers** — Basic (dev), Standard (production), Premium (geo-replication)
- **ACI deployment via YAML** — `az container create --file container.yaml`; container group structure
- **secureEnvironmentVariables** — values hidden from portal, API responses, and `az container show`; contrast with plain `environmentVariables`
- **Restart policies** — `Always` (web servers), `OnFailure` (batch jobs), `Never` (one-shot tasks)
- **Scale to zero** — `Never` policy means ACI stops automatically on exit; billing ends without manual intervention; `az container start` re-runs the job
- **ACI vs App Service vs Static Web Apps** — ACI for containerised batch workloads; App Service for managed long-running PaaS; Static Web Apps for SPAs/static sites

---

## Application Insights — KQL Queries and Alerts

The project emits structured telemetry via `TelemetryClient`. These KQL queries run in **Application Insights → Logs**. Understanding the table schema and query structure is an AZ-204 exam topic.

### Key Tables

| Table | What it contains |
|---|---|
| `customEvents` | `TelemetryClient.TrackEvent(...)` calls — business-level events |
| `customMetrics` | `TelemetryClient.TrackMetric(...)` calls — numeric measurements |
| `requests` | HTTP trigger invocations (Functions automatically emits these) |
| `traces` | `ILogger` output (`LogInformation`, `LogWarning`, etc.) |
| `exceptions` | Unhandled exceptions and `TelemetryClient.TrackException(...)` |
| `dependencies` | Outbound calls (Cosmos DB, Storage, Service Bus — auto-instrumented) |

### Query 1 — Abnormal result count per clinic (last 24 h)

```kusto
customEvents
| where timestamp > ago(24h)
| where name == "LabResultsProcessed"
| extend clinicId       = tostring(customDimensions["ClinicId"])
| extend abnormalCount  = toint(customDimensions["AbnormalCount"])
| summarize totalAbnormal = sum(abnormalCount) by clinicId
| order by totalAbnormal desc
```

### Query 2 — Average pipeline duration by status

```kusto
customMetrics
| where name == "PipelineDurationSeconds"
| extend status   = tostring(customDimensions["Status"])
| extend batchId  = tostring(customDimensions["BatchId"])
| summarize avgDuration = avg(value), p95 = percentile(value, 95) by status
| order by avgDuration desc
```

### Query 3 — Failed blob ingestion rate (last 7 days)

```kusto
customEvents
| where timestamp > ago(7d)
| where name in ("LabResultsProcessed", "FileValidationFailed")
| summarize
    total   = countif(name == "LabResultsProcessed"),
    failed  = countif(name == "FileValidationFailed")
  by bin(timestamp, 1d)
| extend failureRate = round(todouble(failed) / (total + failed) * 100, 1)
| order by timestamp asc
```

### Query 4 — Dead-letter queue findings

```kusto
traces
| where timestamp > ago(24h)
| where message has "Dead-letter"
| project timestamp, message, severityLevel
| order by timestamp desc
```

### Alert Rule — High abnormal result rate

Create a **Log alert** that fires when abnormal results spike:

**Portal:** Application Insights → **Alerts** → **Create** → **Log search**

| Field | Value |
|---|---|
| Query | `customEvents \| where name == "LabResultsProcessed" \| extend n = toint(customDimensions["AbnormalCount"]) \| summarize total = sum(n) by bin(timestamp, 1h)` |
| Aggregation | Sum of `total` |
| Condition | Greater than `10` |
| Evaluation frequency | Every 5 minutes |
| Look-back period | 1 hour |
| Severity | `2 — Warning` |

**AZ-204 exam distinction — metric alert vs log alert:**

| | Metric alert | Log alert |
|---|---|---|
| **Data source** | Pre-aggregated platform metrics (Requests, CPU, etc.) | Arbitrary KQL against raw log data |
| **Latency** | Near-real-time (1–5 min) | Depends on ingestion + evaluation window |
| **Cost** | Lower | Higher (query runs on every evaluation) |
| **Best for** | Known numeric thresholds on standard metrics | Custom business events or complex conditions |

---

## References

- [AZ-204: Developing Solutions for Microsoft Azure — Study Guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/az-204)
- [AZ-204 Exam page](https://learn.microsoft.com/en-us/credentials/certifications/azure-developer/)
