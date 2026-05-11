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
7. [Azure API Management](#azure-api-management)
8. [Authentication & Security](#authentication--security)
9. [Azure Service Bus](#azure-service-bus)
10. [Azure Event Grid](#azure-event-grid)
11. [Azure Cache for Redis](#azure-cache-for-redis)
12. [Container Deployment](#container-deployment)
13. [AZ-204 Concepts Checklist](#az-204-concepts-checklist)

---

## Azure Services

| Service | Role in This Project | AZ-204 Topic |
|---|---|---|
| **Azure Functions** | HTTP upload endpoint, blob trigger, orchestrator, activity functions, Cosmos DB trigger, Event Grid trigger, Service Bus trigger, failed file listing | Isolated worker model, triggers, output bindings |
| **Azure Durable Functions** | Orchestrates the multi-step pipeline with durable state | Function chaining, fan-out/fan-in, monitor, async HTTP API |
| **Azure Blob Storage** | Receives uploaded CSVs; archives processed/failed files; SAS token generation | Blob triggers, storage bindings, server-side copy, SAS tokens |
| **Azure Cosmos DB** | Persists lab records, processing summaries, and audit logs | NoSQL output binding, SDK queries, CosmosDB trigger |
| **Azure API Management** | Gateway for all HTTP endpoints — two products (subscription key, JWT), rate limiting, named values, response caching | APIM policies, products, subscriptions, named values, validate-jwt |
| **Azure Active Directory** | Issues JWT tokens for internal users; two app registrations with delegated scope `LabResults.Read` | App registrations, OAuth 2.0 scopes, OIDC |
| **Azure Key Vault** | Stores connection strings as secrets; app settings reference them at runtime | Key Vault secrets, Key Vault references, soft-delete |
| **Managed Identity** | Function App authenticates to Cosmos, Storage, and Key Vault without connection strings | System-assigned identity, DefaultAzureCredential, RBAC |
| **Azure Service Bus** | Delivers batch-complete notifications (queue) and abnormal-result alerts (topic with subscriptions) | Queues, topics, subscriptions, DLQ, peek-lock |
| **Azure Event Grid** | Push-based fan-out — blob created audit events (system) and abnormal result detected events (custom) | System events, custom events, CloudEvents, subscription filters |
| **Azure Cache for Redis** | Cache-aside on lab results queries; write-invalidation on new record writes | Cache-aside pattern, TTL, eviction, IConnectionMultiplexer |
| **Application Insights** | Telemetry collection with sampling; custom business events and pipeline metrics | Monitoring, custom events, structured logging |
| **MSAL (React SPA)** | Internal dashboard authenticates via authorization code + PKCE; silent token renewal | MSAL auth flows, token acquisition, cache strategy |
| **Azure Container Registry** | Stores the report generator Docker image; pulled by ACI on demand | Registry tiers, image push/pull, admin credentials vs RBAC |
| **Azure Container Instances** | Runs the report generator as a one-shot batch job — queries Cosmos, writes CSV to Blob, exits | Restart policies, secure env vars, scale to zero, batch workloads |

---

## Architecture

### Ingestion Pipeline

When a partner clinic uploads a CSV, the file flows through APIM into an automated processing pipeline. Two independent triggers fire on the same blob write — the BlobTrigger starts the Durable orchestration; the EventGrid trigger writes an audit record. The orchestrator runs validation, parsing, parallel record processing, persistence, and downstream notification as a durable, replay-safe workflow.

```
Partner Clinic  ──── POST /labs/upload ────► Azure API Management
                     Content-Type: text/csv    (subscription key auth, rate limit,
                     Ocp-Apim-Subscription-Key  x-functions-key injected)
                                                        │
                                                        ▼
                                             UploadLabResultsEndpoint
                                             (generates unique filename,
                                              writes blob to lab-results-incoming)
                                                        │
                                               ┌────────┴────────┐
                                               │ blob write       │ blob write
                                               ▼                  ▼
                              LabResultIngestionTrigger     EventGridLabResultAuditor
                              (BlobTrigger)                 (EventGridTrigger — BlobCreated)
                                    │                       writes AuditLog → Cosmos DB
                                    │ StartOrchestration
                                    ▼
                           LabResultOrchestrator
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

After uploading, clinics poll for status using the instance ID returned at upload time, and can query processed results by clinic ID. Both endpoints sit behind the Clinic Standard APIM product — the same subscription key used for upload.

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

Internal staff sign in through the React SPA using MSAL. The acquired access token is passed to APIM, where the Internal Dashboard product validates it against Azure AD before forwarding the request to the Function App — no subscription key required.

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
│   ├── Functions/
│   │   ├── UploadLabResultsEndpoint.cs         # HTTP POST /api/upload → blob write → instanceId
│   │   ├── LabResultIngestionTrigger.cs        # BlobTrigger → schedules orchestration
│   │   ├── LabResultOrchestrator.cs            # Orchestrator — all four Durable patterns
│   │   ├── BatchStatusEndpoint.cs              # HTTP GET /api/status/{instanceId} — async HTTP API
│   │   ├── LabResultsEndpoint.cs               # HTTP GET /api/results/{clinicId} — Redis → Cosmos
│   │   ├── FailedLabFilesEndpoint.cs           # HTTP GET /api/blobs/failed → blob list + SAS URLs
│   │   ├── DownstreamSystemNotifier.cs         # CosmosDBTrigger → App Insights telemetry
│   │   ├── EventGridLabResultAuditor.cs        # EventGridTrigger (BlobCreated) → AuditLog
│   │   ├── ServiceBusLabResultNotifier.cs      # ServiceBusTrigger → App Insights event
│   │   └── ServiceBusDeadLetterMonitor.cs      # TimerTrigger → peeks DLQ every 5 minutes
│   └── Activities/
│       ├── FileValidator.cs                    # ValidateFile — checks headers and data rows
│       ├── FileParser.cs                       # ParseFile — CSV → List<LabRecord>
│       ├── LabRecordProcessor.cs               # ProcessRecord — enriches one record
│       ├── SummaryUpdater.cs                   # StoreSummary — Cosmos DB output binding
│       ├── StorageConfirmationValidator.cs     # CheckStorageConfirmation — Cosmos SDK query
│       ├── TimeoutSummaryWriter.cs             # WriteTimeoutSummary — persists timed-out status
│       ├── MoveProcessedFile.cs                # MoveFile — server-side blob copy + delete
│       ├── PatientResultUpdater.cs             # StoreRecords — Cosmos write + Redis invalidation
│       ├── BatchCompletePublisher.cs           # PublishBatchComplete — ServiceBus queue output
│       ├── AbnormalAlertPublisher.cs           # PublishAbnormalAlert — ServiceBus topic output
│       └── AbnormalResultEventPublisher.cs     # PublishAbnormalEvent — Event Grid custom event
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

1. **Upload** — A partner clinic POSTs a CSV body to `POST /labs/upload` through APIM. `UploadLabResultsEndpoint` generates a unique filename (`lab-results-{timestamp}-{shortGuid}.csv`), writes it to `lab-results-incoming`, and returns `{ "instanceId": "<filename>" }`. The client holds this ID to poll status later.

2. **Orchestration trigger** — `LabResultIngestionTrigger` (BlobTrigger) fires when the blob lands and schedules a `LabResultOrchestrator` instance, using the filename as the deterministic instance ID. If a duplicate upload arrives while an instance is still running it is skipped; if the prior instance reached a terminal state it is purged first.

3. **Audit trigger** — `EventGridLabResultAuditor` (EventGridTrigger) independently receives the `Microsoft.Storage.BlobCreated` system event for the same upload and writes a `LabAuditRecord` to the `AuditLog` Cosmos container. Neither trigger knows about the other — the audit trail is fully decoupled from the processing pipeline.

4. **Validate** — The orchestrator calls `ValidateFile`. If required columns are missing or any `Result` field is non-numeric, it calls `MoveFile` to archive the blob to `lab-results-failed` and returns a failed `ProcessingSummary`. No records are processed.

5. **Parse** — `ParseFile` splits the CSV into rows, skips the header, and maps each row to a `LabRecord` via `LabRecord.From(string[])`.

6. **Process (parallel)** — The orchestrator fans out: one `ProcessRecord` activity per `LabRecord`. Each activity calls `ProcessedRecord.From(record)`, which parses the `ReferenceRange` (e.g. `"4.0-5.6"`), determines `IsAbnormal`, and generates a Cosmos-ready document ID.

7. **Persist** — `StoreRecords` writes all `ProcessedRecord` documents to `LabResultRecords` via output binding and invalidates the Redis cache key for the affected clinic, so the next results query fetches fresh data. `StoreSummary` aggregates totals and abnormal counts into a `ProcessingSummary`, writing it to `ProcessingSummaries` with `ConfirmationStatus = Unknown`.

8. **Publish events** — If the batch contains abnormal results, `AbnormalResultEventPublisher` immediately publishes a `HealthDoc.Lab.AbnormalResultDetected` CloudEvent to the Event Grid custom topic, giving any subscriber early notification before the confirmation monitor runs.

9. **Confirm** — The monitor loop calls `CheckStorageConfirmation` up to 10 times with 30-second durable timers between attempts, querying Cosmos directly via the SDK. On success it sets `ConfirmationStatus = Confirmed`; after 10 failures it sets `TimedOut` and delegates the final Cosmos write to `WriteTimeoutSummary`.

10. **Notify** — `BatchCompletePublisher` sends a `BatchCompletedMessage` to the `lab-results-notifications` Service Bus queue — consumed by `ServiceBusLabResultNotifier`. If abnormal results exist, `AbnormalAlertPublisher` sends the same message to the `lab-results-alerts` topic, which fans it out to the `clinical-alerts` subscription (all messages) and `critical-alerts` (SQL filter: `AbnormalCount > 5`). Separately, `DownstreamSystemNotifier` fires from the Cosmos DB trigger on `ProcessingSummaries` and emits a structured event to Application Insights.

11. **Archive** — `MoveFile` copies the blob from `lab-results-incoming` to `lab-results-processed` via server-side copy (`StartCopyFromUriAsync`) and deletes the source.

### Data Models

| Model | Produced by | Consumed by | Key fields |
|---|---|---|---|
| `FilePayload` | `LabResultIngestionTrigger` | `LabResultOrchestrator` | `FileName`, `Content` |
| `FileArchiveRequest` | Orchestrator | `MoveFile` | `FileName`, `TargetContainer`, `Reason` |
| `ValidationResult` | `ValidateFile` | Orchestrator (gate) | `IsValid`, `Errors` |
| `LabRecord` | `ParseFile` (via `From`) | `ProcessRecord` | `Result`, `ReferenceRange`, `CollectedAt` |
| `ProcessedRecord` | `ProcessRecord` (via `From`) | `StoreRecords`, `StoreSummary` | `IsAbnormal`, `Id`, `ProcessedAt` |
| `ProcessingSummary` | `StoreSummary` | Monitor loop, Service Bus, App Insights | `BatchId`, `AbnormalCount`, `ConfirmationStatus` |
| `BatchCompletedMessage` | `BatchCompletePublisher` | `ServiceBusLabResultNotifier` | `BatchId`, `ClinicId`, `AbnormalCount` |
| `AbnormalResultEvent` | `AbnormalResultEventPublisher` | Event Grid subscribers | `BatchId`, `ClinicId`, `AbnormalCount` |
| `LabAuditRecord` | `EventGridLabResultAuditor` | `AuditLog` Cosmos container | `FileName`, `EventType`, `ReceivedAt` |

`ProcessedRecord` inherits from `LabRecord`. Both expose a static `From(...)` factory so the mapping logic can be unit tested without any Azure dependency.

---

## Durable Functions Patterns

All four patterns are implemented in this project. Patterns 1–3 are inside `LabResultOrchestrator.cs`; Pattern 4 is `BatchStatusEndpoint.cs`.

### Pattern 1 — Function Chaining

Activities execute sequentially — the output of each step is the input to the next.

```
ValidateFile(payload)
    └─► ParseFile(payload)          [only if valid]
            └─► StoreSummary(records)
```

The orchestrator short-circuits if `ValidateFile` returns `IsValid = false`: it calls `MoveFile` to archive the blob and returns a failed summary without touching Cosmos DB. This is the **early exit** variant of function chaining.

**Exam concept:** Guaranteed ordering, sequential dependency, deterministic replay.

### Pattern 2 — Fan-out / Fan-in

Each `LabRecord` is dispatched to a `ProcessRecord` activity independently. All N activities run in parallel. `Task.WhenAll` is the fan-in point — the orchestrator blocks here until every record is processed.

```csharp
var tasks = records.Select(r =>
    context.CallActivityAsync<ProcessedRecord>(AppConfig.Activities.ProcessRecord, r));

var results = await Task.WhenAll(tasks);  // fan-in
```

**Exam concept:** Parallel activity dispatch, `Task.WhenAll`, fan-out/fan-in topology.

### Pattern 3 — Monitor

After `StoreSummary` writes to Cosmos, the orchestrator polls until the document is confirmed persisted. It uses durable timers — not `Thread.Sleep` — so the orchestrator survives a host restart mid-poll.

```
for attempt in 0..9:
    await context.CreateTimer(30 seconds)     ← durable, replay-safe
    result = await CheckStorageConfirmation(batchId)
    if result.ConfirmationStatus == Confirmed → break

if not Confirmed → set TimedOut, call WriteTimeoutSummary activity
```

On timeout, the final Cosmos write is delegated to the `WriteTimeoutSummary` activity — not performed by the orchestrator directly — keeping it deterministic and replay-safe.

**Exam concept:** Monitor pattern, durable timers, external resource polling, status tracking.

### Pattern 4 — Async HTTP API

`BatchStatusEndpoint` lets any caller check orchestration status by instance ID. The instance ID is the blob filename returned by the upload endpoint — the client holds it and polls until a terminal status arrives.

| Runtime status | HTTP response | Meaning |
|---|---|---|
| `Completed` | `200 OK` + serialized output | Pipeline finished |
| `Failed` | `500` + serialized output | Orchestrator threw an exception |
| `Terminated` | `500 Terminated` | Instance was forcibly stopped |
| Any other | `202 Accepted` | Still running — poll again |

The `202 Accepted` response is the key exam detail: it signals the client to keep polling the same URL.

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

    "KeyVaultEndpoint": "https://kv-healthdoc-dev.vault.azure.net/",

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

**Why Key Vault doesn't eliminate local connection strings:** `@Microsoft.KeyVault(...)` references are resolved by the Azure Functions host reading live App Settings from the Azure portal. `local.settings.json` is a flat file read directly by the local host — there is no Key Vault resolution. So locally, the actual connection string values are always required for binding attributes.

In Azure, the connection string app settings are replaced with Key Vault references and the runtime resolves them transparently. The SDK clients don't use connection strings in either environment — they use `DefaultAzureCredential` throughout.

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
  --data-binary @lab_results_2024_05_01.csv
```

Response `201 Created`:

```json
{ "instanceId": "lab-results-20260508213642-8582e72b.csv" }
```

Poll status using the returned `instanceId`:

```bash
curl http://localhost:7220/api/status/lab-results-20260508213642-8582e72b.csv \
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

## Azure API Management

APIM sits in front of all HTTP endpoints. External partner clinics use one product; internal staff use another. The Function App URL is never exposed to either — APIM is the only entry point.

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

The `/labs` prefix is a domain concept visible to clients. The `/api` prefix is an Azure Functions implementation detail. Setting the **Web service URL** to `https://<func-app>.azurewebsites.net/api` absorbs the Functions prefix once — operation URL overrides stay clean (`/upload`, `/status/{id}`, `/results/{clinicId}`).

### Portal Setup

#### Step 1 — Create the APIM Instance

Search **API Management** → **Create**.

| Field | Value |
|---|---|
| Resource name | `apim-healthdoc-dev` (globally unique) |
| Region | same as your Function App |
| Pricing tier | **Consumption** |
| Organization name | HealthDoc |

> Consumption tier is pay-per-call with no hourly charge — ideal for study. Provisioning takes ~5 minutes.

**AZ-204 SKU comparison:**

| Tier | Cold starts | VNet support | Scale | Best for |
|---|---|---|---|---|
| Consumption | Yes (~2 s) | No | Auto | Dev, testing |
| Developer | No | Yes | Manual | Non-production exploration |
| Basic | No | No | Manual | Low-traffic production |
| Standard | No | No | Manual | Medium production |
| Premium | No | Yes | Manual + zone redundancy | Enterprise production |

#### Step 2 — Create the Named Value for the Function Key

Named values are APIM's encrypted key-value store. Policies reference them as `{{Name}}` — the value is never visible to callers.

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
        <set-header name="x-clinic-id" exists-action="override">
            <value>@(context.Subscription.Id)</value>
        </set-header>
    </inbound>
    <outbound>
        <base />
        <set-header name="x-functions-key" exists-action="delete" />
        <set-header name="x-powered-by" exists-action="delete" />
    </outbound>
</policies>
```

**Operation-level — Upload** (rate limit + Content-Type guard):

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

**Operation-level — Get Lab Results** (response caching):

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

Apply the `validate-jwt` policy **at the product level** — not the API level:

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
- **Failed Files** — lists CSVs that failed validation with a one-hour SAS download link each
- **Lab Results** — enter a Clinic ID to query processed records

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

#### What Changed in Code

`Program.cs` registers both SDK clients using `DefaultAzureCredential` and a service endpoint URI instead of a connection string:

```csharp
var credential = new DefaultAzureCredential();

// No connection string — authenticates via Managed Identity (Azure) or az login (local)
new CosmosClient(endpoint, credential);
new BlobServiceClient(new Uri(endpoint), credential);
```

Binding attributes (`[CosmosDBOutput]`, `[BlobTrigger]`, etc.) still reference named connection string settings because the Functions runtime resolves these — not the SDK. In Azure, those app settings are Key Vault references that the runtime resolves transparently before passing to the binding provider.

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

**Create Key Vault** (`kv-healthdoc-dev`, Standard tier, soft-delete and purge protection enabled).

**Add secrets:**

| Secret name | Value |
|---|---|
| `CosmosDBConnectionString` | Full Cosmos connection string |
| `StorageConnectionString` | Full storage account connection string |

**Enable system-assigned Managed Identity:** Function App → **Identity** → **System assigned** → On.

**Grant RBAC roles:**

| Resource | Role | Assignee |
|---|---|---|
| Key Vault | `Key Vault Secrets User` | Function App identity |
| Cosmos DB account | `Cosmos DB Built-in Data Contributor` | Function App identity |
| Storage account | `Storage Blob Data Contributor` | Function App identity |

> **RBAC vs Access Policies:** Key Vault supports both models. Access policies are vault-level and older; Azure RBAC is consistent with all other Azure resources and the recommended approach. Know both for the exam.

> **System-assigned vs user-assigned identity:** System-assigned is tied to the resource and deleted with it — best for single-resource use. User-assigned is independent and can be shared across multiple resources — best for shared credentials or pre-provisioned scenarios.

**Replace App Settings with Key Vault references:** Function App → **Configuration** → replace each connection string value with:

```
@Microsoft.KeyVault(VaultName=kv-healthdoc-dev;SecretName=CosmosDBConnectionString)
```

Also add the endpoint settings:

| Name | Value |
|---|---|
| `CosmosDBEndpoint` | `https://<account>.documents.azure.com:443/` |
| `StorageAccountEndpoint` | `https://<account>.blob.core.windows.net/` |

---

## Azure Service Bus

### Queues, Topics, and the Cosmos DB Trigger

The existing `DownstreamSystemNotifier` handles post-processing notifications via a Cosmos DB trigger. Service Bus is a separate exam topic covering durable messaging — it solves a different class of problem:

| Concern | Cosmos DB trigger | Service Bus |
|---|---|---|
| **Consumer model** | All trigger instances receive all changes | Queue: one consumer per message; topic: each subscription gets its own copy |
| **Filtering** | None | SQL expressions on subscriptions |
| **Delivery guarantee** | At-least-once, tied to change feed | At-least-once with configurable retry and DLQ |
| **Cross-system** | Cosmos-native | Any AMQP or HTTP client |

Both patterns are kept in the project — they are complementary, not competing.

### How It Fits Into the Pipeline

After the monitor loop confirms a batch, the orchestrator calls two publishing activities:

- **`BatchCompletePublisher`** → `lab-results-notifications` queue via `[ServiceBusOutput]` — every completed batch, consumed by `ServiceBusLabResultNotifier`
- **`AbnormalAlertPublisher`** → `lab-results-alerts` topic via `[ServiceBusOutput]` — only when `AbnormalCount > 0`

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

`ServiceBusDeadLetterMonitor` runs every 5 minutes, peeks (not receives) the DLQ via `SubQueue.DeadLetter`, and logs any messages found — leaving them in place for human inspection.

```csharp
_serviceBusClient.CreateReceiver(
    AppConfig.ServiceBus.NotificationsQueue,
    new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
```

### Message TTL and Duplicate Detection

**Message TTL** — set `TimeToLive` on `ServiceBusMessage` or at the queue/topic level. Expired messages are dead-lettered (if configured) or silently discarded.

**Duplicate detection** — enable on the queue/topic, set a `MessageId` on each message. Service Bus discards messages with an ID seen within the detection window (default: 10 minutes).

### Portal Setup

**Create Service Bus namespace** (`sb-healthdoc-dev`, **Standard** tier — Standard required for topics; Basic is queues only).

**Create queue** `lab-results-notifications`: Max delivery count 10, TTL 14 days, dead-lettering on expiration enabled.

**Create topic** `lab-results-alerts` with two subscriptions:

| Subscription | Filter |
|---|---|
| `clinical-alerts` | None — receives all messages |
| `critical-alerts` | SQL: `AbnormalCount > 5` |

Copy the connection string from **Shared access policies** → `RootManageSharedAccessKey`. Add to `local.settings.json` as `ServiceBusConnectionString` and to Function App configuration (or as a Key Vault secret).

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

**System event path** — an Event Grid subscription on the `lab-results-incoming` container sends `Microsoft.Storage.BlobCreated` events to `EventGridLabResultAuditor`. It writes a `LabAuditRecord` to the `AuditLog` Cosmos container. This runs independently of `LabResultIngestionTrigger` — both fire on the same upload with neither knowing about the other.

**Custom event path** — the orchestrator calls `AbnormalResultEventPublisher` immediately after `StoreSummary` when abnormal results are present. The activity publishes a `HealthDoc.Lab.AbnormalResultDetected` CloudEvent to a custom topic via `EventGridPublisherClient`.

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

If a subscriber returns non-2xx or times out, Event Grid retries with exponential backoff — up to 24 hours and 30 attempts by default. After exhausting retries, events can be dead-lettered to a blob container for inspection. Configure `MaxDeliveryAttempts` and `EventTimeToLive` per subscription.

### Portal Setup

**Create custom Event Grid topic** — Portal → **Create a resource** → search **Event Grid Topic**:

| Field | Value | Why |
|---|---|---|
| **Name** | `evgt-healthdoc-abnormal-alerts` | Identifies the custom topic |
| **Topic type** | Custom topic | System topics are created automatically by Azure services (Storage, Cosmos DB, etc.). A custom topic is for events your own application publishes — this is what `AbnormalResultEventPublisher` writes to. |
| **Event schema** | Cloud Event Schema v1.0 | Open CNCF standard, portable across non-Azure systems. `[EventGridTrigger]` accepts both CloudEvents and Event Grid schema — CloudEvents is the recommended choice for new work. |
| **Access tier** | Leave default (Basic) | Controls ingestion throughput; Basic is sufficient for a study project. |

Once created, go to the topic → **Access keys** → copy **Key 1** as `EventGridTopicKey` in `local.settings.json`. Copy the **Topic Endpoint** URL as `EventGridTopicEndpoint`.

**Create system event subscription** — Storage account → **Events** → **+ Event Subscription**:

| Field | Value |
|---|---|
| Filter to event types | `Microsoft.Storage.BlobCreated` |
| Endpoint type | Azure Function → `EventGridLabResultAuditor` |
| Subject begins with | `/blobServices/default/containers/lab-results-incoming/` |

**Create custom event subscription** — Custom topic → **+ Event Subscription**, endpoint: Web Hook. Use [webhook.site](https://webhook.site) in a study environment to inspect delivered events.

**Add `AuditLog` Cosmos container** — partition key `/ClinicId`.

---

## Azure Cache for Redis

### Cache-Aside in the Application Layer

The APIM `cache-lookup`/`cache-store` policies are present but have no effect on the Consumption tier without an external cache linked. Redis provides real cache-aside behaviour directly in application code — and is the dedicated AZ-204 caching topic.

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

**Write-invalidate (delete) rather than write-through (update):** the activity only needs the `clinicId` to delete the key — no need to serialise the full result set, which would duplicate work the read path already does. The cost is one extra Cosmos query on the next read after a write, acceptable for append-only lab data.

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

**TTL** is set per key at write time. After expiry the key is deleted automatically — 60 seconds in this project. A GET within the window is a cache hit; after expiry it's a miss and triggers a fresh Cosmos query.

**Eviction policy** kicks in when the cache reaches its memory limit:

| Policy | Behaviour |
|---|---|
| `noeviction` | Returns errors on write when full |
| `allkeys-lru` | Evicts least-recently-used across all keys |
| `volatile-lru` | Evicts LRU keys that have a TTL (leaves TTL-less keys) |
| `allkeys-lfu` | Evicts least-frequently-used |

Azure Cache for Redis defaults to `volatile-lru`. Since every key in this project has a TTL, `volatile-lru` and `allkeys-lru` behave identically here.

### Portal Setup

**Create Azure Cache for Redis** (`redis-healthdoc-dev`, **Basic C0** — 250 MB, cheapest, no SLA, ideal for study).

**SKU comparison:**

| Tier | Replication | Clustering | Persistence | Best for |
|---|---|---|---|---|
| Basic | No | No | No | Dev/test |
| Standard | Yes | No | No | Production |
| Premium | Yes | Yes (up to 10 shards) | RDB + AOF | High-throughput production |
| Enterprise | Yes | Yes | Yes + active geo-replication | Global, mission-critical |

Copy the **Primary connection string** from **Access keys**. Add to `local.settings.json`:

```json
"RedisConnectionString": "<name>.redis.cache.windows.net:6380,password=<key>,ssl=True,abortConnect=False"
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

## Container Deployment

`HealthDoc.ReportGenerator` is a .NET 10 console app that queries `ProcessingSummaries` from Cosmos DB, generates a CSV report, writes it to a `lab-results-reports` blob container, and exits. It runs as a one-shot ACI batch job — triggered on demand, runs to completion, stops.

A static SPA (the dashboard) would be a poor fit for ACI — the right deployment target for that is Azure Static Web Apps. ACI is used here because containerised batch workloads, restart policies, and scale-to-zero are AZ-204 exam topics, and a backend console app is a genuinely appropriate use of the service.

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

### Multi-Stage Dockerfile

The Dockerfile lives in `HealthDoc.ReportGenerator/` but requires the repo root as its build context because it copies both `HealthDoc.Models/` and `HealthDoc.ReportGenerator/`:

```
Stage 1 (dotnet/sdk:10.0)          Stage 2 (dotnet/runtime:10.0)
  COPY HealthDoc.Models/             COPY published output
  COPY HealthDoc.ReportGenerator/    ENTRYPOINT dotnet HealthDoc.ReportGenerator.dll
  dotnet publish -c Release
```

The final image contains only the .NET runtime and the published binary — no SDK. Build from the repo root:

```bash
docker build -f HealthDoc.ReportGenerator/Dockerfile -t healthdoc-report-generator:latest .
```

### Build and Test Locally

```bash
# From repo root
docker build -f HealthDoc.ReportGenerator/Dockerfile -t healthdoc-report-generator:latest .

docker run \
  -e COSMOS_ENDPOINT=https://<account>.documents.azure.com:443/ \
  -e STORAGE_ENDPOINT=https://<account>.blob.core.windows.net/ \
  healthdoc-report-generator:latest
```

The container connects to real Azure resources using the credentials found by `DefaultAzureCredential`. For local runs, ensure `az login` has been run so the CLI credential is available inside the container — or pass `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, and `AZURE_TENANT_ID` as env vars to use a service principal.

### Azure Container Registry

ACR stores the image before ACI pulls it.

```bash
az acr create \
  --name acrHealthDocDev \
  --resource-group <rg> \
  --sku Basic \
  --admin-enabled true

az acr login --name acrHealthDocDev
docker tag healthdoc-report-generator:latest acrHealthDocDev.azurecr.io/healthdoc-report-generator:latest
docker push acrHealthDocDev.azurecr.io/healthdoc-report-generator:latest
```

`--admin-enabled true` allows ACI to authenticate with a username and password. For production, assign the ACI managed identity the `AcrPull` RBAC role on the registry instead.

**ACR SKU comparison:**

| Tier | Storage | Webhooks | Geo-replication | Best for |
|---|---|---|---|---|
| Basic | 10 GB | No | No | Dev/test |
| Standard | 100 GB | Yes | No | Most production |
| Premium | 500 GB | Yes | Yes | Global, high-throughput |

### Deploy to Azure Container Instances

Copy `HealthDoc.ReportGenerator/container.yaml.example` to `container.yaml` (gitignored), fill in your values, and deploy:

```bash
ACR_PASSWORD=$(az acr credential show --name acrHealthDocDev --query passwords[0].value -o tsv)
az container create --resource-group <rg> --file container.yaml
```

The container runs, generates the report, and stops. Each invocation of `az container create` is a new run.

**`secureEnvironmentVariables` vs `environmentVariables`:**

| | `environmentVariables` | `secureEnvironmentVariables` |
|---|---|---|
| Visible in portal | Yes | No |
| Returned by `az container show` | Yes | No |
| Use for | Non-sensitive config | Secrets, credentials, connection strings |

### Restart Policies

```yaml
restartPolicy: Never
```

| Policy | Behaviour | Use case |
|---|---|---|
| `Always` | Restart on any exit, including clean exit (code 0) | Long-running services — web servers, APIs |
| `OnFailure` | Restart only on non-zero exit code | Batch jobs that should stop cleanly on success |
| `Never` | Never restart | One-shot tasks — run once and stop |

This project uses `Never` — the report generator runs once and exits cleanly. `Always` is the policy for a persistent web server; `OnFailure` is for jobs that should retry on error but stop on success.

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

## AZ-204 Concepts Checklist

### Compute — Azure Functions

- [ ] **Isolated worker model** — `Program.cs`, `HealthDoc.csproj` (`dotnet-isolated` runtime)
- [ ] **HTTP trigger** — `UploadLabResultsEndpoint.cs`, `BatchStatusEndpoint.cs`, `LabResultsEndpoint.cs`, `FailedLabFilesEndpoint.cs`
- [ ] **Blob trigger** — `LabResultIngestionTrigger.cs` (`BlobTrigger` on `lab-results-incoming/{name}`)
- [ ] **CosmosDB trigger** — `DownstreamSystemNotifier.cs` (fires on new documents in `ProcessingSummaries`)
- [ ] **Timer trigger** — `ServiceBusDeadLetterMonitor.cs` (every 5 minutes)
- [ ] **EventGrid trigger** — `EventGridLabResultAuditor.cs` (receives `BlobCreated` system events)
- [ ] **ServiceBus trigger** — `ServiceBusLabResultNotifier.cs` (consumes `lab-results-notifications` queue)
- [ ] **Activity functions** — 11 activities, each decorated with `[Function]` + `[ActivityTrigger]`
- [ ] **Durable orchestrator** — `LabResultOrchestrator.cs` (`[OrchestrationTrigger]`, deterministic replay)
- [ ] **Function chaining** — sequential `ValidateFile → ParseFile → StoreSummary` with early exit
- [ ] **Fan-out / Fan-in** — parallel `ProcessRecord` × N, `Task.WhenAll` fan-in
- [ ] **Monitor pattern** — `context.CreateTimer()` polling loop (durable, replay-safe)
- [ ] **Async HTTP API** — `BatchStatusEndpoint.cs`, `[DurableClient]`, `202 Accepted` polling response
- [ ] **CosmosDB output binding** — `SummaryUpdater.cs`, `TimeoutSummaryWriter.cs`, `StorageConfirmationValidator.cs`, `PatientResultUpdater.cs`
- [ ] **ServiceBus output binding** — `BatchCompletePublisher.cs` (queue), `AbnormalAlertPublisher.cs` (topic)
- [ ] **CosmosDB output binding on EventGrid trigger** — `EventGridLabResultAuditor.cs` writes `LabAuditRecord`
- [ ] **Dependency injection** — all SDK clients registered as singletons in `Program.cs`
- [ ] **Centralized configuration** — `AppConfig.cs` (`const` strings for C# attribute parameters; nested classes per service)
- [ ] **Structured logging** — `ILogger<T>` throughout; cache hit/miss, DLQ findings, pipeline milestones
- [ ] **Application Insights** — sampling in `host.json`; `TelemetryClient` custom events and metrics; pipeline duration metric with dimensions

### Storage

- [ ] **Blob containers** — `lab-results-incoming`, `lab-results-processed`, `lab-results-failed`
- [ ] **Server-side blob copy** — `MoveProcessedFile.cs` (`StartCopyFromUriAsync` + delete source)
- [ ] **SAS token generation** — `FailedLabFilesEndpoint.cs` (`BlobClient.GenerateSasUri`, 1-hour read-only)
- [ ] **Cosmos DB partition key design** — `LabResultRecords` uses `/ClinicId` (single-partition queries by clinic); `ProcessingSummaries` uses `/id`
- [ ] **Cosmos DB SDK query** — `StorageConfirmationValidator.cs` (`ReadItemAsync`, `CosmosException` not-found handling)
- [ ] **Cosmos DB output binding** — declarative writes via `[CosmosDBOutput]` attribute; no SDK call needed

### Security

- [ ] **Azure AD app registration** — `HealthDoc-API` exposes `LabResults.Read` scope; `HealthDoc-Dashboard` consumes it
- [ ] **MSAL — authorization code + PKCE** — `HealthDoc.Dashboard` (`@azure/msal-react`, silent acquisition, popup fallback)
- [ ] **APIM validate-jwt** — product-level policy on Internal Dashboard; `openid-config` from Azure AD OIDC endpoint
- [ ] **APIM policy execution order** — Product → API → Operation stacking; why validate-jwt lives at product level
- [ ] **Subscription keys vs JWT** — system identity vs person identity; when to use each
- [ ] **SAS tokens** — time-limited, scope-limited blob access without sharing account keys
- [ ] **Azure Key Vault secrets** — `CosmosDBConnectionString` and `StorageConnectionString` stored as secrets
- [ ] **Key Vault references in App Settings** — `@Microsoft.KeyVault(VaultName=...;SecretName=...)` resolved transparently by the runtime
- [ ] **Key Vault soft-delete and purge protection** — accidental deletion safeguards
- [ ] **RBAC vs access policies** — RBAC is the modern approach; access policies are vault-level and legacy
- [ ] **System-assigned Managed Identity** — enabled on Function App; tied to the resource lifecycle
- [ ] **System-assigned vs user-assigned** — system-assigned per-resource; user-assigned independent and shareable
- [ ] **DefaultAzureCredential** — `az login` locally → Managed Identity in Azure; no code change between environments
- [ ] **Passwordless SDK clients** — `CosmosClient(endpoint, credential)`, `BlobServiceClient(uri, credential)`
- [ ] **RBAC role assignments** — `Key Vault Secrets User`, `Cosmos DB Built-in Data Contributor`, `Storage Blob Data Contributor`, `EventGrid Data Sender`

### Monitor & Optimize

- [ ] **Application Insights sampling** — `host.json` sampling config; `excludedTypes: Request` keeps request telemetry unsampled
- [ ] **Custom events** — `TelemetryClient.TrackEvent` in `DownstreamSystemNotifier.cs` and `ServiceBusLabResultNotifier.cs`
- [ ] **Custom metrics** — `TelemetryClient.TrackMetric` for pipeline duration with `FileName`, `BatchId`, `Status` dimensions
- [ ] **Cache-aside pattern** — `LabResultsEndpoint.cs`: Redis check → Cosmos fallback → cache store; `PatientResultUpdater.cs`: write-invalidate
- [ ] **IConnectionMultiplexer singleton** — connection pool reuse; one instance per application lifetime
- [ ] **Redis TTL** — 60s per key via `StringSetAsync(key, value, TimeSpan)`
- [ ] **Redis eviction policies** — `volatile-lru` default; `allkeys-lru`, `allkeys-lfu`, `noeviction` variants
- [ ] **Redis SKU tiers** — Basic (dev), Standard (replication), Premium (clustering + persistence), Enterprise (geo-replication)
- [ ] **APIM external cache** — links Redis to APIM so `cache-lookup`/`cache-store` policies work on Consumption tier

### API Management

- [ ] **Consumption SKU** — pay-per-call; cold starts; no built-in cache; no VNet
- [ ] **APIM SKU comparison** — Consumption / Developer / Basic / Standard / Premium tiers
- [ ] **Named values** — encrypted key-value store; referenced as `{{Name}}` in policy XML
- [ ] **Products and subscriptions** — Clinic Standard (subscription required) and Internal Dashboard (JWT, no key)
- [ ] **API-level policies** — `set-header` for key injection and clinic-id tagging; outbound header cleanup
- [ ] **Operation-level policies** — `rate-limit-by-key` (Developer+ tier), `choose`/`return-response` Content-Type guard, `cache-lookup`/`cache-store`
- [ ] **Public vs internal URL decoupling** — `/labs` public prefix maps to `/api` Functions prefix via Web service URL

### Messaging & Events

- [ ] **Service Bus queue** — `BatchCompletePublisher.cs` (`[ServiceBusOutput]`); `ServiceBusLabResultNotifier.cs` (`[ServiceBusTrigger]`, peek-lock)
- [ ] **Service Bus topic + SQL subscriptions** — `AbnormalAlertPublisher.cs` → `lab-results-alerts`; `clinical-alerts` (all) and `critical-alerts` (`AbnormalCount > 5`)
- [ ] **Dead-letter queue** — `ServiceBusDeadLetterMonitor.cs` peeks via `SubQueue.DeadLetter` option
- [ ] **Peek-lock vs receive-and-delete** — peek-lock re-delivers on failure; receive-and-delete deletes immediately
- [ ] **Message TTL and duplicate detection** — queue/topic-level config; `MessageId`-based dedup window
- [ ] **Queues vs topics** — queue = one consumer per message; topic = each subscription gets independent delivery
- [ ] **Event Grid system events** — `EventGridLabResultAuditor.cs` (`[EventGridTrigger]`); `Microsoft.Storage.BlobCreated` subscription on blob container
- [ ] **Event Grid custom events** — `AbnormalResultEventPublisher.cs` publishes via `EventGridPublisherClient`; `EventGridPublisherClient` registered as singleton
- [ ] **CloudEvents vs Event Grid schema** — CloudEvents is the open standard; `[EventGridTrigger]` accepts both
- [ ] **Subscription filters** — subject-begins-with limits auditor to `lab-results-incoming` only; advanced filters for field-level matching
- [ ] **Event Grid retry and dead-lettering** — exponential backoff up to 30 attempts / 24 hours; undelivered events to blob storage
- [ ] **Event Grid vs Service Bus vs BlobTrigger** — push fan-out vs durable queuing vs polling

### Containers

- [ ] **Multi-stage Dockerfile** — `dotnet/sdk` build stage compiles and publishes; `dotnet/runtime` serve stage carries only the output — no SDK in the final image
- [ ] **Repo-root build context** — required when the Dockerfile COPYs from sibling projects; `docker build -f SubProject/Dockerfile .` from the repo root
- [ ] **Azure Container Registry** — `az acr create`, `docker push`, admin credentials vs `AcrPull` RBAC role
- [ ] **ACR SKU tiers** — Basic (dev), Standard (production), Premium (geo-replication)
- [ ] **ACI deployment via YAML** — `az container create --file container.yaml`; container group structure
- [ ] **secureEnvironmentVariables** — values hidden from portal, API responses, and `az container show`; contrast with plain `environmentVariables`
- [ ] **Restart policies** — `Always` (web servers), `OnFailure` (batch jobs), `Never` (one-shot tasks)
- [ ] **Scale to zero** — `Never` policy means ACI stops automatically on exit; billing ends without manual intervention; `az container start` re-runs the job
- [ ] **ACI vs App Service vs Static Web Apps** — ACI for containerised batch workloads; App Service for managed long-running PaaS; Static Web Apps for SPAs/static sites
