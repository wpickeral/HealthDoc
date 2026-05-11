# HealthDoc

A regional healthcare network receives lab result CSV files from partner clinics daily. Previously, a staff member manually downloaded each file, validated it, entered results into the system, and emailed a confirmation back to the clinic — a process that was slow, error-prone, and impossible to scale. 

HealthDoc replaces that workflow with a fully automated ingestion pipeline: partner clinics POST CSV files to an HTTP endpoint through Azure API Management, which triggers an Azure Durable Functions orchestration that validates, parses, processes, stores, and confirms each batch without human intervention.

Built as an AZ-204 exam study project. Every section of this README maps to an exam topic.

Co-authored with [Claude](https://claude.ai) (Anthropic).

---

## Azure Services

| Service | Role in This Project | AZ-204 Topic |
|---|---|---|
| **Azure API Management** | Front door for the HTTP API surface — two products (external subscription key, internal JWT), rate limiting, named values, backend routing | APIM policies, products, subscriptions, named values, validate-jwt |
| **Azure Functions** | HTTP upload endpoint, blob trigger, orchestrator, activity functions, Cosmos DB trigger, failed file listing with SAS URLs | Isolated worker model, HTTP triggers, blob triggers, output bindings |
| **Azure Durable Functions** | Orchestrates the multi-step pipeline with state | Function chaining, fan-out/fan-in, monitor pattern, async HTTP API |
| **Azure Blob Storage** | Receives uploaded CSVs internally; archives processed/failed files; SAS token generation for secure downloads | Blob triggers, storage bindings, server-side copy, SAS tokens |
| **Azure Cosmos DB** | Persists processing summaries and lab records; triggers downstream notification | NoSQL output binding, SDK queries, CosmosDB trigger |
| **Azure Active Directory** | Issues JWT tokens for internal users; two app registrations (API + SPA) with delegated scope `LabResults.Read` | App registrations, OAuth 2.0 scopes, OIDC |
| **MSAL (React SPA)** | Internal dashboard authenticates users via authorization code + PKCE flow; silent token renewal via refresh token | MSAL auth flows, token acquisition, cache strategy |
| **Application Insights** | Telemetry collection with sampling; business event tracking | Monitoring and diagnostics |

---

## Architecture

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
                                                        │ blob write
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
                          │ MoveFile              │  Fan-out
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

Partner Clinic  ──── GET /labs/status/{instanceId} ────► Azure API Management
                     GET /labs/results/{clinicId}                  │
                     Ocp-Apim-Subscription-Key         ┌───────────┴───────────┐
                                                       ▼                       ▼
                                                GetBatchStatus        LabResultsEndpoint
                                           (DurableClient —        (CosmosClient —
                                            queries instance)       queries by clinicId)
                                                       │
                                           202 Accepted  → still running, poll again
                                           200 OK        → completed
                                           500           → failed / terminated

Internal User  ──── Sign In (MSAL popup) ────► Azure Active Directory
(HealthDoc.Dashboard)                               │ access token (LabResults.Read scope)
                                                    │
                     GET /labs/failed-files  ◄──────┘
                     GET /labs/results/{clinicId}
                     Authorization: Bearer <token>
                                      │
                                      ▼
                             Azure API Management
                          (Internal Dashboard product —
                           validate-jwt policy, no subscription key)
                                      │
                         ┌───────────┴───────────┐
                         ▼                       ▼
               FailedLabFilesEndpoint    LabResultsEndpoint
               (lists lab-results-failed  (CosmosClient —
                container + SAS URLs)      queries by clinicId)
```

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

`GetBatchStatus` exposes a single HTTP endpoint that lets any caller check on an orchestration instance by its ID. The instance ID is the generated blob filename returned by `UploadLabResultsEndpoint` in the upload response — the client receives it as `instanceId` and is responsible for persisting it to poll status later. The function uses the `[DurableClient]` binding to call `GetInstanceAsync`, then maps the runtime status to an appropriate HTTP response:

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
│   │   ├── UploadLabResultsEndpoint.cs     # HTTP POST /api/upload → writes blob → triggers pipeline
│   │   ├── LabResultIngestionTrigger.cs    # BlobTrigger entry point → schedules orchestration
│   │   ├── LabResultOrchestrator.cs        # Orchestrator (patterns 1–3)
│   │   ├── BatchStatusEndpoint.cs          # HTTP GET /api/status/{instanceId} (pattern 4)
│   │   ├── LabResultsEndpoint.cs           # HTTP GET /api/results/{clinicId} → Cosmos query
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
├── HealthDoc.Dashboard/                    # Internal React/TypeScript SPA (Vite + MSAL)
│   ├── src/
│   │   ├── main.tsx                        # Entry point — MsalProvider wrapper
│   │   ├── App.tsx                         # Auth gate: login page or Dashboard
│   │   ├── authConfig.ts                   # MSAL config, API scope, APIM base URL
│   │   ├── hooks/useApiToken.ts            # Silent token acquisition with popup fallback
│   │   └── components/
│   │       ├── Dashboard.tsx               # Tab shell — shows logged-in user
│   │       ├── FailedFilesPanel.tsx        # Lists failed CSVs with SAS download links
│   │       └── ResultsPanel.tsx            # Clinic ID search → processed records table
│   └── .env.example                        # Required env vars (tenant ID, client IDs, APIM URL)
│
└── lab_results_2024_05_01.csv             # Sample input file
```

---

## Data Flow

1. **Upload** — A partner clinic POSTs a CSV file body to `POST /labs/upload` through APIM. `UploadLabResultsEndpoint` generates a unique filename (`lab-results-{timestamp}-{shortGuid}.csv`), writes the blob directly to `lab-results-incoming`, and returns `{ "instanceId": "<filename>" }`. The client persists this ID to poll status later.

2. **Trigger** — `LabResultIngestionTrigger` fires automatically via `BlobTrigger` when the blob lands. It reads the blob content, wraps it in a `FilePayload`, and schedules a new `LabResultOrchestrator` instance using the blob filename as the deterministic instance ID (the same value returned to the client as `instanceId`). If an instance with that ID is still running or pending, the duplicate upload is skipped. If the prior instance has already reached a terminal state, it is purged and a fresh instance is scheduled in its place.

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
- `lab-results-incoming` — upload endpoint writes blobs here; BlobTrigger fires automatically
- `lab-results-processed` — successfully processed files are moved here
- `lab-results-failed` — files that fail validation are moved here

Cosmos DB setup required:
- Database: `LabResults`
- Container: `ProcessingSummaries` with partition key `/id`
- Container: `LabResultRecords` with partition key `/ClinicId`

```bash
dotnet restore
dotnet build
cd HealthDoc
func start --port 7220
```

Trigger a run by POSTing a CSV to the upload endpoint:

```bash
curl -X POST http://localhost:7220/api/upload \
  -H "Content-Type: text/csv" \
  -H "x-functions-key: <your-local-function-key>" \
  --data-binary @lab_results_2024_05_01.csv
```

The response returns the `instanceId` to use when polling status:

```json
{
    "instanceId": "lab-results-20260508213642-8582e72b.csv"
}
```

---

## Running Tests

```bash
dotnet test HealthDoc.Tests/HealthDoc.Tests.csproj
```

10 tests across two classes:

- `LabRecordTests` — `From` maps all CSV columns; `From` trims whitespace
- `ProcessedRecordTests` — base field mapping, composite ID format, `IsAbnormal` boundary cases (in-range, at boundary, below min, above max), `ProcessedAt` timestamp precision

---

## Azure API Management

### Why APIM Exists — The Core Problem It Solves

Without APIM, your Function App endpoints are exposed directly to the outside world. Any clinic that knows the URL and function key can call them. In a real healthcare system this creates four problems:

| Problem | Without APIM | With APIM |
|---|---|---|
| **Centralized auth** | Revoking one clinic's access means rotating the Function key — which breaks every other clinic | Each clinic gets its own subscription key; revoke one without touching others |
| **Rate limiting** | A misbehaving clinic can flood the system with uploads | Per-subscription rate limits enforced at the gateway before requests reach your code |
| **Visibility** | No easy way to see which clinic is calling which endpoint, how often, or whether they're succeeding | Every request logged with subscription context; Analytics blade shows call volume, latency, errors per operation |
| **Direct exposure** | External partners know your internal infrastructure details — Azure Functions URLs, routes, versions | The Function App URL becomes an internal implementation detail; clinics only ever see the APIM gateway URL |

The gateway pattern: clinics talk to APIM, APIM talks to your Function App. The `x-functions-key` header is injected by policy — external partners never see it.

---

### Public URL vs Internal URL — Hiding Implementation Details

This is one of APIM's core value propositions. The URL a clinic uses and the URL APIM forwards to internally are completely independent:

| | URL |
|---|---|
| **Client calls (public)** | `https://apim-health-doc-prod.azure-api.net/labs/upload` |
| **APIM forwards to (internal)** | `https://health-doc-bhgtenhbddbmfefr.eastus-01.azurewebsites.net/api/upload` |

The `/labs` prefix is a domain concept — it describes what the API does from the client's perspective. The `/api` prefix is an Azure Functions implementation detail. APIM maps between them so the two never leak into each other.

This decoupling has real consequences:
- You could migrate from Azure Functions to Azure Container Apps and clients would never know — just update the backend URL in APIM
- You could version your API (`/labs/v2/...`) without changing your Function routes
- The internal hostname, which exposes that you're running on Azure and reveals the region (`eastus`), is never visible to external partners

In this project the mapping is configured by setting the **Web service URL** to `https://<func-app>.azurewebsites.net/api` — the Azure Functions route prefix is absorbed into the backend base URL once, so operation overrides stay clean (`/upload`, `/status/{id}`, `/results/{clinicId}`).

---

APIM sits in front of all three HTTP endpoints, adding subscription-key authentication, rate limiting, response caching, and a clean public URL that decouples clinics from the Function App's internal host key.

**AZ-204 exam concepts covered:** products, subscriptions, named values, API-level policies, operation-level policies, `rate-limit-by-key`, `cache-lookup`/`cache-store`, `set-header`, `choose`/`return-response`.

---

### Step 1 — Create the APIM Instance

In the Azure portal: search **API Management** → **Create**.

| Field | Value |
|---|---|
| Subscription / Resource group | your existing RG |
| Resource name | `apim-healthdoc-dev` (must be globally unique) |
| Region | same as your Function App |
| Pricing tier | **Consumption** |
| Organization name | HealthDoc |
| Administrator email | your email |

> Consumption tier is pay-per-call (like Azure Functions) — no hourly charge, ideal for study. Provisioning takes ~5 minutes.

**AZ-204 SKU comparison — know this for the exam:**

| Tier | Cold starts | VNet support | Scale | Best for |
|---|---|---|---|---|
| Consumption | Yes (~2 s) | No | Auto | Dev, testing, low traffic |
| Developer | No | Yes (ext/int) | Manual | Non-production exploration |
| Basic | No | No | Manual | Low-traffic production |
| Standard | No | No | Manual | Medium production |
| Premium | No | Yes (ext/int) | Manual + zone redundancy | Enterprise production |

---

### Step 2 — Create a Named Value for Your Function Key

Named values are APIM's encrypted key-value store. Policy XML references them as `{{Name}}` — the value is never exposed to callers.

1. In your APIM instance → **Named values** → **Add**
2. Fill in:

| Field | Value |
|---|---|
| Name | `FunctionAppKey` |
| Display name | `FunctionAppKey` |
| Type | **Secret** |
| Value | your Function App host key (Function App → **App keys** → `default`) |

---

### Step 3 — Create the Lab Results API

1. **APIs** → **Add API** → **HTTP**
2. Fill in:

| Field | Value |
|---|---|
| Display name | `Lab Results API` |
| Name | `lab-results-api` |
| Web service URL | `https://<your-func-app>.azurewebsites.net/api` |
| API URL suffix | `labs` |

Including `/api` in the Web service URL means the Azure Functions route prefix is set once here rather than repeated in every operation's backend URL override.

All operations on this API are now accessible at `https://<apim>.azure-api.net/labs/...`.

---

### Step 4 — Add the Three Operations

#### Operation 1 — Upload Lab Results

| Field | Value |
|---|---|
| Display name | Upload Lab Results |
| Method | `POST` |
| URL | `/upload` |
| Backend URL override | `/upload` |

#### Operation 2 — Get Processing Status

| Field | Value |
|---|---|
| Display name | Get Processing Status |
| Method | `GET` |
| URL | `/status/{instanceId}` |
| Backend URL override | `/status/{instanceId}` |

#### Operation 3 — Get Lab Results

| Field | Value |
|---|---|
| Display name | Get Lab Results |
| Method | `GET` |
| URL | `/results/{clinicId}` |
| Backend URL override | `/results/{clinicId}` |

---

### Step 5 — Apply Policies

#### API-level policy (applies to all three operations)

Select the API → **Design** tab → **All operations** → **Policies** (the `</>` icon).

```xml
<policies>
    <inbound>
        <base />

        <!-- Authenticate to Function App using stored named value -->
        <!-- Clinic never sees this key — it stays inside APIM -->
        <set-header name="x-functions-key" exists-action="override">
            <value>{{FunctionAppKey}}</value>
        </set-header>

        <!-- Tag every request with the clinic's subscription ID for tracing -->
        <set-header name="x-clinic-id" exists-action="override">
            <value>@(context.Subscription.Id)</value>
        </set-header>

    </inbound>
    <backend>
        <base />
    </backend>
    <outbound>
        <base />
        <!-- Strip internal headers before returning response to clinic -->
        <set-header name="x-functions-key" exists-action="delete" />
        <set-header name="x-powered-by" exists-action="delete" />
    </outbound>
    <on-error>
        <base />
    </on-error>
</policies>
```

#### Operation-level policy — Upload only (rate limiting + size guard)

Select the **Upload Lab Results** operation → **Policies**.

```xml
<policies>
    <inbound>
        <base />

        <!-- Limit each clinic to 10 uploads per minute -->
        <!-- counter-key uses the subscription ID so each clinic has its own counter -->
        <!-- NOTE: rate-limit-by-key is not supported on the Consumption tier -->
        <!-- Requires Developer tier or above — include here as a study reference -->
        <rate-limit-by-key
            calls="10"
            renewal-period="60"
            counter-key="@(context.Subscription.Id)"
            increment-condition="@(context.Response.StatusCode == 201)"
        />

        <!-- Reject requests that are not CSV -->
        <!-- Content-Length is unreliable for streaming uploads — validate Content-Type instead -->
        <choose>
            <when condition='@(!context.Request.Headers.GetValueOrDefault("Content-Type", "").Contains("text/csv"))'>
                <return-response>
                    <set-status code="415" reason="Unsupported Media Type" />
                    <set-body>Content-Type must be text/csv</set-body>
                </return-response>
            </when>
        </choose>

    </inbound>
    <backend>
        <base />
    </backend>
    <outbound>
        <base />
    </outbound>
    <on-error>
        <base />
    </on-error>
</policies>
```

#### Operation-level policy — Get Lab Results (caching)

Select the **Get Lab Results** operation → **Policies**.

> **Note — Consumption tier:** `cache-lookup` / `cache-store` are supported on all tiers including Consumption, but the Consumption tier has no built-in cache. Without an external Redis Cache configured (APIM → **External cache**), these policies are accepted but silently do nothing. To enable caching on Consumption, provision an Azure Cache for Redis and link it under External cache. On Developer tier and above the built-in cache works with no additional setup.

```xml
<policies>
    <inbound>
        <base />

        <!-- Check cache before calling Function App -->
        <!-- Cache key = full request URL by default, so /results/CLINIC_001 and      -->
        <!-- /results/CLINIC_002 automatically get separate cache entries — no extra   -->
        <!-- configuration needed because clinicId is already in the path.             -->
        <!-- If clinicId were a query parameter (?clinicId=X) instead, you would need -->
        <!-- to add <vary-by-query-parameter>clinicId</vary-by-query-parameter> here.  -->
        <cache-lookup vary-by-developer="false"
                      vary-by-developer-groups="false"
                      allow-private-response-caching="true">
        </cache-lookup>

    </inbound>
    <backend>
        <base />
    </backend>
    <outbound>
        <base />

        <!-- Store the response in cache for 60 seconds -->
        <cache-store duration="60" />

    </outbound>
    <on-error>
        <base />
    </on-error>
</policies>
```

---

### Step 6 — Create a Product and Subscription

**Create the product:**

1. **Products** → **Add**
2. Fill in:

| Field | Value |
|---|---|
| Display name | `Clinic Standard` |
| Id | `clinic-standard` |
| Description | `Standard access tier for registered clinics. Provides authenticated upload, status polling, and results retrieval. Each clinic receives a unique subscription key.` |
| Requires subscription | ✅ checked |
| Requires approval | unchecked |
| State | Published |

3. Under **APIs**, add **Lab Results API** to the product.

**Create a test subscription:**

1. **Subscriptions** → **Add**
2. Fill in:

| Field | Value |
|---|---|
| Name | `clinic-001-test` |
| Display name | `CLINIC_001 Test` |
| Scope | Product → `Clinic Standard` |

3. Save, then click **...** → **Show/hide keys** → copy the primary key.

---

### Step 7 — Test Through APIM

Use the built-in APIM test console (**APIs** → **Lab Results API** → **Test** tab) or Postman.

**Upload a CSV:**

```
POST https://<apim>.azure-api.net/labs/upload
Ocp-Apim-Subscription-Key: <your-subscription-key>
Content-Type: text/csv

ClinicId,PatientId,TestCode,Result,Unit,ReferenceRange,CollectedAt
CLINIC_001,PAT_123,HBA1C,6.2,%,4.0-5.6,2024-05-01T08:30:00
CLINIC_001,PAT_124,HBA1C,5.1,%,4.0-5.6,2024-05-01T09:00:00
CLINIC_001,PAT_125,GLUCOSE,210,mg/dL,70-100,2024-05-01T09:15:00
```

Response `201 Created`:
```json
{
    "instanceId": "lab-results-20260508213642-8582e72b.csv"
}
```

**Poll status** (use the `instanceId` from the upload response):

```
GET https://<apim>.azure-api.net/labs/status/lab-results-20240501143022-a3f9b21c.csv
Ocp-Apim-Subscription-Key: <your-subscription-key>
```

Returns `202 Accepted` while running, `200 OK` with `ProcessingSummary` JSON when complete.

**Query results for the clinic:**

```
GET https://<apim>.azure-api.net/labs/results/CLINIC_001
Ocp-Apim-Subscription-Key: <your-subscription-key>
```

**Verify rate limiting** — send the upload request 11+ times within a minute; calls beyond 10 should return `429 Too Many Requests`.

---

## Azure Security (AZ-204 — Implement Azure Security)

This section adds MSAL-authenticated access for internal users via a React SPA, plus JWT validation on APIM so the same Azure AD token authorizes the call end-to-end.

### Why Subscription Keys for External Clinics, JWT for Internal Users?

External clinics use **APIM subscription keys** (`Ocp-Apim-Subscription-Key`). This is the right fit here because the unit of identity is the clinic itself — one key per clinic, provisioned and revoked by the platform team. It's simple, doesn't require clinics to adopt any identity provider, and APIM manages the lifecycle natively.

Internal users use **JWT tokens** issued by Azure AD. Internal users have individual identities (their org account), so a shared key would be a step backwards — you'd lose the ability to know *who* acted, not just *which system*.

The key distinction: **subscription keys authenticate a system; JWT tokens authenticate a person.**

### When Would JWT Add Value for External Clinics?

There are scenarios where you'd want to move external clinics off subscription keys and onto JWT:

| Scenario | Why JWT wins |
|---|---|
| **Multiple users per clinic** | A clinic has admins and read-only staff — JWT claims/roles let APIM or the backend enforce per-user permissions. Subscription keys give everyone at that clinic identical access. |
| **The clinic already has an Azure AD tenant** | You can configure B2B federation so clinics log in with *their own* org credentials. No separate credentials to provision or rotate. |
| **Audit and compliance requirements** | JWT carries a user identity (`sub`, `oid` claims). You can log exactly which person uploaded a file, not just which clinic. Subscription keys only tell you the clinic. |
| **Short-lived credential requirements** | Tokens expire (typically 1 hour) and refresh automatically via MSAL. Subscription keys are long-lived and require manual rotation if compromised. |
| **Delegated access** | A clinic could grant a third-party billing system scoped access to their results only, using OAuth scopes. Subscription keys are all-or-nothing. |

In this project, clinics are external organizations with no Azure AD relationship, and the unit of trust is the clinic as a whole — so subscription keys are the correct and simpler choice. The `validate-jwt` policy is applied to the internal dashboard product, where individual identity and Azure AD integration are natural fits.

### Architecture

```
Internal User
   │
   ▼
HealthDoc.Dashboard (React + MSAL)          ← HealthDoc.Dashboard/
   │ 1. MSAL login (auth code + PKCE)
   │ 2. Acquire access token (LabResults.Read scope)
   │ 3. GET /labs/failed-files or /labs/results/{clinicId}
   │    Authorization: Bearer <token>
   ▼
APIM — "Internal Dashboard" product (subscriptionRequired: false)
   │ validate-jwt policy → Azure AD OIDC config
   │ x-functions-key injected (existing API-level policy)
   ▼
Function App
   ├── GET /api/blobs/failed   → FailedLabFilesEndpoint.cs  (list + SAS URLs)
   └── GET /api/results/{clinicId}  → LabResultsEndpoint.cs (existing)
```

### Step 1 — Azure AD App Registrations (Azure Portal)

**Register the API app (`HealthDoc-API`):**
1. Azure Active Directory → App registrations → New registration
2. Name: `HealthDoc-API`, supported account types: single tenant
3. After creating: Expose an API → Add a scope
   - Scope name: `LabResults.Read`
   - Who can consent: Admins and users
   - Save — note the `api://<api-client-id>` Application ID URI

**Register the SPA app (`HealthDoc-Dashboard`):**
1. New registration — Name: `HealthDoc-Dashboard`, single tenant
2. Add platform: Single-page application, redirect URI: `http://localhost:5173`
3. API permissions → Add permission → My APIs → `HealthDoc-API` → `LabResults.Read` → Grant admin consent

### Step 2 — APIM Named Values

In Azure Portal → APIM → Named values, add:

| Name | Value |
|---|---|
| `TenantId` | Your Azure AD tenant ID |
| `ApiClientId` | Client ID of the `HealthDoc-API` registration |

### Step 3 — APIM Internal Dashboard Product

1. APIM → Products → Add
   - Name: `Internal Dashboard`
   - Description: `Internal access for authenticated staff. Provides read access to processed results and failed file inspection. Secured via Azure AD JWT validation — no subscription key required.`
   - `subscriptionRequired`: **off**
   - Published: on

2. Add two operations to this product (same API as the existing "External Clinics" product):
   - **List Failed Files**: `GET /labs/failed-files` → backend `GET /api/blobs/failed`
   - **Get Lab Results**: `GET /labs/results/{clinicId}` → backend `GET /api/results/{clinicId}` (reuse)

3. Set inbound policy **on the product** — not on the API:

> **Important:** This policy must be applied at the product level, not the API level. API-level policies run for every request regardless of which product it came through — placing `validate-jwt` there would require external clinics to present a JWT token in addition to their subscription key, breaking their access. Product-level policies only run for requests that arrive through that specific product, keeping the two access models independent.

#### APIM Policy Execution Order

Policies do not override each other — they **stack**. Every request passes through all three layers in sequence:

```
Product policy  →  API policy  →  Operation policy  →  Backend
```

For a clinic uploading a CSV (Clinic Standard product):
1. **Product** — subscription key validated automatically (`subscriptionRequired: true`)
2. **API** — `x-functions-key` injected, `x-clinic-id` tagged, response headers cleaned
3. **Operation** — `rate-limit-by-key` enforced, Content-Type validated

For an internal user fetching results (Internal Dashboard product):
1. **Product** — `validate-jwt` checks the Azure AD bearer token
2. **API** — `x-functions-key` injected, response headers cleaned
3. **Operation** — (none configured)

This is why `validate-jwt` must live at the product level rather than the API level. If it were placed at the API level it would sit in step 2 for both products, forcing external clinics to present a JWT token on top of their subscription key.

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

The existing API-level inbound policy already injects `x-functions-key`, so no change needed there.

### Step 4 — Frontend Setup

```bash
cd HealthDoc.Dashboard
cp .env.example .env
# Fill in your tenant ID and client IDs in .env
npm install
npm run dev
```

Then navigate to `http://localhost:5173`. Click **Sign In**, complete Azure AD authentication, and the dashboard loads with two tabs:

- **Failed Files** — lists CSVs that failed validation, with a one-hour SAS download link each
- **Lab Results** — enter a Clinic ID to query processed records

### Verify End-to-End

1. Upload an invalid CSV (missing columns) via the existing APIM upload endpoint — it lands in `lab-results-failed`
2. Sign in to the dashboard, open **Failed Files** — the file should appear with a working download link
3. Call `GET https://<apim>.azure-api.net/labs/failed-files` **without** a token → expect `401 Unauthorized`
4. Call the same endpoint **with** a valid token (copy from browser DevTools Network tab) → expect `200 OK`

---

## Azure Key Vault & Managed Identity

### Why Plaintext Secrets Are the Problem

The original `local.settings.json` stored connection strings containing real account keys. Any developer who clones the repo, any CI log that prints environment variables, any accidental git commit of the file — all of these expose credentials that give full access to the storage account and Cosmos DB database. Key Vault and Managed Identity solve this at both layers:

| Layer | Problem | Solution |
|---|---|---|
| **At rest** | Connection strings stored in config files, app settings, or environment variables | Secrets stored in Key Vault; app settings hold a reference, not the value |
| **In transit** | App authenticates to Azure services using a shared key anyone can copy | App authenticates using its Azure identity — no secret to steal or rotate |

### What Changed in This Project

**`Program.cs`** — `CosmosClient` and `BlobServiceClient` now authenticate with `DefaultAzureCredential` and a service endpoint URI instead of a connection string:

```csharp
var credential = new DefaultAzureCredential();

// CosmosClient — no key, no connection string
new CosmosClient(endpoint, credential);

// BlobServiceClient — no key, no connection string
new BlobServiceClient(new Uri(endpoint), credential);
```

**Binding attributes** (`[CosmosDBTrigger]`, `[CosmosDBOutput]`, `[BlobTrigger]`) still reference a named connection string because the Functions runtime resolves these — not the SDK. In Azure, those app settings are Key Vault references that the runtime transparently resolves before passing to the binding provider. Locally, they remain plaintext in `local.settings.json` which is never committed.

**`local.settings.json` additions:**

```json
"CosmosDBEndpoint": "https://<account>.documents.azure.com:443/",
"StorageAccountEndpoint": "https://<account>.blob.core.windows.net/",
"KeyVaultEndpoint": "https://kv-healthdoc-dev.vault.azure.net/"
```

### DefaultAzureCredential Chain

`DefaultAzureCredential` tries these credential sources in order and uses the first that succeeds:

| Order | Credential type | When it applies |
|---|---|---|
| 1 | Environment credential | `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID` set |
| 2 | Workload Identity | Azure Kubernetes Service with federated credentials |
| 3 | Managed Identity | Running inside Azure (App Service, Functions, VM, ACI) |
| 4 | Visual Studio credential | Signed in to Visual Studio |
| 5 | Azure CLI credential | `az login` has been run locally |
| 6 | Azure PowerShell credential | `Connect-AzAccount` has been run locally |
| 7 | Interactive browser | Falls back to browser login if nothing else works |

In this project: locally → credential #5 (`az login`). In Azure → credential #3 (Managed Identity). No code change needed between environments.

### Azure Portal Setup — Step by Step

#### Step 1 — Create the Key Vault

Search **Key Vault** → **Create**.

| Field | Value |
|---|---|
| Resource group | same as Function App |
| Name | `kv-healthdoc-dev` (globally unique) |
| Region | same region as all other resources |
| Pricing tier | Standard |
| Soft-delete | enabled (default) — protects against accidental deletion |
| Purge protection | enabled — prevents hard-delete during retention period |

#### Step 2 — Store Secrets

Key Vault → **Secrets** → **Generate/Import** for each:

| Secret name | Value |
|---|---|
| `CosmosDBConnectionString` | Full Cosmos DB connection string (from Cosmos DB → Keys) |
| `StorageConnectionString` | Full storage account connection string (from Storage → Access keys) |

#### Step 3 — Enable System-Assigned Managed Identity

Function App → **Identity** → **System assigned** → **Status: On** → Save.

Azure creates a service principal in Azure AD with the same name as the Function App. This identity is the "who" for all RBAC role assignments below.

**System-assigned vs user-assigned (exam concept):**

| | System-assigned | User-assigned |
|---|---|---|
| Lifecycle | Tied to the resource — deleted with the Function App | Independent — persists if the resource is deleted |
| Sharing | One identity per resource | One identity can be assigned to many resources |
| Best for | Single-resource use, simple setup | Shared identity across resources, pre-provisioned credentials |

This project uses system-assigned because the identity is only needed by this one Function App.

#### Step 4 — Grant Key Vault Access

Key Vault → **Access control (IAM)** → **Add role assignment**:

| Field | Value |
|---|---|
| Role | `Key Vault Secrets User` |
| Assign access to | Managed Identity |
| Members | Select your Function App |

> **RBAC vs Access Policies:** Key Vault supports two permission models. The older model is **access policies** (vault-level; grant/deny per principal per secret). The newer model is **Azure RBAC** (consistent with all other Azure resources; use role assignments). RBAC is the recommended approach and what this project uses. Both appear on the AZ-204 exam — know the difference.

#### Step 5 — Grant Cosmos DB Access

Cosmos DB account → **Access control (IAM)** → **Add role assignment**:

| Role | Assignee |
|---|---|
| `Cosmos DB Built-in Data Contributor` | Function App Managed Identity |

This role allows read and write to data plane (documents). It does NOT allow management plane operations (creating databases, changing throughput, etc.) — principle of least privilege.

#### Step 6 — Grant Blob Storage Access

Storage account → **Access control (IAM)** → **Add role assignment**:

| Role | Assignee |
|---|---|
| `Storage Blob Data Contributor` | Function App Managed Identity |

#### Step 7 — Replace App Settings with Key Vault References

Function App → **Configuration** → **Application settings**.

For each connection string setting, replace the raw value with a Key Vault reference in this format:

```
@Microsoft.KeyVault(VaultName=kv-healthdoc-dev;SecretName=CosmosDBConnectionString)
```

The Functions runtime resolves the reference at startup. The app code reads `Environment.GetEnvironmentVariable("CosmosDBConnectionString")` and receives the secret value — no SDK change needed for the binding layer.

Also add the two new endpoint settings:

| Name | Value |
|---|---|
| `CosmosDBEndpoint` | `https://<account>.documents.azure.com:443/` |
| `StorageAccountEndpoint` | `https://<account>.blob.core.windows.net/` |

---

## Azure Service Bus

### Why Service Bus (and not just the Cosmos DB trigger)?

The existing `DownstreamSystemNotifier` already handles post-processing notifications via a Cosmos DB trigger. Service Bus solves a different class of problem:

| Concern | Cosmos DB trigger | Service Bus |
|---|---|---|
| **Delivery guarantee** | At-least-once, tied to change feed | At-least-once with configurable retry and DLQ |
| **Consumer model** | All trigger instances receive all changes | Competing consumers — one message processed by exactly one consumer |
| **Fan-out** | All trigger instances process the same document | Topics fan out to multiple independent subscriptions simultaneously |
| **Filtering** | None — all documents in the container trigger | SQL filters on subscriptions route messages by content |
| **Decoupling** | Consumer must know the Cosmos account | Consumer only needs a connection string to the namespace |
| **Cross-system** | Cosmos-native — hard to bridge to non-Azure consumers | Any system that supports AMQP or HTTP can receive messages |

This project keeps **both** patterns so both are documented — they are complementary, not competing.

### What Was Added

After `StoreSummary` writes the batch summary to Cosmos, the orchestrator calls two new activities:

1. **`BatchCompletePublisher`** — sends a `BatchCompletedMessage` to the `lab-results-notifications` **queue** via `[ServiceBusOutput]` binding. Every completed batch goes here.
2. **`AbnormalAlertPublisher`** — sends the same message to the `lab-results-alerts` **topic**, but only when `AbnormalCount > 0`. Two subscriptions on the topic filter this independently.

A new **`ServiceBusLabResultNotifier`** function consumes the notifications queue with `[ServiceBusTrigger]` and emits an App Insights custom event. A **`ServiceBusDeadLetterMonitor`** timer function peeks the dead-letter sub-queue every 5 minutes and logs any messages found there.

### Queues vs Topics vs Subscriptions

```
Queue (lab-results-notifications)
  └─ One message → one consumer
     Used for: guaranteed delivery of every batch to exactly one notifier

Topic (lab-results-alerts)
  ├─ Subscription: clinical-alerts    (no filter — receives all messages)
  └─ Subscription: critical-alerts    (SQL filter: AbnormalCount > 5)
     Used for: fan-out — one message delivered independently to each matching subscription
```

**AZ-204 exam rule:** Use a queue when one consumer should process each message. Use a topic when multiple independent consumers each need their own copy, optionally with content-based filtering.

### Peek-Lock vs Receive-and-Delete

`[ServiceBusTrigger]` uses **peek-lock** by default, which is the safer and more common mode:

| | Peek-lock | Receive-and-delete |
|---|---|---|
| **How it works** | Message is locked (invisible) while processing; completed on success, released on failure | Message is deleted immediately on receipt — no retry possible |
| **On function exception** | Lock expires → message reappears → redelivered | Message is gone — data loss on failure |
| **MaxDeliveryCount** | After N failures, message moves to dead-letter queue | N/A — no redelivery |
| **Best for** | Any processing where you cannot afford to lose a message | Low-value idempotent operations where duplicate processing is worse than loss |

### Dead-Letter Queue

Messages land in the dead-letter sub-queue (`{queue}/$DeadLetterQueue`) when:
- Delivery count exceeds `MaxDeliveryCount` (default: 10) after repeated processing failures
- Message TTL expires before it is consumed
- A consumer explicitly dead-letters it via `DeadLetterMessageAsync`

`ServiceBusDeadLetterMonitor` peeks (not receives) so messages remain visible for human inspection. The `SubQueue.DeadLetter` receiver option targets the sub-queue directly:

```csharp
_serviceBusClient.CreateReceiver(
    AppConfig.ServiceBus.NotificationsQueue,
    new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
```

### Message TTL and Duplicate Detection

Two additional Service Bus features worth knowing for the exam:

**Message TTL** — set `TimeToLive` on `ServiceBusMessage` or at the queue/topic level. Expired messages are dead-lettered (if dead-lettering on expiration is enabled) or silently discarded.

**Duplicate detection** — enable on the queue/topic and set a `MessageId` on each message. Service Bus discards any message with an ID it has seen within the duplicate detection window (default: 10 minutes). Useful for idempotent retry scenarios.

### Azure Portal Setup

#### Step 1 — Create the Service Bus Namespace

Search **Service Bus** → **Create**.

| Field | Value |
|---|---|
| Resource group | same as Function App |
| Namespace name | `sb-healthdoc-dev` (globally unique) |
| Region | same region |
| Pricing tier | **Standard** (required for topics; Basic only supports queues) |

#### Step 2 — Create the Notifications Queue

Namespace → **Queues** → **Add**:

| Field | Value |
|---|---|
| Name | `lab-results-notifications` |
| Max delivery count | 10 |
| Message TTL | 14 days (default) |
| Dead-lettering on message expiration | enabled |

#### Step 3 — Create the Alerts Topic and Subscriptions

Namespace → **Topics** → **Add**:

| Field | Value |
|---|---|
| Name | `lab-results-alerts` |

Then open the topic → **Subscriptions** → **Add** twice:

**Subscription 1:**

| Field | Value |
|---|---|
| Name | `clinical-alerts` |
| Filter type | None (receives all messages) |

**Subscription 2:**

| Field | Value |
|---|---|
| Name | `critical-alerts` |
| Filter type | SQL filter |
| Filter expression | `AbnormalCount > 5` |

#### Step 4 — Copy the Connection String

Namespace → **Shared access policies** → `RootManageSharedAccessKey` → copy the primary connection string. Add it to `local.settings.json` as `ServiceBusConnectionString`, and to the Function App configuration (or as a Key Vault secret referenced via `@Microsoft.KeyVault(...)`).

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
- [ ] **HTTP trigger (upload)** — `UploadLabResultsEndpoint.cs`; accepts CSV body, generates filename, writes to Blob Storage via `BlobServiceClient`, returns `instanceId`
- [ ] **API Management** — Consumption SKU; named values, product, subscription, three operations
- [ ] **APIM policies** — API-level: `set-header` (key injection, clinic-id tagging), outbound header cleanup; operation-level: `rate-limit-by-key`, `choose`/`return-response` size guard, `cache-lookup`/`cache-store`
- [ ] **MSAL authentication** — `HealthDoc.Dashboard` SPA uses `@azure/msal-react`; authorization code + PKCE flow, silent token renewal, popup fallback
- [ ] **JWT validation policy** — APIM `validate-jwt` on Internal Dashboard product verifies Azure AD tokens against OIDC discovery endpoint
- [ ] **SAS token generation** — `FailedLabFilesEndpoint.cs` generates time-limited read-only SAS URIs for blob downloads via `BlobClient.GenerateSasUri`
- [ ] **Azure AD app registration** — two registrations: API app exposes `LabResults.Read` scope; SPA app consumes it
- [ ] **Azure Key Vault** — secrets stored in Key Vault; app settings reference them via `@Microsoft.KeyVault(...)`; runtime resolves transparently
- [ ] **Managed Identity** — system-assigned identity on Function App; `DefaultAzureCredential` resolves to Managed Identity in Azure and `az login` locally
- [ ] **Passwordless SDK clients** — `CosmosClient(endpoint, credential)` and `BlobServiceClient(uri, credential)` in `Program.cs`; no connection strings in SDK layer
- [ ] **RBAC role assignments** — `Key Vault Secrets User`, `Cosmos DB Built-in Data Contributor`, `Storage Blob Data Contributor` granted to the Function App identity
- [ ] **DefaultAzureCredential chain** — ordered credential resolution: environment → workload identity → managed identity → VS → CLI → PowerShell → browser
- [ ] **System-assigned vs user-assigned identity** — system-assigned lifecycle tied to the resource; user-assigned independent and shareable
- [ ] **Service Bus queue** — `BatchCompletePublisher.cs` uses `[ServiceBusOutput]`; `ServiceBusLabResultNotifier.cs` uses `[ServiceBusTrigger]` with peek-lock
- [ ] **Service Bus topic + subscriptions** — `AbnormalAlertPublisher.cs` publishes to `lab-results-alerts` topic; two subscriptions (all alerts / SQL filter `AbnormalCount > 5`)
- [ ] **Dead-letter queue** — `ServiceBusDeadLetterMonitor.cs` peeks DLQ via `SubQueue.DeadLetter` receiver option on a timer trigger
- [ ] **Queues vs topics** — queue = competing consumers, one delivery; topic = fan-out, each subscription gets its own copy
- [ ] **Peek-lock vs receive-and-delete** — peek-lock (default) re-delivers on failure; receive-and-delete removes immediately with no retry
- [ ] **Message TTL and duplicate detection** — configurable at queue/topic level; duplicate detection deduplicates by MessageId within a time window
