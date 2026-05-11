# HealthDoc.ReportGenerator

A .NET 10 console app that queries `ProcessingSummaries` from Cosmos DB, builds a CSV report, writes it to Blob Storage, and exits. It runs as a one-shot ACI batch job — triggered on demand, runs to completion, stops.

## What it does

1. Reads all documents from the `LabResults/ProcessingSummaries` Cosmos container
2. Generates a CSV ordered by `ProcessedAt` with columns: `BatchId`, `ClinicId`, `TotalRecords`, `AbnormalCount`, `AbnormalRate%`, `ConfirmationStatus`, `ProcessedAt`
3. Writes the file to the `lab-results-reports` blob container as `report-{yyyyMMdd-HHmmss}.csv`

## Configuration

Two environment variables are required:

| Variable | Value |
|---|---|
| `COSMOS_ENDPOINT` | Cosmos DB account endpoint URL |
| `STORAGE_ENDPOINT` | Storage account blob service endpoint URL |

Authentication uses `DefaultAzureCredential` — `az login` locally, Managed Identity in Azure.

## Running locally

```bash
export COSMOS_ENDPOINT=https://<account>.documents.azure.com:443/
export STORAGE_ENDPOINT=https://<account>.blob.core.windows.net/

dotnet run
```

## Docker

The Dockerfile requires the **repo root** as its build context because it copies both `HealthDoc.Models/` and `HealthDoc.ReportGenerator/`:

```bash
# From the repo root:
docker build -f HealthDoc.ReportGenerator/Dockerfile -t healthdoc-report-generator:latest .

# Test locally:
docker run \
  -e COSMOS_ENDPOINT=https://<account>.documents.azure.com:443/ \
  -e STORAGE_ENDPOINT=https://<account>.blob.core.windows.net/ \
  healthdoc-report-generator:latest
```

## Deploy to Azure Container Instances

**1. Push to ACR:**

```bash
az acr login --name <acr-name>
docker tag healthdoc-report-generator:latest <acr-name>.azurecr.io/healthdoc-report-generator:latest
docker push <acr-name>.azurecr.io/healthdoc-report-generator:latest
```

**2. Create a deployment file and run:**

```bash
cp container.yaml.example container.yaml   # container.yaml is gitignored
# Fill in <placeholders> with real values
az container create --resource-group <rg> --file container.yaml
```

`container.yaml` uses `secureValue` for both environment variables — values are hidden from the portal, `az container show`, and API responses.

**3. On-demand re-runs:**

```bash
az container start --resource-group <rg> --name aci-healthdoc-report-generator
```

With `restartPolicy: Never`, the container stops automatically when the process exits — no manual stop needed, and ACI billing ends immediately.
