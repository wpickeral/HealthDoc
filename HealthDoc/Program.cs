using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Storage.Blobs;
using HealthDoc;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

builder.Services
    .AddApplicationInsightsTelemetryWorkerService()
    .ConfigureFunctionsApplicationInsights();

// CosmosClient is thread safe — register as singleton
builder.Services.AddSingleton(sp =>
{
    var connectionString = Environment.GetEnvironmentVariable(AppConfig.CosmosDb.Connection);
    return new CosmosClient(connectionString);
});

// BlobServiceClient is thread safe — register as singleton
builder.Services.AddSingleton(sp =>
{
    var connectionString = Environment.GetEnvironmentVariable(AppConfig.Blob.Connection);
    return new BlobServiceClient(connectionString);
});

builder.Build().Run();