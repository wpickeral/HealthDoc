using Azure.Identity;
using Azure.Messaging.EventGrid;
using Azure.Messaging.ServiceBus;
using Azure.Storage.Blobs;
using Azure;
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

// DefaultAzureCredential resolves: az login locally → Managed Identity in Azure.
// No connection string or secret is read here — passwordless authentication throughout.
var credential = new DefaultAzureCredential();

// CosmosClient — thread safe, registered as singleton, authenticated via Managed Identity
builder.Services.AddSingleton(sp =>
{
    var endpoint = Environment.GetEnvironmentVariable(AppConfig.CosmosDb.Endpoint)
        ?? throw new InvalidOperationException($"{AppConfig.CosmosDb.Endpoint} is not configured");
    return new CosmosClient(endpoint, credential);
});

// BlobServiceClient — thread safe, registered as singleton, authenticated via Managed Identity
builder.Services.AddSingleton(sp =>
{
    var endpoint = Environment.GetEnvironmentVariable(AppConfig.Blob.Endpoint)
        ?? throw new InvalidOperationException($"{AppConfig.Blob.Endpoint} is not configured");
    return new BlobServiceClient(new Uri(endpoint), credential);
});

// ServiceBusClient — thread safe, registered as singleton, used by the DLQ monitor
builder.Services.AddSingleton(sp =>
{
    var connection = Environment.GetEnvironmentVariable(AppConfig.ServiceBus.Connection)
        ?? throw new InvalidOperationException($"{AppConfig.ServiceBus.Connection} is not configured");
    return new ServiceBusClient(connection);
});

// EventGridPublisherClient — publishes custom events to the HealthDoc custom topic.
// Uses AzureKeyCredential (topic access key) locally. In Azure, swap for DefaultAzureCredential
// and grant the Function App's Managed Identity the "EventGrid Data Sender" RBAC role on the topic.
builder.Services.AddSingleton(sp =>
{
    var endpoint = Environment.GetEnvironmentVariable(AppConfig.EventGrid.TopicEndpoint)
        ?? throw new InvalidOperationException($"{AppConfig.EventGrid.TopicEndpoint} is not configured");
    var key = Environment.GetEnvironmentVariable(AppConfig.EventGrid.TopicKey)
        ?? throw new InvalidOperationException($"{AppConfig.EventGrid.TopicKey} is not configured");
    return new EventGridPublisherClient(new Uri(endpoint), new AzureKeyCredential(key));
});

builder.Build().Run();