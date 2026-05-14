namespace HealthDoc;

public static class AppConfig
{
    public static class CosmosDb
    {
        public const string Connection            = "CosmosDBConnectionString"; // binding attributes
        public const string Endpoint              = "CosmosDBEndpoint";         // SDK client (passwordless)
        public const string Database              = "LabResults";
        public const string SummariesContainer    = "ProcessingSummaries";
        public const string LabResultRecordsContainer = "LabResultRecords";
        public const string AuditLogContainer     = "AuditLog";
    }

    public static class Blob
    {
        public const string Connection        = "StorageConnectionString";  // binding attributes
        public const string Endpoint          = "StorageAccountEndpoint";   // SDK client (passwordless)
        public const string IncomingContainer = "lab-results-incoming";
        public const string IncomingTriggerPath = "lab-results-incoming/{name}";
        public const string ProcessedContainer = "lab-results-processed";
        public const string FailedContainer   = "lab-results-failed";

        // Stored access policy ID for service SAS tokens on the failed-files container.
        // A service SAS that references this policy can be revoked instantly by deleting the policy.
        // See README.md "Stored Access Policies" for the CLI commands.
        public const string FailedReadPolicyId = "failed-read";
    }

    public static class KeyVault
    {
        public const string Endpoint = "KeyVaultEndpoint";
    }

    public static class Activities
    {
        // + concatenation is required — string interpolation ($"...") is not valid on const declarations
        private const string Prefix = "LabResultOrchestrator_";

        public const string ValidateFile = Prefix + "ValidateFile";
        public const string ParseFile = Prefix + "ParseFile";
        public const string ProcessRecord = Prefix + "ProcessRecord";
        public const string StoreSummary = Prefix + "StoreSummary";
        public const string StoreRecords = Prefix + "StoreRecords";
        public const string CheckStorageConfirmation = Prefix + "CheckStorageConfirmation";
        public const string WriteTimeoutSummary = Prefix + "WriteTimeoutSummary";
        public const string MoveFile = Prefix + "MoveFile";
        public const string NotifyDownstreamSystems = Prefix + "NotifyDownstreamSystems";
        public const string PublishBatchComplete     = Prefix + "PublishBatchComplete";
        public const string PublishAbnormalAlert     = Prefix + "PublishAbnormalAlert";
        public const string PublishAbnormalEvent     = Prefix + "PublishAbnormalEvent";
    }

    public static class Redis
    {
        public const string Connection = "RedisConnectionString";
        public static string ResultsCacheKey(string clinicId) => $"results:{clinicId}";
        public static readonly TimeSpan DefaultTtl = TimeSpan.FromSeconds(60);
    }

    public static class EventGrid
    {
        public const string TopicEndpoint = "EventGridTopicEndpoint";
        public const string TopicKey      = "EventGridTopicKey";
        public const string Source        = "/healthdoc/labs/orchestrator";
        public const string AbnormalResultDetectedType = "HealthDoc.Lab.AbnormalResultDetected";
    }

    public static class ServiceBus
    {
        public const string Connection         = "ServiceBusConnectionString";
        public const string NotificationsQueue = "lab-results-notifications";
        public const string AlertsTopic        = "lab-results-alerts";
        public const string ClinicalAlertsSub  = "clinical-alerts";
        public const string CriticalAlertsSub  = "critical-alerts";
    }

    public static class Metrics
    {
        public const string PipelineDuration = "PipelineDurationSeconds";

        public static class Dimensions
        {
            public const string FileName = "FileName";
            public const string BatchId = "BatchId";
            public const string Status = "Status";
        }
    }

    public static class Analytics
    {
        public static class CustomEvents
        {
            public const string LabResultsProcessed    = "LabResultsProcessed";
            public const string FileValidationFailed   = "FileValidationFailed";
            public const string LabResultsBatchComplete = "LabResultsBatchComplete";
            public const string ClinicalAlertReceived  = "ClinicalAlertReceived";
            public const string CriticalAlertReceived  = "CriticalAlertReceived";
        }
    }
}