namespace HealthDoc;

public static class AppConfig
{
    public static class CosmosDb
    {
        public const string Connection = "CosmosDBConnectionString";
        public const string Database = "LabResults";
        public const string SummariesContainer = "ProcessingSummaries";
        public const string LabResultRecordsContainer = "LabResultRecords";
    }

    public static class Blob
    {
        public const string Connection = "StorageConnectionString";
        public const string IncomingContainer = "lab-results-incoming";
        public const string IncomingTriggerPath = "lab-results-incoming/{name}";
        public const string ProcessedContainer = "lab-results-processed";
        public const string FailedContainer = "lab-results-failed";
    }

    public static class Activities
    {
        // + concatenation is required — string interpolation ($"...") is not valid on const declarations
        private const string Prefix = "LabResultOrchestrator_";

        public const string ValidateFile             = Prefix + "ValidateFile";
        public const string ParseFile                = Prefix + "ParseFile";
        public const string ProcessRecord            = Prefix + "ProcessRecord";
        public const string StoreSummary             = Prefix + "StoreSummary";
        public const string StoreRecords             = Prefix + "StoreRecords";
        public const string CheckStorageConfirmation = Prefix + "CheckStorageConfirmation";
        public const string WriteTimeoutSummary      = Prefix + "WriteTimeoutSummary";
        public const string MoveFile                 = Prefix + "MoveFile";
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
}