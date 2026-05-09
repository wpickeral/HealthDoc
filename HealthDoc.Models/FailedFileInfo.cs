namespace HealthDoc.Models;

public record FailedFileInfo(string FileName, string DownloadUrl, DateTimeOffset? UploadedAt);
