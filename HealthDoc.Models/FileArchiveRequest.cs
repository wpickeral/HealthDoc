namespace HealthDoc.Models;

public class FileArchiveRequest
{
    /// <summary>
    /// The name of the file to move.
    /// </summary>
    public string FileName { get; set; } = string.Empty;

    /// <summary>
    /// The destination container. Either lab-results-processed or lab-results-failed.
    /// </summary>
    public string TargetContainer { get; set; } = string.Empty;

    /// <summary>
    /// The reason the file is being moved. Used for logging and observability.
    /// </summary>
    public string Reason { get; set; } = string.Empty;
}