namespace HealthDoc.Models;

/// Result of ValidateFile activity
public class ValidationResult
{
    public bool          IsValid { get; set; }
    public List<string>  Errors  { get; set; } = [];
}