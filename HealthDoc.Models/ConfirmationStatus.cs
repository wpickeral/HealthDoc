namespace HealthDoc.Models;

public enum ConfirmationStatus
{
    Unknown = 0,    // document just written — monitor hasn't run yet
    Pending = 1,    // monitor checked — document not found yet
    Confirmed = 2,  // monitor confirmed — batch fully processed
    TimedOut = 3    // monitor exhausted max attempts without confirmation
}