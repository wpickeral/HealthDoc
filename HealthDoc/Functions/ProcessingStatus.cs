using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace HealthDoc;

public class ProcessingStatus
{
    private readonly ILogger<ProcessingStatus> _logger;

    public ProcessingStatus(ILogger<ProcessingStatus> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// HTTP endpoint that lets callers check the progress of a specific batch. Returns
    /// <c>202 Accepted</c> while the orchestration is still running so clients know to
    /// poll again, <c>200 OK</c> with the <see cref="ProcessingSummary"/> on completion,
    /// or <c>500</c> if the orchestration failed or was terminated.
    /// </summary>
    [Function("GetProcessingStatus")]
    public async Task<IActionResult> GetStatus(
        [HttpTrigger(AuthorizationLevel.Function, "get",
            Route = "status/{instanceId}")]
        HttpRequest req,
        [DurableClient] DurableTaskClient client,
        string instanceId)
    {
        var status = await client.GetInstanceAsync(instanceId);

        return status?.RuntimeStatus switch
        {
            OrchestrationRuntimeStatus.Completed => new OkObjectResult(status.SerializedOutput),
            OrchestrationRuntimeStatus.Failed => new ObjectResult(status.SerializedOutput) { StatusCode = 500 },
            OrchestrationRuntimeStatus.Terminated => new ObjectResult("Terminated") { StatusCode = 500 },
            _ => new AcceptedResult() // Still running — client should poll again
        };
    }
}