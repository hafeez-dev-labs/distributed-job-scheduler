using DistributedJobScheduler.Application;
using DistributedJobScheduler.Contracts;
using DistributedJobScheduler.Domain;
using Microsoft.AspNetCore.Mvc;

namespace DistributedJobScheduler.Api.Controllers;

[ApiController]
[Route("jobs")]
public sealed class JobsController(IJobManagementService service) : ControllerBase
{
    [HttpPost]
    public async Task<ActionResult<JobResponse>> Create(CreateJobRequest request, CancellationToken cancellationToken)
    {
        try
        {
            var job = await service.CreateAsync(request, GetIdempotencyKey(), cancellationToken);
            return Created($"/jobs/{job.Id}", ToResponse(job));
        }
        catch (ArgumentException exception)
        {
            return BadRequest(exception.Message);
        }
    }

    [HttpGet("{id:guid}")]
    public async Task<ActionResult<JobResponse>> Get(Guid id, CancellationToken cancellationToken)
    {
        var job = await service.GetAsync(id, cancellationToken);
        return job is null ? NotFound() : Ok(ToResponse(job));
    }

    [HttpGet("{id:guid}/dependencies")]
    public async Task<ActionResult<IReadOnlyList<JobDependencyResponse>>> GetDependencies(Guid id, CancellationToken cancellationToken)
    {
        var job = await service.GetAsync(id, cancellationToken);
        if (job is null)
            return NotFound();

        var dependencies = await service.GetDependenciesAsync(id, cancellationToken);
        return Ok(dependencies.Select(dependency => new JobDependencyResponse(dependency.JobId, dependency.DependsOnJobId)).ToArray());
    }

    [HttpPost("{id:guid}/dependencies")]
    public async Task<ActionResult<JobDependencyResponse>> AddDependency(Guid id, AddJobDependencyRequest request, CancellationToken cancellationToken)
    {
        try
        {
            await service.AddDependencyAsync(id, request.DependsOnJobId, cancellationToken);
            return Created(
                $"/jobs/{id}/dependencies/{request.DependsOnJobId}",
                new JobDependencyResponse(id, request.DependsOnJobId));
        }
        catch (InvalidOperationException exception)
        {
            return Conflict(exception.Message);
        }
    }

    [HttpPost("{id:guid}/pause")]
    public Task<ActionResult<JobResponse>> Pause(Guid id, CancellationToken cancellationToken) =>
        ChangeState(() => service.PauseAsync(id, cancellationToken));

    [HttpPost("{id:guid}/resume")]
    public Task<ActionResult<JobResponse>> Resume(Guid id, CancellationToken cancellationToken) =>
        ChangeState(() => service.ResumeAsync(id, cancellationToken));

    [HttpPost("{id:guid}/cancel")]
    public Task<ActionResult<JobResponse>> Cancel(Guid id, CancellationToken cancellationToken) =>
        ChangeState(() => service.CancelAsync(id, cancellationToken));

    [HttpPost("{id:guid}/trigger")]
    public async Task<ActionResult<ExecutionResponse>> Trigger(Guid id, CancellationToken cancellationToken)
    {
        try
        {
            var execution = await service.TriggerNowAsync(id, GetIdempotencyKey(), cancellationToken);
            return execution is null ? NotFound() : Accepted($"/jobs/{id}/executions/{execution.Id}", ToResponse(execution));
        }
        catch (ArgumentException exception)
        {
            return BadRequest(exception.Message);
        }
        catch (InvalidOperationException exception)
        {
            return Conflict(exception.Message);
        }
    }

    [HttpGet("{id:guid}/executions/{executionId:guid}")]
    public async Task<ActionResult<ExecutionResponse>> GetExecution(Guid id, Guid executionId, CancellationToken cancellationToken)
    {
        var execution = await service.GetExecutionAsync(executionId, cancellationToken);
        return execution is null || execution.JobId != id ? NotFound() : Ok(ToResponse(execution));
    }

    private async Task<ActionResult<JobResponse>> ChangeState(Func<Task<JobDefinition?>> operation)
    {
        try
        {
            var job = await operation();
            return job is null ? NotFound() : Ok(ToResponse(job));
        }
        catch (InvalidOperationException exception)
        {
            return Conflict(exception.Message);
        }
    }

    private string GetIdempotencyKey() => Request.Headers["Idempotency-Key"].ToString();

    private static JobResponse ToResponse(JobDefinition job) =>
        new(job.Id, job.Name, job.CronExpression, job.Status.ToString(), job.TenantId, job.ConcurrencyGroup, job.MaxConcurrentExecutions);

    private static ExecutionResponse ToResponse(JobExecution execution) =>
        new(execution.Id, execution.JobId, execution.Status.ToString(), execution.Attempt, execution.StartedAt, execution.CompletedAt, execution.FailureReason);
}
