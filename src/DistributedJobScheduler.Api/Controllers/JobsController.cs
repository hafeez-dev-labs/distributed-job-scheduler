using DistributedJobScheduler.Application;
using DistributedJobScheduler.Contracts;
using DistributedJobScheduler.Domain;
using Microsoft.AspNetCore.Mvc;

namespace DistributedJobScheduler.Api.Controllers;

[ApiController]
[Route("jobs")]
public sealed class JobsController(IJobRepository repository) : ControllerBase
{
    [HttpPost]
    public async Task<ActionResult<JobResponse>> Create(CreateJobRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.Name))
            return BadRequest("Job name is required.");

        var job = new JobDefinition(
            Guid.NewGuid(),
            request.Name,
            request.Schedule,
            JobPriority.Normal,
            new RetryPolicy(request.Retries),
            JobStatus.Active);

        await repository.SaveAsync(job, cancellationToken);
        return Created($"/jobs/{job.Id}", new JobResponse(job.Id, job.Name, job.CronExpression, job.Status.ToString()));
    }

    [HttpGet("{id:guid}")]
    public async Task<ActionResult<JobResponse>> Get(Guid id, CancellationToken cancellationToken)
    {
        var job = await repository.GetAsync(id, cancellationToken);
        return job is null
            ? NotFound()
            : Ok(new JobResponse(job.Id, job.Name, job.CronExpression, job.Status.ToString()));
    }
}
