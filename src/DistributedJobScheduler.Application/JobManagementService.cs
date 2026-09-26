using Cronos;
using DistributedJobScheduler.Contracts;
using DistributedJobScheduler.Domain;

namespace DistributedJobScheduler.Application;

public sealed class JobManagementService(IJobRepository repository) : IJobManagementService
{
    public async Task<JobDefinition> CreateAsync(CreateJobRequest request, string idempotencyKey, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(idempotencyKey))
            throw new ArgumentException("Idempotency-Key header is required.");
        if (string.IsNullOrWhiteSpace(request.Name))
            throw new ArgumentException("Job name is required.");
        if (request.Retries < 0)
            throw new ArgumentException("Retries cannot be negative.");
        if (request.MaxConcurrentExecutions is <= 0)
            throw new ArgumentException("Max concurrent executions must be greater than zero.");

        var tenantId = request.TenantId?.Trim();
        var concurrencyGroup = request.ConcurrencyGroup?.Trim();
        if (!string.IsNullOrWhiteSpace(concurrencyGroup) && string.IsNullOrWhiteSpace(tenantId))
            throw new ArgumentException("A concurrency group requires a tenant.");

        if (!string.IsNullOrWhiteSpace(request.Schedule))
        {
            try
            {
                CronExpression.Parse(request.Schedule.Trim());
            }
            catch (CronFormatException)
            {
                throw new ArgumentException("Invalid cron expression.");
            }
        }

        var job = new JobDefinition(
            Guid.NewGuid(),
            request.Name.Trim(),
            string.IsNullOrWhiteSpace(request.Schedule) ? null : request.Schedule.Trim(),
            JobPriority.Normal,
            new RetryPolicy(request.Retries),
            JobStatus.Active,
            null,
            string.IsNullOrWhiteSpace(tenantId) ? null : tenantId,
            string.IsNullOrWhiteSpace(concurrencyGroup) ? null : concurrencyGroup,
            request.MaxConcurrentExecutions);

        return await repository.CreateAsync(job, idempotencyKey.Trim(), cancellationToken);
    }

    public Task<JobDefinition?> GetAsync(Guid id, CancellationToken cancellationToken = default) =>
        repository.GetAsync(id, cancellationToken);

    public Task<IReadOnlyList<JobDependency>> GetDependenciesAsync(Guid id, CancellationToken cancellationToken = default) =>
        repository.GetDependenciesAsync(id, cancellationToken);

    public Task AddDependencyAsync(Guid id, Guid dependsOnJobId, CancellationToken cancellationToken = default) =>
        repository.AddDependencyAsync(id, dependsOnJobId, cancellationToken);

    public async Task<JobDefinition?> PauseAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var job = await repository.GetAsync(id, cancellationToken);
        if (job is null)
            return null;
        if (job.Status == JobStatus.Cancelled)
            throw new InvalidOperationException("Cancelled jobs cannot be paused.");
        if (job.Status == JobStatus.Paused)
            return job;
        if (job.Status != JobStatus.Active)
            throw new InvalidOperationException("Only active jobs can be paused.");

        var updated = job with { Status = JobStatus.Paused };
        await repository.SaveAsync(updated, cancellationToken);
        return updated;
    }

    public async Task<JobDefinition?> ResumeAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var job = await repository.GetAsync(id, cancellationToken);
        if (job is null)
            return null;
        if (job.Status == JobStatus.Cancelled)
            throw new InvalidOperationException("Cancelled jobs cannot be resumed.");
        if (job.Status == JobStatus.Active)
            return job;
        if (job.Status != JobStatus.Paused)
            throw new InvalidOperationException("Only paused jobs can be resumed.");

        var updated = job with { Status = JobStatus.Active };
        await repository.SaveAsync(updated, cancellationToken);
        return updated;
    }

    public async Task<JobDefinition?> CancelAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var job = await repository.GetAsync(id, cancellationToken);
        if (job is null)
            return null;
        if (job.Status == JobStatus.Cancelled)
            return job;

        var updated = job with { Status = JobStatus.Cancelled };
        await repository.SaveAsync(updated, cancellationToken);
        return updated;
    }

    public async Task<JobExecution?> TriggerNowAsync(Guid id, string idempotencyKey, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(idempotencyKey))
            throw new ArgumentException("Idempotency-Key header is required.");

        var job = await repository.GetAsync(id, cancellationToken);
        if (job is null)
            return null;
        if (job.Status == JobStatus.Cancelled)
            throw new InvalidOperationException("Cancelled jobs cannot be triggered.");

        var execution = new JobExecution(Guid.NewGuid(), job.Id, JobExecutionStatus.Pending);
        return await repository.CreateExecutionAsync(execution, idempotencyKey.Trim(), cancellationToken);
    }

    public Task<JobExecution?> GetExecutionAsync(Guid id, CancellationToken cancellationToken = default) =>
        repository.GetExecutionAsync(id, cancellationToken);
}

public interface IJobManagementService
{
    Task<JobDefinition> CreateAsync(CreateJobRequest request, string idempotencyKey, CancellationToken cancellationToken = default);
    Task<JobDefinition?> GetAsync(Guid id, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<JobDependency>> GetDependenciesAsync(Guid id, CancellationToken cancellationToken = default);
    Task AddDependencyAsync(Guid id, Guid dependsOnJobId, CancellationToken cancellationToken = default);
    Task<JobDefinition?> PauseAsync(Guid id, CancellationToken cancellationToken = default);
    Task<JobDefinition?> ResumeAsync(Guid id, CancellationToken cancellationToken = default);
    Task<JobDefinition?> CancelAsync(Guid id, CancellationToken cancellationToken = default);
    Task<JobExecution?> TriggerNowAsync(Guid id, string idempotencyKey, CancellationToken cancellationToken = default);
    Task<JobExecution?> GetExecutionAsync(Guid id, CancellationToken cancellationToken = default);
}
