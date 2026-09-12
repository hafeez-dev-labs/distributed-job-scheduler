using System.Collections.Concurrent;
using DistributedJobScheduler.Application;
using DistributedJobScheduler.Domain;

namespace DistributedJobScheduler.Infrastructure;

public sealed class InMemoryJobRepository : IJobRepository
{
    private readonly ConcurrentDictionary<Guid, JobDefinition> jobs = new();
    private readonly ConcurrentDictionary<Guid, JobExecution> executions = new();
    private readonly ConcurrentDictionary<string, Guid> jobIdempotency = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Guid> executionIdempotency = new(StringComparer.Ordinal);

    public Task<JobDefinition?> GetAsync(Guid id, CancellationToken cancellationToken = default) =>
        Task.FromResult(jobs.TryGetValue(id, out var job) ? job : null);

    public Task<JobDefinition> CreateAsync(JobDefinition job, string idempotencyKey, CancellationToken cancellationToken = default)
    {
        var existingId = jobIdempotency.GetOrAdd(idempotencyKey, job.Id);
        if (existingId != job.Id && jobs.TryGetValue(existingId, out var existing))
            return Task.FromResult(existing);

        jobs.TryAdd(job.Id, job);
        return Task.FromResult(jobs[job.Id]);
    }

    public Task SaveAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        jobs[job.Id] = job;
        return Task.CompletedTask;
    }

    public Task<JobExecution?> GetExecutionAsync(Guid id, CancellationToken cancellationToken = default) =>
        Task.FromResult(executions.TryGetValue(id, out var execution) ? execution : null);

    public Task<JobExecution> CreateExecutionAsync(JobExecution execution, string idempotencyKey, CancellationToken cancellationToken = default)
    {
        var existingId = executionIdempotency.GetOrAdd(idempotencyKey, execution.Id);
        if (existingId != execution.Id && executions.TryGetValue(existingId, out var existing))
            return Task.FromResult(existing);

        executions.TryAdd(execution.Id, execution);
        return Task.FromResult(executions[execution.Id]);
    }
}
