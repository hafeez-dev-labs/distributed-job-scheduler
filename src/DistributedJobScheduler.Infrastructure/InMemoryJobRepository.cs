using System.Collections.Concurrent;
using DistributedJobScheduler.Application;
using DistributedJobScheduler.Domain;

namespace DistributedJobScheduler.Infrastructure;

public sealed class InMemoryJobRepository : IJobRepository
{
    private readonly ConcurrentDictionary<Guid, JobDefinition> jobs = new();

    public Task<JobDefinition?> GetAsync(Guid id, CancellationToken cancellationToken = default) =>
        Task.FromResult(jobs.TryGetValue(id, out var job) ? job : null);

    public Task SaveAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        jobs[job.Id] = job;
        return Task.CompletedTask;
    }
}
