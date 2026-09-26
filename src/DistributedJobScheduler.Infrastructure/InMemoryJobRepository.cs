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
    private readonly ConcurrentDictionary<Guid, (string Owner, DateTimeOffset LeaseUntil)> schedulerLeases = new();
    private readonly ConcurrentDictionary<Guid, ConcurrentDictionary<Guid, byte>> dependencies = new();
    private readonly object executionGate = new();
    private readonly object dependencyGate = new();

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

    public Task<IReadOnlyList<JobDefinition>> GetDueJobsAsync(DateTimeOffset now, CancellationToken cancellationToken = default)
    {
        var due = jobs.Values
            .Where(job => job.Status == JobStatus.Active && !string.IsNullOrWhiteSpace(job.CronExpression) &&
                (job.NextExecutionAt is null || job.NextExecutionAt <= now))
            .OrderBy(job => job.NextExecutionAt ?? DateTimeOffset.MinValue)
            .ToList();
        return Task.FromResult<IReadOnlyList<JobDefinition>>(due);
    }

    public Task<bool> TryAcquireSchedulerLeaseAsync(Guid jobId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        while (true)
        {
            if (schedulerLeases.TryGetValue(jobId, out var current) && current.LeaseUntil > now && current.Owner != owner)
                return Task.FromResult(false);

            if (schedulerLeases.TryUpdate(jobId, (owner, now.Add(duration)), current) ||
                schedulerLeases.TryAdd(jobId, (owner, now.Add(duration))))
                return Task.FromResult(true);
        }
    }

    public Task<bool> RenewSchedulerLeaseAsync(Guid jobId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        if (!schedulerLeases.TryGetValue(jobId, out var current) || current.Owner != owner || current.LeaseUntil <= now)
            return Task.FromResult(false);
        return Task.FromResult(schedulerLeases.TryUpdate(jobId, (owner, now.Add(duration)), current));
    }

    public Task<JobExecution?> GetExecutionAsync(Guid id, CancellationToken cancellationToken = default) =>
        Task.FromResult(executions.TryGetValue(id, out var execution) ? execution : null);

    public Task<bool> TryAcquireExecutionLeaseAsync(Guid executionId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default) =>
        TryAcquireExecutionLeaseAsync(executionId, owner, now, duration, null, null, null, cancellationToken);

    public Task<bool> TryAcquireExecutionLeaseAsync(
        Guid executionId,
        string owner,
        DateTimeOffset now,
        TimeSpan duration,
        int? maxConcurrentExecutions,
        string? tenantId,
        string? concurrencyGroup,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(owner))
            throw new ArgumentException("Execution lease owner is required.", nameof(owner));

        lock (executionGate)
        {
            if (!executions.TryGetValue(executionId, out var execution))
                return Task.FromResult(false);
            if (execution.Status is JobExecutionStatus.Succeeded or JobExecutionStatus.Cancelled)
                return Task.FromResult(false);
            if (!string.IsNullOrWhiteSpace(execution.LeaseOwner) &&
                execution.LeaseUntil is { } activeLease &&
                activeLease > now &&
                execution.LeaseOwner != owner)
                return Task.FromResult(false);

            if (maxConcurrentExecutions is > 0)
            {
                var active = executions.Values.Count(item =>
                    item.Id != executionId &&
                    item.Status == JobExecutionStatus.Running &&
                    item.LeaseUntil is { } leaseUntil &&
                    leaseUntil > now &&
                    jobs.TryGetValue(item.JobId, out var job) &&
                    MatchesConcurrencyScope(item.JobId, execution.JobId, job, tenantId, concurrencyGroup));

                if (active >= maxConcurrentExecutions.Value)
                    return Task.FromResult(false);
            }

            executions[executionId] = execution with
            {
                LeaseOwner = owner,
                LeaseUntil = now.Add(duration)
            };
            return Task.FromResult(true);
        }
    }

    public Task<bool> RenewExecutionLeaseAsync(Guid executionId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        lock (executionGate)
        {
            if (!executions.TryGetValue(executionId, out var execution) ||
                execution.LeaseOwner != owner ||
                execution.LeaseUntil is null ||
                execution.LeaseUntil <= now)
                return Task.FromResult(false);

            executions[executionId] = execution with { LeaseUntil = now.Add(duration) };
            return Task.FromResult(true);
        }
    }

    public Task<bool> TryUpdateExecutionAsync(JobExecution execution, string owner, DateTimeOffset now, CancellationToken cancellationToken = default)
    {
        lock (executionGate)
        {
            if (!executions.TryGetValue(execution.Id, out var current) ||
                current.LeaseOwner != owner ||
                current.LeaseUntil is null ||
                current.LeaseUntil <= now)
                return Task.FromResult(false);

            executions[execution.Id] = execution with { LeaseOwner = owner, LeaseUntil = current.LeaseUntil };
            return Task.FromResult(true);
        }
    }

    public Task<bool> ReleaseExecutionLeaseAsync(Guid executionId, string owner, CancellationToken cancellationToken = default)
    {
        lock (executionGate)
        {
            if (!executions.TryGetValue(executionId, out var execution) || execution.LeaseOwner != owner)
                return Task.FromResult(false);

            executions[executionId] = execution with { LeaseOwner = null, LeaseUntil = null };
            return Task.FromResult(true);
        }
    }

    public Task<JobExecution> CreateExecutionAsync(JobExecution execution, string idempotencyKey, CancellationToken cancellationToken = default)
    {
        var existingId = executionIdempotency.GetOrAdd(idempotencyKey, execution.Id);
        if (existingId != execution.Id && executions.TryGetValue(existingId, out var existing))
            return Task.FromResult(existing);

        executions.TryAdd(execution.Id, execution);
        return Task.FromResult(executions[execution.Id]);
    }

    public Task UpdateExecutionAsync(JobExecution execution, CancellationToken cancellationToken = default)
    {
        executions[execution.Id] = execution;
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<JobDependency>> GetDependenciesAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        lock (dependencyGate)
        {
            var result = dependencies.TryGetValue(jobId, out var direct)
                ? direct.Keys.OrderBy(id => id).Select(id => new JobDependency(jobId, id)).ToArray()
                : Array.Empty<JobDependency>();
            return Task.FromResult<IReadOnlyList<JobDependency>>(result);
        }
    }

    public Task AddDependencyAsync(Guid jobId, Guid dependsOnJobId, CancellationToken cancellationToken = default)
    {
        lock (dependencyGate)
        {
            if (!jobs.ContainsKey(jobId) || !jobs.ContainsKey(dependsOnJobId))
                throw new InvalidOperationException("Both jobs must exist before a dependency can be created.");
            if (jobId == dependsOnJobId)
                throw new InvalidOperationException("A job cannot depend on itself.");
            if (HasPath(dependsOnJobId, jobId))
                throw new InvalidOperationException("The dependency would create a cycle.");

            var direct = dependencies.GetOrAdd(jobId, _ => new ConcurrentDictionary<Guid, byte>());
            direct.TryAdd(dependsOnJobId, 0);
            return Task.CompletedTask;
        }
    }

    public Task<bool> AreDependenciesSatisfiedAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        lock (dependencyGate)
        {
            if (!dependencies.TryGetValue(jobId, out var direct) || direct.IsEmpty)
                return Task.FromResult(true);

            foreach (var dependencyId in direct.Keys)
            {
                if (!executions.Values.Any(execution =>
                    execution.JobId == dependencyId &&
                    execution.Status == JobExecutionStatus.Succeeded))
                    return Task.FromResult(false);
            }

            return Task.FromResult(true);
        }
    }

    public Task<bool> HasFailedDependencyAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        lock (dependencyGate)
        {
            if (!dependencies.TryGetValue(jobId, out var direct) || direct.IsEmpty)
                return Task.FromResult(false);

            foreach (var dependencyId in direct.Keys)
            {
                var dependencyExecutions = executions.Values.Where(execution => execution.JobId == dependencyId).ToArray();
                if (dependencyExecutions.Any(execution => execution.Status == JobExecutionStatus.Succeeded))
                    continue;
                if (dependencyExecutions.Any(execution => execution.Status is JobExecutionStatus.Failed or JobExecutionStatus.DeadLettered or JobExecutionStatus.Cancelled))
                    return Task.FromResult(true);
            }

            return Task.FromResult(false);
        }
    }

    private bool HasPath(Guid start, Guid target)
    {
        var visited = new HashSet<Guid>();
        var stack = new Stack<Guid>();
        stack.Push(start);

        while (stack.Count > 0)
        {
            var current = stack.Pop();
            if (!visited.Add(current))
                continue;
            if (current == target)
                return true;
            if (dependencies.TryGetValue(current, out var next))
            {
                foreach (var dependency in next.Keys)
                    stack.Push(dependency);
            }
        }

        return false;
    }

    private static bool MatchesConcurrencyScope(
        Guid activeJobId,
        Guid targetJobId,
        JobDefinition activeJob,
        string? tenantId,
        string? concurrencyGroup) =>
        !string.IsNullOrWhiteSpace(concurrencyGroup)
            ? string.Equals(activeJob.TenantId, tenantId, StringComparison.Ordinal) &&
              string.Equals(activeJob.ConcurrencyGroup, concurrencyGroup, StringComparison.Ordinal)
            : !string.IsNullOrWhiteSpace(tenantId)
                ? string.Equals(activeJob.TenantId, tenantId, StringComparison.Ordinal)
                : activeJobId == targetJobId;
}
