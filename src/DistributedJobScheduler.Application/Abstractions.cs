using DistributedJobScheduler.Domain;

namespace DistributedJobScheduler.Application;

public interface IJobRepository
{
    Task<JobDefinition?> GetAsync(Guid id, CancellationToken cancellationToken = default);
    Task<JobDefinition> CreateAsync(JobDefinition job, string idempotencyKey, CancellationToken cancellationToken = default);
    Task SaveAsync(JobDefinition job, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<JobDefinition>> GetDueJobsAsync(DateTimeOffset now, CancellationToken cancellationToken = default);
    Task<bool> TryAcquireSchedulerLeaseAsync(Guid jobId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default);
    Task<bool> RenewSchedulerLeaseAsync(Guid jobId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default);
    Task<JobExecution?> GetExecutionAsync(Guid id, CancellationToken cancellationToken = default);
    Task<bool> TryAcquireExecutionLeaseAsync(Guid executionId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default);
    Task<bool> TryAcquireExecutionLeaseAsync(Guid executionId, string owner, DateTimeOffset now, TimeSpan duration, int? maxConcurrentExecutions, string? tenantId, string? concurrencyGroup, CancellationToken cancellationToken = default);
    Task<bool> RenewExecutionLeaseAsync(Guid executionId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default);
    Task<bool> TryUpdateExecutionAsync(JobExecution execution, string owner, DateTimeOffset now, CancellationToken cancellationToken = default);
    Task<bool> ReleaseExecutionLeaseAsync(Guid executionId, string owner, CancellationToken cancellationToken = default);
    Task<JobExecution> CreateExecutionAsync(JobExecution execution, string idempotencyKey, CancellationToken cancellationToken = default);
    Task UpdateExecutionAsync(JobExecution execution, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<JobDependency>> GetDependenciesAsync(Guid jobId, CancellationToken cancellationToken = default);
    Task AddDependencyAsync(Guid jobId, Guid dependsOnJobId, CancellationToken cancellationToken = default);
    Task<bool> AreDependenciesSatisfiedAsync(Guid jobId, CancellationToken cancellationToken = default);
    Task<bool> HasFailedDependencyAsync(Guid jobId, CancellationToken cancellationToken = default);
}

public interface IJobScheduler { Task ScheduleAsync(JobDefinition job, CancellationToken cancellationToken = default); }
public interface IJobDispatcher { Task DispatchAsync(JobExecution execution, CancellationToken cancellationToken = default); }
public interface IJobExecutor { Task ExecuteAsync(JobExecution execution, CancellationToken cancellationToken = default); }

public interface IJobQueue
{
    Task EnqueueAsync(JobExecution execution, JobPriority priority, DateTimeOffset availableAt, CancellationToken cancellationToken = default);
    Task<QueueMessage?> TryDequeueAsync(string consumer, DateTimeOffset now, TimeSpan visibilityTimeout, CancellationToken cancellationToken = default);
    Task<bool> AcknowledgeAsync(Guid messageId, string consumer, CancellationToken cancellationToken = default);
    Task<bool> RequeueAsync(Guid messageId, string consumer, CancellationToken cancellationToken = default);
    Task<bool> RequeueAsync(Guid messageId, string consumer, DateTimeOffset availableAt, CancellationToken cancellationToken = default);
    Task<bool> DeadLetterAsync(Guid messageId, string consumer, JobExecution execution, string reason, CancellationToken cancellationToken = default);
    Task<int> ReplayDeadLettersAsync(CancellationToken cancellationToken = default);
}
