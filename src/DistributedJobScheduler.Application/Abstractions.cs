using DistributedJobScheduler.Domain;

namespace DistributedJobScheduler.Application;

public interface IJobRepository
{
    Task<JobDefinition?> GetAsync(Guid id, CancellationToken cancellationToken = default);
    Task<JobDefinition> CreateAsync(JobDefinition job, string idempotencyKey, CancellationToken cancellationToken = default);
    Task SaveAsync(JobDefinition job, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<JobDefinition>> GetDueJobsAsync(DateTimeOffset now, CancellationToken cancellationToken = default);
    Task<bool> TryAcquireSchedulerLeaseAsync(Guid jobId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default);
    Task<JobExecution?> GetExecutionAsync(Guid id, CancellationToken cancellationToken = default);
    Task<JobExecution> CreateExecutionAsync(JobExecution execution, string idempotencyKey, CancellationToken cancellationToken = default);
}

public interface IJobScheduler
{
    Task ScheduleAsync(JobDefinition job, CancellationToken cancellationToken = default);
}

public interface IJobDispatcher
{
    Task DispatchAsync(JobExecution execution, CancellationToken cancellationToken = default);
}

public interface IJobExecutor
{
    Task ExecuteAsync(JobExecution execution, CancellationToken cancellationToken = default);
}

public interface IJobQueue
{
    Task EnqueueAsync(JobExecution execution, JobPriority priority, DateTimeOffset availableAt, CancellationToken cancellationToken = default);
    Task<QueueMessage?> TryDequeueAsync(string consumer, DateTimeOffset now, TimeSpan visibilityTimeout, CancellationToken cancellationToken = default);
    Task<bool> AcknowledgeAsync(Guid messageId, string consumer, CancellationToken cancellationToken = default);
    Task<bool> RequeueAsync(Guid messageId, string consumer, CancellationToken cancellationToken = default);
}
