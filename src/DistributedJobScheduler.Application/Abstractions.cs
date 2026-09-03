using DistributedJobScheduler.Domain;

namespace DistributedJobScheduler.Application;

public interface IJobRepository
{
    Task<JobDefinition?> GetAsync(Guid id, CancellationToken cancellationToken = default);
    Task SaveAsync(JobDefinition job, CancellationToken cancellationToken = default);
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
