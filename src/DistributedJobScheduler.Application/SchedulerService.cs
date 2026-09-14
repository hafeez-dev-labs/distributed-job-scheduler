using Cronos;
using DistributedJobScheduler.Domain;

namespace DistributedJobScheduler.Application;

public sealed class SchedulerService(IJobRepository repository)
{
    public async Task<int> EvaluateDueJobsAsync(DateTimeOffset now, string schedulerId, CancellationToken cancellationToken = default)
    {
        var dueJobs = await repository.GetDueJobsAsync(now, cancellationToken);
        var scheduled = 0;

        foreach (var job in dueJobs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!await repository.TryAcquireSchedulerLeaseAsync(job.Id, schedulerId, now, TimeSpan.FromSeconds(30), cancellationToken))
                continue;

            var nextExecutionAt = CalculateNextExecution(job.CronExpression, now);
            await repository.SaveAsync(job with { NextExecutionAt = nextExecutionAt }, cancellationToken);
            scheduled++;
        }

        return scheduled;
    }

    public static DateTimeOffset CalculateNextExecution(string? cronExpression, DateTimeOffset now)
    {
        if (string.IsNullOrWhiteSpace(cronExpression))
            throw new ArgumentException("A cron expression is required for scheduling.", nameof(cronExpression));

        var cron = CronExpression.Parse(cronExpression);
        var next = cron.GetNextOccurrence(now.UtcDateTime, false);
        if (next is null)
            throw new InvalidOperationException("The cron expression has no future occurrence.");

        return new DateTimeOffset(DateTime.SpecifyKind(next.Value, DateTimeKind.Utc));
    }
}
