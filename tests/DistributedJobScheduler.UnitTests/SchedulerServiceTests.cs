using DistributedJobScheduler.Application;
using DistributedJobScheduler.Domain;
using DistributedJobScheduler.Infrastructure;
using Xunit;

namespace DistributedJobScheduler.UnitTests;

public sealed class SchedulerServiceTests
{
    [Fact]
    public void CalculateNextExecutionUsesUtcAndMovesForward()
    {
        var now = new DateTimeOffset(2026, 9, 14, 10, 30, 0, TimeSpan.Zero);

        var next = SchedulerService.CalculateNextExecution("0 * * * *", now);

        Assert.Equal(new DateTimeOffset(2026, 9, 14, 11, 0, 0, TimeSpan.Zero), next);
    }

    [Fact]
    public async Task EvaluateDueJobsAdvancesScheduleToFutureOccurrence()
    {
        var repository = new InMemoryJobRepository();
        var service = new SchedulerService(repository);
        var now = new DateTimeOffset(2026, 9, 14, 10, 30, 0, TimeSpan.Zero);
        var job = new JobDefinition(
            Guid.NewGuid(),
            "Report",
            "0 * * * *",
            JobPriority.Normal,
            new RetryPolicy(),
            JobStatus.Active,
            now.AddMinutes(-30));
        await repository.CreateAsync(job, "create-1");

        var scheduled = await service.EvaluateDueJobsAsync(now, "scheduler-a");
        var updated = await repository.GetAsync(job.Id);

        Assert.Equal(1, scheduled);
        Assert.Equal(now.AddMinutes(30), updated!.NextExecutionAt);
    }

    [Fact]
    public async Task SchedulerLeasePreventsConcurrentOwnersAndAllowsExpiredLeaseRecovery()
    {
        var repository = new InMemoryJobRepository();
        var jobId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;

        Assert.True(await repository.TryAcquireSchedulerLeaseAsync(jobId, "scheduler-a", now, TimeSpan.FromSeconds(30)));
        Assert.False(await repository.TryAcquireSchedulerLeaseAsync(jobId, "scheduler-b", now.AddSeconds(1), TimeSpan.FromSeconds(30)));
        Assert.True(await repository.TryAcquireSchedulerLeaseAsync(jobId, "scheduler-b", now.AddSeconds(31), TimeSpan.FromSeconds(30)));
    }
}
