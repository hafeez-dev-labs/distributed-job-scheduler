using DistributedJobScheduler.Application;
using DistributedJobScheduler.Contracts;
using DistributedJobScheduler.Infrastructure;
using DistributedJobScheduler.Domain;
using Xunit;

namespace DistributedJobScheduler.UnitTests;

public sealed class JobManagementServiceTests
{
    [Fact]
    public async Task CreatePersistsJobAndRejectsInvalidCron()
    {
        var repository = new InMemoryJobRepository();
        var service = new JobManagementService(repository);

        var job = await service.CreateAsync(new CreateJobRequest("Report", "0 0 * * *"), "create-1");

        Assert.Equal("Report", job.Name);
        Assert.Equal(JobStatus.Active, job.Status);
        Assert.NotNull(await repository.GetAsync(job.Id));
        await Assert.ThrowsAsync<ArgumentException>(() => service.CreateAsync(new CreateJobRequest("Broken", "not-a-cron"), "create-2"));
    }

    [Fact]
    public async Task CreateWithSameIdempotencyKeyReturnsOriginalJob()
    {
        var repository = new InMemoryJobRepository();
        var service = new JobManagementService(repository);

        var first = await service.CreateAsync(new CreateJobRequest("Report", "0 0 * * *"), "same-key");
        var second = await service.CreateAsync(new CreateJobRequest("Different", "0 1 * * *"), "same-key");

        Assert.Equal(first.Id, second.Id);
        Assert.Equal(first.Name, second.Name);
    }

    [Fact]
    public async Task PauseResumeAndCancelFollowLifecycleRules()
    {
        var repository = new InMemoryJobRepository();
        var service = new JobManagementService(repository);
        var job = await service.CreateAsync(new CreateJobRequest("Report", null), "create-1");

        var paused = await service.PauseAsync(job.Id);
        var resumed = await service.ResumeAsync(job.Id);
        var cancelled = await service.CancelAsync(job.Id);

        Assert.Equal(JobStatus.Paused, paused!.Status);
        Assert.Equal(JobStatus.Active, resumed!.Status);
        Assert.Equal(JobStatus.Cancelled, cancelled!.Status);
        await Assert.ThrowsAsync<InvalidOperationException>(() => service.TriggerNowAsync(job.Id, "trigger-1"));
    }

    [Fact]
    public async Task TriggerNowCreatesPendingExecutionAndIsIdempotent()
    {
        var repository = new InMemoryJobRepository();
        var service = new JobManagementService(repository);
        var job = await service.CreateAsync(new CreateJobRequest("Report", null), "create-1");

        var first = await service.TriggerNowAsync(job.Id, "trigger-key");
        var second = await service.TriggerNowAsync(job.Id, "trigger-key");

        Assert.NotNull(first);
        Assert.Equal(JobExecutionStatus.Pending, first!.Status);
        Assert.Equal(first.Id, second!.Id);
    }
}
