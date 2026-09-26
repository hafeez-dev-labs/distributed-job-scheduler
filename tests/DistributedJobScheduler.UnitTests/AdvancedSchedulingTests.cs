using DistributedJobScheduler.Domain;
using DistributedJobScheduler.Infrastructure;
using Xunit;

namespace DistributedJobScheduler.UnitTests;

public sealed class SqliteAdvancedSchedulingTests
{
    [Fact]
    public async Task DependencyGraphRejectsCycles()
    {
        var path = Path.Combine(Path.GetTempPath(), $"scheduler-{Guid.NewGuid():N}.db");
        try
        {
            var repository = new SqliteJobRepository($"Data Source={path}");
            var first = CreateJob("First");
            var second = CreateJob("Second");
            var third = CreateJob("Third");

            await repository.CreateAsync(first, "first");
            await repository.CreateAsync(second, "second");
            await repository.CreateAsync(third, "third");

            await repository.AddDependencyAsync(second.Id, first.Id);
            await repository.AddDependencyAsync(third.Id, second.Id);

            var dependencies = await repository.GetDependenciesAsync(third.Id);
            Assert.Single(dependencies);
            Assert.Equal(second.Id, dependencies[0].DependsOnJobId);
            await Assert.ThrowsAsync<InvalidOperationException>(() => repository.AddDependencyAsync(first.Id, third.Id));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task TenantConcurrencyLimitSpansJobs()
    {
        var path = Path.Combine(Path.GetTempPath(), $"scheduler-{Guid.NewGuid():N}.db");
        try
        {
            var repository = new SqliteJobRepository($"Data Source={path}");
            var firstJob = new JobDefinition(Guid.NewGuid(), "First", null, JobPriority.Normal, new RetryPolicy(), JobStatus.Active, null, "tenant-1", null, 1);
            var secondJob = new JobDefinition(Guid.NewGuid(), "Second", null, JobPriority.Normal, new RetryPolicy(), JobStatus.Active, null, "tenant-1", null, 1);

            await repository.CreateAsync(firstJob, "first");
            await repository.CreateAsync(secondJob, "second");

            var first = new JobExecution(Guid.NewGuid(), firstJob.Id, JobExecutionStatus.Pending);
            var second = new JobExecution(Guid.NewGuid(), secondJob.Id, JobExecutionStatus.Pending);
            await repository.CreateExecutionAsync(first, "exec-first");
            await repository.CreateExecutionAsync(second, "exec-second");

            var now = DateTimeOffset.UtcNow;
            Assert.True(await repository.TryAcquireExecutionLeaseAsync(first.Id, "worker-1", now, TimeSpan.FromMinutes(1), 1, "tenant-1", null));
            Assert.False(await repository.TryAcquireExecutionLeaseAsync(second.Id, "worker-2", now, TimeSpan.FromMinutes(1), 1, "tenant-1", null));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    private static JobDefinition CreateJob(string name) =>
        new(Guid.NewGuid(), name, null, JobPriority.Normal, new RetryPolicy(), JobStatus.Active);
}
