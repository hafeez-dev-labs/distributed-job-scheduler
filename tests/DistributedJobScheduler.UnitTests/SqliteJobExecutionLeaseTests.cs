using DistributedJobScheduler.Domain;
using DistributedJobScheduler.Infrastructure;
using Xunit;

namespace DistributedJobScheduler.UnitTests;

public sealed class SqliteJobExecutionLeaseTests
{
    [Fact]
    public async Task ExecutionLeasePreventsDuplicateOwnersAndRecoversAfterExpiry()
    {
        var path = Path.Combine(Path.GetTempPath(), $"scheduler-{Guid.NewGuid():N}.db");
        try
        {
            var repository = new SqliteJobRepository($"Data Source={path}");
            var job = new JobDefinition(
                Guid.NewGuid(),
                "Report",
                "0 * * * *",
                JobPriority.Normal,
                new RetryPolicy(),
                JobStatus.Active,
                DateTimeOffset.UtcNow);
            await repository.CreateAsync(job, "job-create");

            var execution = new JobExecution(Guid.NewGuid(), job.Id, JobExecutionStatus.Pending);
            await repository.CreateExecutionAsync(execution, "execution-create");

            var now = new DateTimeOffset(2026, 9, 25, 10, 0, 0, TimeSpan.Zero);

            Assert.True(await repository.TryAcquireExecutionLeaseAsync(execution.Id, "worker-a", now, TimeSpan.FromSeconds(30)));
            Assert.False(await repository.TryAcquireExecutionLeaseAsync(execution.Id, "worker-b", now.AddSeconds(1), TimeSpan.FromSeconds(30)));

            var running = execution with
            {
                Status = JobExecutionStatus.Running,
                Attempt = 1,
                StartedAt = now
            };

            Assert.False(await repository.TryUpdateExecutionAsync(running, "worker-b", now.AddSeconds(1)));
            Assert.True(await repository.TryUpdateExecutionAsync(running, "worker-a", now.AddSeconds(1)));
            Assert.True(await repository.RenewExecutionLeaseAsync(execution.Id, "worker-a", now.AddSeconds(10), TimeSpan.FromSeconds(30)));

            Assert.False(await repository.TryAcquireExecutionLeaseAsync(execution.Id, "worker-b", now.AddSeconds(20), TimeSpan.FromSeconds(30)));
            Assert.True(await repository.TryAcquireExecutionLeaseAsync(execution.Id, "worker-b", now.AddSeconds(41), TimeSpan.FromSeconds(30)));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }
}
