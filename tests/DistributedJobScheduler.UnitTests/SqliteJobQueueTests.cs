using DistributedJobScheduler.Domain;
using DistributedJobScheduler.Infrastructure;
using Xunit;

namespace DistributedJobScheduler.UnitTests;

public sealed class SqliteJobQueueTests
{
    [Fact]
    public async Task DequeueHonorsPriorityAndAcknowledgementIsIdempotent()
    {
        var path = Path.Combine(Path.GetTempPath(), $"scheduler-{Guid.NewGuid():N}.db");
        try
        {
            var queue = new SqliteJobQueue($"Data Source={path}");
            var now = DateTimeOffset.UtcNow;
            var low = new JobExecution(Guid.NewGuid(), Guid.NewGuid(), JobExecutionStatus.Pending);
            var high = new JobExecution(Guid.NewGuid(), Guid.NewGuid(), JobExecutionStatus.Pending);

            await queue.EnqueueAsync(low, JobPriority.Low, now);
            await queue.EnqueueAsync(high, JobPriority.High, now);

            var message = await queue.TryDequeueAsync("worker-1", now, TimeSpan.FromMinutes(1));

            Assert.NotNull(message);
            Assert.Equal(high.Id, message!.ExecutionId);
            Assert.True(await queue.AcknowledgeAsync(message.Id, "worker-1"));
            Assert.True(await queue.AcknowledgeAsync(message.Id, "worker-1"));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task ExpiredLeaseMakesMessageVisibleAgain()
    {
        var path = Path.Combine(Path.GetTempPath(), $"scheduler-{Guid.NewGuid():N}.db");
        try
        {
            var queue = new SqliteJobQueue($"Data Source={path}");
            var now = DateTimeOffset.UtcNow;
            var execution = new JobExecution(Guid.NewGuid(), Guid.NewGuid(), JobExecutionStatus.Pending);
            await queue.EnqueueAsync(execution, JobPriority.Normal, now);

            var first = await queue.TryDequeueAsync("worker-1", now, TimeSpan.FromSeconds(5));
            var hidden = await queue.TryDequeueAsync("worker-2", now.AddSeconds(1), TimeSpan.FromSeconds(5));
            var reclaimed = await queue.TryDequeueAsync("worker-2", now.AddSeconds(6), TimeSpan.FromSeconds(5));

            Assert.NotNull(first);
            Assert.Null(hidden);
            Assert.NotNull(reclaimed);
            Assert.Equal(first!.Id, reclaimed!.Id);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
}
