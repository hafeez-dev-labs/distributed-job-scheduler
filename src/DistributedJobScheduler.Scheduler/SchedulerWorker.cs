using DistributedJobScheduler.Application;
using Microsoft.Extensions.Hosting;

namespace DistributedJobScheduler.Scheduler;

public sealed class SchedulerWorker(
    SchedulerService scheduler,
    ILogger<SchedulerWorker> logger) : BackgroundService
{
    private readonly string schedulerId = $"scheduler-{Environment.MachineName}-{Guid.NewGuid():N}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Scheduler host started with instance {SchedulerId}.", schedulerId);

        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTimeOffset.UtcNow;
            var scheduled = await scheduler.EvaluateDueJobsAsync(now, schedulerId, stoppingToken);
            if (scheduled > 0)
                logger.LogInformation("Advanced {ScheduledCount} due job schedule(s) at {EvaluatedAt}.", scheduled, now);

            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }
}
