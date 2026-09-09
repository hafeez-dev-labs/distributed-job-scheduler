using Microsoft.Extensions.Hosting;

namespace DistributedJobScheduler.Scheduler;

public sealed class SchedulerWorker(ILogger<SchedulerWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Scheduler host started.");
        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
    }
}
