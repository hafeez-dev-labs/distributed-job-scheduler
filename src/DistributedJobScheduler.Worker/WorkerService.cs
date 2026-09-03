using Microsoft.Extensions.Hosting;

namespace DistributedJobScheduler.Worker;

public sealed class WorkerService(ILogger<WorkerService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Worker host started.");
        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
    }
}
