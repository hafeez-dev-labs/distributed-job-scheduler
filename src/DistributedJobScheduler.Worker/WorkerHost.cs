using Microsoft.Extensions.Hosting;

namespace DistributedJobScheduler.Worker;

public sealed class WorkerHost(WorkerService service):BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)=>service.RunAsync(stoppingToken);
}