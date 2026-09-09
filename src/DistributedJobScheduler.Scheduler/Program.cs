using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<DistributedJobScheduler.Scheduler.SchedulerWorker>();
await builder.Build().RunAsync();
