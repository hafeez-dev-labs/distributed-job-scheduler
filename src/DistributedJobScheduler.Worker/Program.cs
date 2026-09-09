using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<DistributedJobScheduler.Worker.WorkerService>();
await builder.Build().RunAsync();
