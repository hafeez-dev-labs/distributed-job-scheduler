using DistributedJobScheduler.Application;
using DistributedJobScheduler.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
var databasePath = builder.Configuration["Scheduler:DatabasePath"] ?? "scheduler.db";
var connectionString = $"Data Source={databasePath}";

builder.Services.AddSingleton<IJobRepository>(_ => new SqliteJobRepository(connectionString));
builder.Services.AddSingleton<IJobQueue>(_ => new SqliteJobQueue(connectionString));
builder.Services.AddSingleton<SchedulerService>();
builder.Services.AddHostedService<DistributedJobScheduler.Scheduler.SchedulerWorker>();

await builder.Build().RunAsync();
