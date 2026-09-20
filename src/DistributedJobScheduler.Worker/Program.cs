using DistributedJobScheduler.Application;
using DistributedJobScheduler.Infrastructure;
using DistributedJobScheduler.Worker;

var builder = Host.CreateApplicationBuilder(args);
var databasePath = builder.Configuration["Worker:DatabasePath"] ?? "scheduler.db";
var connectionString = $"Data Source={databasePath}";
builder.Services.AddSingleton<IJobRepository>(_ => new SqliteJobRepository(connectionString));
builder.Services.AddSingleton<IJobQueue>(_ => new SqliteJobQueue(connectionString));
builder.Services.AddSingleton<WorkerRegistry>();
builder.Services.AddSingleton<WorkerService>();
builder.Services.AddHostedService<WorkerHost>();
await builder.Build().RunAsync();