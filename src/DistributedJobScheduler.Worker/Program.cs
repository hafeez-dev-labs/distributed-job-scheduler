using DistributedJobScheduler.Application;
using DistributedJobScheduler.Infrastructure;
using DistributedJobScheduler.Worker;

var builder = Host.CreateApplicationBuilder(args);
var databasePath = builder.Configuration["Worker:DatabasePath"] ?? "scheduler.db";
var connectionString = $"Data Source={databasePath}";

builder.Services.AddOpenTelemetry().ConfigureResource(r => r.AddService(JobSchedulerTelemetry.ServiceName + ".worker")).WithTracing(t => t.AddSource(JobSchedulerTelemetry.ActivitySourceName)).WithMetrics(m => m.AddMeter(JobSchedulerTelemetry.MeterName)).UseOtlpExporter();
builder.Services.AddSingleton<IJobRepository>(_ => new SqliteJobRepository(connectionString));
builder.Services.AddSingleton<IJobQueue>(_ => new SqliteJobQueue(connectionString));
builder.Services.AddSingleton<WorkerRegistry>();
builder.Services.AddSingleton<WorkerService>();
builder.Services.AddHostedService<WorkerHost>();
await builder.Build().RunAsync();