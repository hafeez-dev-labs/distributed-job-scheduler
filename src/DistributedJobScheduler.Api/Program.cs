using System.Diagnostics;
using DistributedJobScheduler.Application;
using DistributedJobScheduler.Infrastructure;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllers();
builder.Services.AddHealthChecks();
builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService(JobSchedulerTelemetry.ServiceName + ".api"))
    .WithTracing(tracing => tracing.AddSource(JobSchedulerTelemetry.ActivitySourceName).AddAspNetCoreInstrumentation(options => options.RecordException = true))
    .WithMetrics(metrics => metrics.AddMeter(JobSchedulerTelemetry.MeterName).AddAspNetCoreInstrumentation())
    .UseOtlpExporter();
builder.Services.AddSingleton<IJobRepository>(_ => new SqliteJobRepository(builder.Configuration.GetConnectionString("Scheduler") ?? "Data Source=distributed-job-scheduler.db"));
builder.Services.AddScoped<IJobManagementService, JobManagementService>();
var app = builder.Build();
app.Use(async (context, next) =>
{
    var correlationId = context.Request.Headers["X-Correlation-Id"].FirstOrDefault();
    if (string.IsNullOrWhiteSpace(correlationId)) correlationId = Guid.NewGuid().ToString("N");
    context.Response.Headers["X-Correlation-Id"] = correlationId;
    Activity.Current?.SetTag("correlation.id", correlationId);
    using var scope = context.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger("DistributedJobScheduler.Request")
        .BeginScope(new Dictionary<string, object?> { ["CorrelationId"] = correlationId });
    await next();
});
app.MapControllers();
app.MapHealthChecks("/health");
app.Run();
public partial class Program;
