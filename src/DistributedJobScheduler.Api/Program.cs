using System.Diagnostics;
using System.Globalization;
using System.Threading.RateLimiting;
using DistributedJobScheduler.Application;
using DistributedJobScheduler.Infrastructure;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllers();
builder.Services.AddHealthChecks();

var permitLimit = builder.Configuration.GetValue("RateLimiting:PermitLimit", 60);
var windowSeconds = builder.Configuration.GetValue("RateLimiting:WindowSeconds", 60);
if (permitLimit < 1 || windowSeconds < 1)
    throw new InvalidOperationException("Rate limit settings must be positive.");

builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
    options.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(context =>
        RateLimitPartition.GetFixedWindowLimiter(
            context.Connection.RemoteIpAddress?.ToString() ?? "unknown",
            _ => new FixedWindowRateLimiterOptions
            {
                PermitLimit = permitLimit,
                Window = TimeSpan.FromSeconds(windowSeconds),
                QueueLimit = 0,
                AutoReplenishment = true
            }));
    options.OnRejected = (context, _) =>
    {
        context.HttpContext.Response.Headers.RetryAfter = windowSeconds.ToString(CultureInfo.InvariantCulture);
        return ValueTask.CompletedTask;
    };
});

builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService(JobSchedulerTelemetry.ServiceName + ".api"))
    .WithTracing(tracing => tracing.AddSource(JobSchedulerTelemetry.ActivitySourceName).AddAspNetCoreInstrumentation(options => options.RecordException = true))
    .WithMetrics(metrics => metrics.AddMeter(JobSchedulerTelemetry.MeterName).AddAspNetCoreInstrumentation())
    .UseOtlpExporter();
builder.Services.AddSingleton<IJobRepository>(_ => new SqliteJobRepository(builder.Configuration.GetConnectionString("Scheduler") ?? "Data Source=distributed-job-scheduler.db"));
builder.Services.AddScoped<IJobManagementService, JobManagementService>();
builder.Services.AddSingleton<DashboardService>(_ => new DashboardService(builder.Configuration.GetConnectionString("Scheduler") ?? "Data Source=distributed-job-scheduler.db"));
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
app.UseRateLimiter();
app.MapControllers();
app.MapHealthChecks("/health");
app.MapGet("/dashboard", () => Results.Content(DashboardPage.Html, "text/html"));
app.MapGet("/dashboard/data", async (DashboardService dashboardService, string? search, string? status, CancellationToken cancellationToken) =>
    Results.Ok(await dashboardService.GetSnapshotAsync(search, status, cancellationToken)));
app.Run();
public partial class Program;
