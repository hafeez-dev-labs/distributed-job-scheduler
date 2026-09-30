using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace DistributedJobScheduler.Application;

public static class JobSchedulerTelemetry
{
    public const string ServiceName = "distributed-job-scheduler";
    public const string ActivitySourceName = "DistributedJobScheduler";
    public const string MeterName = "DistributedJobScheduler";
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
    public static readonly Meter Meter = new(MeterName);
    public static readonly Counter<long> ScheduledJobs = Meter.CreateCounter<long>("scheduler.jobs.scheduled");
    public static readonly Counter<long> ExecutionFailures = Meter.CreateCounter<long>("scheduler.executions.failures");
    public static readonly Counter<long> ExecutionRetries = Meter.CreateCounter<long>("scheduler.executions.retries");
    public static readonly Counter<long> DeadLetteredExecutions = Meter.CreateCounter<long>("scheduler.executions.dead_lettered");
    public static readonly Histogram<double> ExecutionDuration = Meter.CreateHistogram<double>("scheduler.execution.duration", "s");
    public static Activity? StartActivity(string name) => ActivitySource.StartActivity(name, ActivityKind.Internal);
}
