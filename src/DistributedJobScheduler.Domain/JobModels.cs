namespace DistributedJobScheduler.Domain;

public enum JobStatus { Draft, Active, Paused, Cancelled }
public enum JobExecutionStatus { Pending, Running, Succeeded, Failed, DeadLettered, Cancelled }
public enum JobPriority { Low, Normal, High, Critical }

public sealed record RetryPolicy(int MaxAttempts = 3, int InitialDelaySeconds = 5, double BackoffMultiplier = 2.0);

public sealed record JobDefinition(
    Guid Id,
    string Name,
    string? CronExpression,
    JobPriority Priority,
    RetryPolicy RetryPolicy,
    JobStatus Status = JobStatus.Draft);

public sealed record JobExecution(
    Guid Id,
    Guid JobId,
    JobExecutionStatus Status,
    int Attempt = 0,
    DateTimeOffset? StartedAt = null,
    DateTimeOffset? CompletedAt = null,
    string? FailureReason = null);
