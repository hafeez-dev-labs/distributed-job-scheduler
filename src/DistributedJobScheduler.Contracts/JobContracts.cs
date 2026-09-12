namespace DistributedJobScheduler.Contracts;

public sealed record CreateJobRequest(string Name, string? Schedule, int Retries = 3);
public sealed record JobResponse(Guid Id, string Name, string? Schedule, string Status);
public sealed record ExecutionResponse(Guid Id, Guid JobId, string Status, int Attempt, DateTimeOffset? StartedAt, DateTimeOffset? CompletedAt, string? FailureReason);
