namespace DistributedJobScheduler.Contracts;

public sealed record CreateJobRequest(
    string Name,
    string? Schedule,
    int Retries = 3,
    string? TenantId = null,
    string? ConcurrencyGroup = null,
    int? MaxConcurrentExecutions = null);

public sealed record JobResponse(
    Guid Id,
    string Name,
    string? Schedule,
    string Status,
    string? TenantId = null,
    string? ConcurrencyGroup = null,
    int? MaxConcurrentExecutions = null);

public sealed record ExecutionResponse(
    Guid Id,
    Guid JobId,
    string Status,
    int Attempt,
    DateTimeOffset? StartedAt,
    DateTimeOffset? CompletedAt,
    string? FailureReason);

public sealed record AddJobDependencyRequest(Guid DependsOnJobId);
public sealed record JobDependencyResponse(Guid JobId, Guid DependsOnJobId);
