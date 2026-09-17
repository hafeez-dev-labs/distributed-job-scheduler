namespace DistributedJobScheduler.Domain;

public sealed record QueueMessage(
    Guid Id,
    Guid ExecutionId,
    Guid JobId,
    JobPriority Priority,
    DateTimeOffset AvailableAt,
    DateTimeOffset? LeasedUntil = null,
    string? LeaseOwner = null);
