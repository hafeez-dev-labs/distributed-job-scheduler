namespace DistributedJobScheduler.Contracts;

public sealed record CreateJobRequest(string Name, string? Schedule, int Retries = 3);
public sealed record JobResponse(Guid Id, string Name, string? Schedule, string Status);
