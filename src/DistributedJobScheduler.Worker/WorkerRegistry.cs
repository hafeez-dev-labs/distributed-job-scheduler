using System.Collections.Concurrent;

namespace DistributedJobScheduler.Worker;

public sealed class WorkerRegistry(ILogger<WorkerRegistry> logger)
{
    private readonly ConcurrentDictionary<string, DateTimeOffset> workers = new();
    public string Register(){var id=$"worker-{Environment.MachineName}-{Guid.NewGuid():N}";workers[id]=DateTimeOffset.UtcNow;logger.LogInformation("Worker {WorkerId} registered.",id);return id;}
    public void Heartbeat(string workerId){workers[workerId]=DateTimeOffset.UtcNow;logger.LogDebug("Worker {WorkerId} heartbeat at {HeartbeatAt}.",workerId,workers[workerId]);}
    public IReadOnlyDictionary<string,DateTimeOffset> Snapshot()=>workers;
}