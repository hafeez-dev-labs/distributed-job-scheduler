using DistributedJobScheduler.Application;
using DistributedJobScheduler.Domain;

namespace DistributedJobScheduler.Worker;

public sealed class WorkerService(IJobRepository repository,IJobQueue queue,WorkerRegistry registry,ILogger<WorkerService> logger)
{
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var workerId=registry.Register();
        var concurrency=Math.Max(1,Environment.GetEnvironmentVariable("WORKER_CONCURRENCY") is string value && int.TryParse(value,out var parsed)?parsed:4);
        using var semaphore=new SemaphoreSlim(concurrency,concurrency);
        var heartbeat=HeartbeatAsync(workerId,cancellationToken);
        try
        {
            while(!cancellationToken.IsCancellationRequested)
            {
                await semaphore.WaitAsync(cancellationToken);
                var message=await queue.TryDequeueAsync(workerId,DateTimeOffset.UtcNow,TimeSpan.FromSeconds(30),cancellationToken);
                if(message is null){semaphore.Release();await Task.Delay(250,cancellationToken);continue;}
                _=ExecuteAsync(message,workerId,semaphore,cancellationToken);
            }
        }
        finally { await heartbeat; }
    }

    private async Task ExecuteAsync(QueueMessage message,string workerId,SemaphoreSlim semaphore,CancellationToken cancellationToken)
    {
        try
        {
            var execution=await repository.GetExecutionAsync(message.ExecutionId,cancellationToken);
            if(execution is null){await queue.AcknowledgeAsync(message.Id,workerId,cancellationToken);return;}
            var running=execution with {Status=JobExecutionStatus.Running,Attempt=execution.Attempt+1,StartedAt=DateTimeOffset.UtcNow,FailureReason=null};
            await repository.UpdateExecutionAsync(running,cancellationToken);
            logger.LogInformation("Worker {WorkerId} executing job {JobId}, execution {ExecutionId}, attempt {Attempt}.",workerId,running.JobId,running.Id,running.Attempt);
            await Task.Delay(10,cancellationToken);
            var completed=running with {Status=JobExecutionStatus.Succeeded,CompletedAt=DateTimeOffset.UtcNow};
            await repository.UpdateExecutionAsync(completed,cancellationToken);
            await queue.AcknowledgeAsync(message.Id,workerId,cancellationToken);
        }
        catch(OperationCanceledException) when(cancellationToken.IsCancellationRequested){await queue.RequeueAsync(message.Id,workerId,CancellationToken.None);}
        catch(Exception exception){logger.LogError(exception,"Worker {WorkerId} failed execution {ExecutionId}.",workerId,message.ExecutionId);await queue.RequeueAsync(message.Id,workerId,cancellationToken);}
        finally{semaphore.Release();}
    }

    private async Task HeartbeatAsync(string workerId,CancellationToken cancellationToken)
    {
        while(!cancellationToken.IsCancellationRequested){registry.Heartbeat(workerId);await Task.Delay(5000,cancellationToken);}
    }
}