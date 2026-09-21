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
            if(execution.Status is JobExecutionStatus.Succeeded or JobExecutionStatus.Cancelled)
            {
                await queue.AcknowledgeAsync(message.Id,workerId,cancellationToken);
                return;
            }

            if(execution.Status == JobExecutionStatus.DeadLettered)
            {
                execution = execution with {Status=JobExecutionStatus.Pending,Attempt=0,StartedAt=null,CompletedAt=null,FailureReason=null};
                await repository.UpdateExecutionAsync(execution,cancellationToken);
                logger.LogInformation("Execution {ExecutionId} replayed from the DLQ.",execution.Id);
            }

            var job=await repository.GetAsync(message.JobId,cancellationToken);
            if(job is null){await queue.AcknowledgeAsync(message.Id,workerId,cancellationToken);return;}

            var running=execution with {Status=JobExecutionStatus.Running,Attempt=execution.Attempt+1,StartedAt=DateTimeOffset.UtcNow,CompletedAt=null,FailureReason=null};
            await repository.UpdateExecutionAsync(running,cancellationToken);
            logger.LogInformation("Worker {WorkerId} executing job {JobId}, execution {ExecutionId}, attempt {Attempt}.",workerId,running.JobId,running.Id,running.Attempt);

            var failAttempts=Environment.GetEnvironmentVariable("WORKER_FAIL_ATTEMPTS");
            if(int.TryParse(failAttempts,out var simulatedFailures) && running.Attempt<=simulatedFailures)
                throw new InvalidOperationException("Simulated worker failure.");

            await Task.Delay(10,cancellationToken);
            var completed=running with {Status=JobExecutionStatus.Succeeded,CompletedAt=DateTimeOffset.UtcNow};
            await repository.UpdateExecutionAsync(completed,cancellationToken);
            await queue.AcknowledgeAsync(message.Id,workerId,cancellationToken);
        }
        catch(OperationCanceledException) when(cancellationToken.IsCancellationRequested)
        {
            await queue.RequeueAsync(message.Id,workerId,DateTimeOffset.UtcNow,CancellationToken.None);
        }
        catch(Exception exception)
        {
            try
            {
                var execution=await repository.GetExecutionAsync(message.ExecutionId,cancellationToken);
                var job=await repository.GetAsync(message.JobId,cancellationToken);
                if(execution is null || job is null){await queue.RequeueAsync(message.Id,workerId,DateTimeOffset.UtcNow,cancellationToken);return;}

                var failed=execution with {Status=JobExecutionStatus.Failed,FailureReason=exception.Message,CompletedAt=null};
                await repository.UpdateExecutionAsync(failed,cancellationToken);

                if(ReliabilityPolicy.ShouldDeadLetter(failed.Attempt,job.RetryPolicy))
                {
                    var deadLettered=failed with {Status=JobExecutionStatus.DeadLettered};
                    await repository.UpdateExecutionAsync(deadLettered,cancellationToken);
                    await queue.DeadLetterAsync(message.Id,workerId,deadLettered,exception.Message,cancellationToken);
                    logger.LogWarning("Execution {ExecutionId} moved to DLQ after {Attempt} attempts.",failed.Id,failed.Attempt);
                }
                else
                {
                    var delay=ReliabilityPolicy.GetBackoff(failed.Attempt,job.RetryPolicy);
                    await queue.RequeueAsync(message.Id,workerId,DateTimeOffset.UtcNow.Add(delay),cancellationToken);
                    logger.LogWarning("Execution {ExecutionId} failed on attempt {Attempt}; retrying after {Delay}.",failed.Id,failed.Attempt,delay);
                }
            }
            catch(Exception recoveryException)
            {
                logger.LogError(recoveryException,"Failed to persist recovery state for execution {ExecutionId}.",message.ExecutionId);
                await queue.RequeueAsync(message.Id,workerId,DateTimeOffset.UtcNow.AddSeconds(1),CancellationToken.None);
            }
        }
        finally{semaphore.Release();}
    }

    private async Task HeartbeatAsync(string workerId,CancellationToken cancellationToken)
    {
        while(!cancellationToken.IsCancellationRequested){registry.Heartbeat(workerId);await Task.Delay(5000,cancellationToken);}
    }
}
