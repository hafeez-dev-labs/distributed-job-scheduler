using DistributedJobScheduler.Application;
using DistributedJobScheduler.Domain;

namespace DistributedJobScheduler.Worker;

public sealed class WorkerService(IJobRepository repository, IJobQueue queue, WorkerRegistry registry, ILogger<WorkerService> logger)
{
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var workerId = registry.Register();
        var concurrency = GetPositiveInt("WORKER_CONCURRENCY", 4);
        using var semaphore = new SemaphoreSlim(concurrency, concurrency);
        var heartbeat = HeartbeatAsync(workerId, cancellationToken);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await semaphore.WaitAsync(cancellationToken);
                var message = await queue.TryDequeueAsync(workerId, DateTimeOffset.UtcNow, TimeSpan.FromSeconds(30), cancellationToken);
                if (message is null)
                {
                    semaphore.Release();
                    await Task.Delay(250, cancellationToken);
                    continue;
                }

                _ = ExecuteAsync(message, workerId, semaphore, cancellationToken);
            }
        }
        finally
        {
            await heartbeat;
        }
    }

    private async Task ExecuteAsync(QueueMessage message, string workerId, SemaphoreSlim semaphore, CancellationToken cancellationToken)
    {
        try
        {
            var execution = await repository.GetExecutionAsync(message.ExecutionId, cancellationToken);
            if (execution is null)
            {
                await queue.AcknowledgeAsync(message.Id, workerId, cancellationToken);
                return;
            }

            if (execution.Status is JobExecutionStatus.Succeeded or JobExecutionStatus.Cancelled)
            {
                await queue.AcknowledgeAsync(message.Id, workerId, cancellationToken);
                return;
            }

            if (execution.Status == JobExecutionStatus.DeadLettered)
            {
                execution = execution with
                {
                    Status = JobExecutionStatus.Pending,
                    Attempt = 0,
                    StartedAt = null,
                    CompletedAt = null,
                    FailureReason = null
                };
                await repository.UpdateExecutionAsync(execution, cancellationToken);
                logger.LogInformation("Execution {ExecutionId} replayed from the DLQ.", execution.Id);
            }

            var job = await repository.GetAsync(message.JobId, cancellationToken);
            if (job is null)
            {
                await queue.AcknowledgeAsync(message.Id, workerId, cancellationToken);
                return;
            }

            var now = DateTimeOffset.UtcNow;
            var executionLease = TimeSpan.FromSeconds(GetPositiveInt("WORKER_EXECUTION_LEASE_SECONDS", 30));
            if (!await repository.TryAcquireExecutionLeaseAsync(message.ExecutionId, workerId, now, executionLease, cancellationToken))
            {
                await queue.RequeueAsync(message.Id, workerId, DateTimeOffset.UtcNow.AddSeconds(1), cancellationToken);
                return;
            }

            var running = execution with
            {
                Status = JobExecutionStatus.Running,
                Attempt = execution.Attempt + 1,
                StartedAt = DateTimeOffset.UtcNow,
                CompletedAt = null,
                FailureReason = null,
                LeaseOwner = workerId,
                LeaseUntil = now.Add(executionLease)
            };

            if (!await repository.TryUpdateExecutionAsync(running, workerId, DateTimeOffset.UtcNow, cancellationToken))
            {
                await queue.RequeueAsync(message.Id, workerId, DateTimeOffset.UtcNow.AddSeconds(1), cancellationToken);
                await repository.ReleaseExecutionLeaseAsync(message.ExecutionId, workerId, cancellationToken);
                return;
            }

            using var executionCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var leaseLost = false;
            var renewalTask = RenewExecutionLeaseAsync(
                message.ExecutionId,
                workerId,
                executionLease,
                executionCancellation,
                () =>
                {
                    leaseLost = true;
                    executionCancellation.Cancel();
                });

            try
            {
                var workSeconds = GetPositiveDouble("WORKER_EXECUTION_SECONDS", 0.01);
                var timeoutSeconds = GetPositiveDouble("WORKER_EXECUTION_TIMEOUT_SECONDS", 30);
                await Task.Delay(TimeSpan.FromSeconds(workSeconds), executionCancellation.Token)
                    .WaitAsync(TimeSpan.FromSeconds(timeoutSeconds), executionCancellation.Token);

                if (leaseLost)
                {
                    logger.LogWarning("Execution {ExecutionId} lost its lease before completion.", running.Id);
                    return;
                }

                var completed = running with
                {
                    Status = JobExecutionStatus.Succeeded,
                    CompletedAt = DateTimeOffset.UtcNow
                };

                if (!await repository.TryUpdateExecutionAsync(completed, workerId, DateTimeOffset.UtcNow, cancellationToken))
                {
                    logger.LogWarning("Execution {ExecutionId} could not commit completion because its lease was lost.", completed.Id);
                    return;
                }

                await queue.AcknowledgeAsync(message.Id, workerId, cancellationToken);
                return;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && leaseLost)
            {
                logger.LogWarning("Execution {ExecutionId} stopped because its lease was reclaimed.", running.Id);
            }
            catch (TimeoutException)
            {
                await HandleFailureAsync(message, workerId, new TimeoutException("Worker execution exceeded the configured timeout."), cancellationToken);
            }
            catch (Exception exception)
            {
                await HandleFailureAsync(message, workerId, exception, cancellationToken);
            }
            finally
            {
                executionCancellation.Cancel();
                try
                {
                    await renewalTask;
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await queue.RequeueAsync(message.Id, workerId, DateTimeOffset.UtcNow, CancellationToken.None);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task HandleFailureAsync(QueueMessage message, string workerId, Exception exception, CancellationToken cancellationToken)
    {
        var execution = await repository.GetExecutionAsync(message.ExecutionId, cancellationToken);
        var job = await repository.GetAsync(message.JobId, cancellationToken);

        if (execution is null || job is null)
        {
            await queue.RequeueAsync(message.Id, workerId, DateTimeOffset.UtcNow, cancellationToken);
            return;
        }

        var failed = execution with
        {
            Status = JobExecutionStatus.Failed,
            FailureReason = exception.Message,
            CompletedAt = null
        };

        if (!await repository.TryUpdateExecutionAsync(failed, workerId, DateTimeOffset.UtcNow, cancellationToken))
        {
            await queue.RequeueAsync(message.Id, workerId, DateTimeOffset.UtcNow, cancellationToken);
            return;
        }

        if (ReliabilityPolicy.ShouldDeadLetter(failed.Attempt, job.RetryPolicy))
        {
            var deadLettered = failed with { Status = JobExecutionStatus.DeadLettered };
            if (await repository.TryUpdateExecutionAsync(deadLettered, workerId, DateTimeOffset.UtcNow, cancellationToken))
            {
                await queue.DeadLetterAsync(message.Id, workerId, deadLettered, exception.Message, cancellationToken);
                await repository.ReleaseExecutionLeaseAsync(message.ExecutionId, workerId, cancellationToken);
                logger.LogWarning("Execution {ExecutionId} moved to DLQ after {Attempt} attempts.", failed.Id, failed.Attempt);
            }
            return;
        }

        var delay = ReliabilityPolicy.GetBackoff(failed.Attempt, job.RetryPolicy);
        await queue.RequeueAsync(message.Id, workerId, DateTimeOffset.UtcNow.Add(delay), cancellationToken);
        await repository.ReleaseExecutionLeaseAsync(message.ExecutionId, workerId, cancellationToken);
        logger.LogWarning("Execution {ExecutionId} failed on attempt {Attempt}; retrying after {Delay}.", failed.Id, failed.Attempt, delay);
    }

    private async Task RenewExecutionLeaseAsync(
        Guid executionId,
        string workerId,
        TimeSpan leaseDuration,
        CancellationTokenSource cancellation,
        Action leaseLost)
    {
        var interval = TimeSpan.FromMilliseconds(Math.Max(250, leaseDuration.TotalMilliseconds / 3));

        try
        {
            while (!cancellation.IsCancellationRequested)
            {
                await Task.Delay(interval, cancellation.Token);
                if (!await repository.RenewExecutionLeaseAsync(executionId, workerId, DateTimeOffset.UtcNow, leaseDuration, cancellation.Token))
                {
                    leaseLost();
                    return;
                }
            }
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
        }
    }

    private async Task HeartbeatAsync(string workerId, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            registry.Heartbeat(workerId);
            await Task.Delay(5000, cancellationToken);
        }
    }

    private static int GetPositiveInt(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), out var value) && value > 0 ? value : fallback;

    private static double GetPositiveDouble(string name, double fallback) =>
        double.TryParse(Environment.GetEnvironmentVariable(name), out var value) && value > 0 ? value : fallback;
}
