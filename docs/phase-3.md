# Phase 3 — Queue and Dispatch

Phase 3 introduces the durable dispatch boundary between scheduling and worker execution.

## Queue model

- Scheduled jobs create pending executions and durable queue messages.
- Queue messages preserve the job priority.
- Messages become visible when their `AvailableAt` time is reached.
- A dequeue operation claims one message for a consumer for a short visibility timeout.
- Expired leases make messages eligible for another consumer.
- Acknowledgement removes a message only when the current consumer owns its lease.
- Repeated acknowledgement of an already-removed message is safe.
- Explicit requeue releases the consumer lease without executing the job.

## Persistence

SQLite stores queue messages in `QueueMessages` with a unique execution identifier. The queue is exposed through `IJobQueue`, so scheduling does not depend on the concrete persistence implementation.

## Scheduling boundary

The scheduler still evaluates cron expressions and advances `NextExecutionAt`. For each due job it creates an idempotent pending execution and publishes the corresponding queue message. Worker execution is intentionally not part of this phase.

## Delivery semantics

The queue provides at-least-once delivery behavior through visibility leases. A consumer that fails before acknowledgement leaves the message available after its lease expires. Exactly-once execution is not claimed.

## Boundaries

Worker registration, execution, heartbeats, graceful shutdown, retries, dead-letter handling, and production broker integrations remain later phases.
