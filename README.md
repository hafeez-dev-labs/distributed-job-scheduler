# Distributed Job Scheduler

A distributed job scheduling platform inspired by Hangfire, Azure Functions, and cron. The project is designed as a production-oriented .NET distributed-systems exercise covering scheduling, queuing, worker coordination, retries, failure recovery, and observability.

## Architecture

```text
Client
  |
  v
API / Job Registry
  |
  v
Scheduler
  |
  v
Queue / Broker
  |
  +-------------------+
  |                   |
v v
Worker 1           Worker N
  |                   |
  +---------+---------+
            |
            v
        Execution
            |
      +-----+------+
      |            |
   Success      Failure
                   |
             Retry / Backoff
                   |
                 DLQ

Observability and Dashboard span the platform.
```

## Example

```http
POST /jobs
Content-Type: application/json

{
  "name": "GenerateMonthlyReport",
  "schedule": "0 0 1 * *",
  "retries": 5
}
```

## Initial Project Structure

```text
src/
  DistributedJobScheduler.Api/
  DistributedJobScheduler.Application/
  DistributedJobScheduler.Domain/
  DistributedJobScheduler.Infrastructure/
  DistributedJobScheduler.Scheduler/
  DistributedJobScheduler.Worker/
  DistributedJobScheduler.Contracts/

tests/
  DistributedJobScheduler.UnitTests/
  DistributedJobScheduler.IntegrationTests/
  DistributedJobScheduler.ArchitectureTests/

docs/
  architecture/
  adr/

infra/
  docker/

README.md
DistributedJobScheduler.sln
```

## Planned Capabilities

- Cron and one-off job scheduling
- Job priorities
- Retries with exponential backoff
- Dead-letter queues and replay
- Distributed locking and leases
- Worker heartbeats and crash recovery
- Job dependencies
- Concurrency limits
- Idempotent execution
- Execution history
- Metrics, tracing, and structured logging
- Operational dashboard

## Engineering Goals

The project is intended to demonstrate production-grade distributed-systems design rather than only CRUD functionality. Scheduling, dispatch, and execution are separated so that each concern can scale independently and failure scenarios can be reasoned about explicitly.

Key guarantees and design goals include horizontal scalability, explicit job and execution state transitions, at-least-once delivery with duplicate-execution protection, crash recovery, and observable end-to-end execution state.

See the repository EPIC for the phased implementation plan.
