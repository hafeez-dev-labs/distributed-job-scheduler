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
Idempotency-Key: generate-monthly-report
Content-Type: application/json

{
  "name": "GenerateMonthlyReport",
  "schedule": "0 0 1 * *",
  "retries": 5
}
```

Phase 1 provides job creation, retrieval, lifecycle operations, immediate trigger requests, cron validation, idempotency, and SQLite persistence. Phase 2 adds cron evaluation and distributed scheduler leases. Phase 3 adds durable queueing, job priorities, visibility leases, acknowledgement, and explicit requeue semantics. See `docs/phase-1.md`, `docs/phase-2.md`, and `docs/phase-3.md` for the phase contracts and boundaries.

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

## Phase 7 — Dependencies & Advanced Scheduling ✅

- [x] Job dependency relationships with API endpoints
- [x] Self-dependency and transitive-cycle rejection
- [x] Dependency-aware execution blocking and failure propagation
- [x] Per-job, tenant-wide, and tenant/group concurrency limits
- [x] Atomic concurrency enforcement at the execution-lease boundary
- [x] Pause/cancel lifecycle handling before work starts
- [x] Retry/requeue behavior respects dependency and concurrency rules
- [x] Regression coverage for dependency and advanced-scheduling behavior
- [x] Phase 7 execution semantics documented in docs/phase-7.md

## Planned Capabilities

- Authentication and authorization
- Operational dashboard
- Metrics, tracing, and structured logging
- Production broker integration
- Production deployment and load testing
