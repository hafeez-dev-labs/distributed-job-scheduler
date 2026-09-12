# Phase 1 — Job Management API

Phase 1 turns the foundation into a usable job-management surface while keeping scheduling, queueing, and execution outside the API responsibility.

## Endpoints

- `POST /jobs` creates an active job. The `Idempotency-Key` header is required.
- `GET /jobs/{id}` retrieves a job.
- `POST /jobs/{id}/pause` pauses an active job.
- `POST /jobs/{id}/resume` resumes a paused job.
- `POST /jobs/{id}/cancel` cancels a job. Cancellation is terminal.
- `POST /jobs/{id}/trigger` creates a pending execution. The `Idempotency-Key` header is required.
- `GET /jobs/{id}/executions/{executionId}` retrieves execution metadata.

## Job creation

```http
POST /jobs
Idempotency-Key: create-monthly-report
Content-Type: application/json

{
  "name": "GenerateMonthlyReport",
  "schedule": "0 0 1 * *",
  "retries": 5
}
```

A valid cron expression uses the standard five-field format. A missing schedule is treated as a one-off job. Invalid schedules and negative retry counts are rejected before persistence.

## Lifecycle

```text
Active <-> Paused
   |
   v
Cancelled
```

Repeated pause, resume, and cancel requests are safe when the requested state is already reached. Invalid transitions return `409 Conflict`.

Triggering a job creates a `Pending` execution. Dispatch and execution are intentionally deferred to later phases.

## Idempotency

Create and trigger operations use separate idempotency namespaces. Reusing the same key for the same operation returns the original resource instead of creating duplicate work.

The SQLite repository performs the resource and idempotency writes together so the key and resource remain consistent across process restarts.

## Persistence

The API uses SQLite by default with `Data Source=distributed-job-scheduler.db`. The connection can be overridden with the `ConnectionStrings:Scheduler` configuration value.

Repository access remains behind `IJobRepository`, keeping application logic independent from the persistence implementation.

## Boundaries

Phase 1 does not evaluate schedules, acquire distributed leases, publish broker messages, execute jobs, or implement retries and DLQ behavior. Those concerns remain in their planned phases.
