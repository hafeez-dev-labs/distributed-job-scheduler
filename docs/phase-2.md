# Phase 2 — Scheduler

Phase 2 adds the scheduler core without coupling scheduling to queueing or worker execution.

## Scheduling model

- Active cron jobs are evaluated in UTC.
- Each job persists `NextExecutionAt`.
- Jobs with no calculated occurrence are eligible for initial scheduling.
- Due jobs are protected by a short-lived distributed lease before their schedule is advanced.
- Missed occurrences are skipped and the next future cron occurrence is persisted.
- Scheduler instances use unique instance identifiers and can reclaim expired leases.

## Coordination

SQLite stores scheduler leases in `SchedulerLeases`, keyed by job ID. Lease acquisition is an atomic upsert that succeeds when no lease exists, the existing lease has expired, or the same scheduler instance is renewing its lease.

## Boundaries

Phase 2 evaluates and advances schedules only. Queue publication, delivery semantics, worker execution, retries, and dead-letter handling remain later phases.
