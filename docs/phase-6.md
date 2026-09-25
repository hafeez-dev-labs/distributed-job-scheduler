# Phase 6 — Distributed Coordination and Execution Safety

## Execution ownership

Each worker execution now has an explicit lease owner and expiry time persisted with the execution record.

A worker must acquire the execution lease before transitioning an execution to Running. State updates while the execution is active require the same lease owner and a non-expired lease.

## Lease renewal

Long-running worker executions renew their execution lease at roughly one-third of the lease duration. If renewal fails, the worker stops treating the execution as owned and does not commit a successful completion.

## Recovery

An execution whose lease has expired can be acquired by another worker. This provides recovery from a crashed or abandoned worker without relying on an in-memory worker registry.

## Timeouts

Worker execution duration and execution timeout can be controlled with:

- WORKER_EXECUTION_SECONDS
- WORKER_EXECUTION_TIMEOUT_SECONDS
- WORKER_EXECUTION_LEASE_SECONDS

A timeout is recorded as a failure and follows the existing bounded retry/dead-letter policy.

## Duplicate delivery

Queue delivery remains at-least-once. A duplicate or reclaimed queue message must also acquire the execution lease before work begins. A worker that cannot acquire the lease requeues the message rather than executing the same execution concurrently.

## Scheduler leases

Scheduler leases can now be renewed explicitly by the repository layer, allowing future long-running scheduling operations to extend ownership without changing the existing lease model.

## Semantics

The scheduler and worker pipeline intentionally uses at-least-once delivery with duplicate-execution protection at the execution-lease boundary. Exactly-once processing is not claimed.
