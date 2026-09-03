# ADR-0001: Separate Scheduling, Dispatch, and Execution

## Status
Accepted

## Context
A distributed scheduler must scale scheduling coordination, message delivery, and job execution independently. Coupling these responsibilities would make worker scaling, failure recovery, and future broker changes harder to reason about.

## Decision
Keep API, Scheduler, and Worker as separate executable hosts. Keep domain concepts independent from infrastructure. Expose infrastructure capabilities through application-owned abstractions.

## Consequences
The system has more processes and projects than a monolithic worker, but it provides explicit boundaries for horizontal scaling, failure isolation, testing, and future infrastructure replacement.
