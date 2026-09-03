# Phase 0 Architecture

## Dependency Direction

```text
Contracts
   ^
Application ---> Domain
   ^
Infrastructure

API ------> Application / Contracts / Infrastructure
Scheduler -> Application / Contracts / Infrastructure
Worker ----> Application / Contracts / Infrastructure
```

The Domain project contains business concepts and state vocabulary without infrastructure dependencies. Application owns use-case abstractions. Infrastructure provides technical implementations. API, Scheduler, and Worker are separate hosts so scheduling and execution can scale independently.

## Hosts

- API: external job-management surface and health endpoint
- Scheduler: future due-work evaluation and dispatch coordination
- Worker: future queue consumption and job execution

Phase 0 intentionally keeps persistence and messaging implementations minimal. Those concerns are introduced in later phases behind the application abstractions.
