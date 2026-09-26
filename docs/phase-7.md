# Phase 7 — Dependencies & Advanced Scheduling

## Dependency model

Jobs can declare dependencies on other jobs through the job API. A dependency points from a dependent job to a prerequisite job.

The repository rejects self-dependencies and transitive cycles before persisting the relationship.

A dependent execution remains pending while prerequisites have not yet succeeded. If a prerequisite reaches Failed, DeadLettered, or Cancelled without any successful execution, the dependent execution is failed with an explicit dependency error. The same dependency checks are applied again when retries or requeued messages are processed.

A prerequisite is considered satisfied once it has at least one successful execution. This keeps the scheduling contract deterministic without tying dependency resolution to a specific external scheduler or message broker.

## Concurrency model

Each job can optionally define MaxConcurrentExecutions.

The concurrency scope is selected from the available identity fields:

- No tenant and no group: limit applies to the individual job.
- Tenant without a group: limit applies across all jobs in that tenant.
- Tenant plus group: limit applies across jobs sharing that tenant/group.

Execution lease acquisition enforces the configured limit atomically in the SQLite repository and under an execution lock in the in-memory test repository. An execution that cannot acquire a slot is requeued without starting work.

## Lifecycle interactions

Paused jobs are requeued and remain non-executable until resumed. Cancelled jobs are transitioned to a cancelled execution state and their queue messages are acknowledged. Dependency checks happen before execution lease acquisition, so waiting or blocked work does not consume a concurrency slot.

Retries preserve the same dependency and concurrency checks, preventing a retry from bypassing prerequisite or concurrency rules.

## API

POST /jobs
GET /jobs/{id}
POST /jobs/{id}/dependencies
GET /jobs/{id}/dependencies
POST /jobs/{id}/pause
POST /jobs/{id}/resume
POST /jobs/{id}/cancel
POST /jobs/{id}/trigger
GET /jobs/{id}/executions/{executionId}

Example job creation:

POST /jobs
Idempotency-Key: tenant-report
Content-Type: application/json

{
  "name": "GenerateMonthlyReport",
  "schedule": "0 0 1 * *",
  "retries": 5,
  "tenantId": "tenant-1",
  "concurrencyGroup": "reports",
  "maxConcurrentExecutions": 2
}

Example dependency:

POST /jobs/{dependentJobId}/dependencies
Content-Type: application/json

{
  "dependsOnJobId": "{prerequisiteJobId}"
}

## Delivery

Focused Phase 7 implementation against main with one implementation commit.