# Operational Metadata Implementation Plan

## Contract and Dependencies

Implement [live monitoring and audit](../../specs/operational-metadata.md). Depend on [identity/control](../ddl-identity/README.md), [supervisor](../supervisor/README.md), [transaction/outcome](../concurrency-mvcc/README.md) and [storage](../storage-manager/README.md) bookkeeping. Live observations are not repeatable invocation SQL snapshots, and object-level DDL authority is not global monitoring authority.

## Decisions Before Implementation

Define bounded registry schemas/status projections, observation times/consistency labels, pagination generations or explicit best-effort semantics, audit/outbox payloads, retention/expiry policy and sink retry budgets. Only database admins inspect global registries/audit initially; ordinary users get their own status/usage without global totals. Resolve obsolete checkpoint-job terminology to supported redb maintenance in specs before implementing job kinds.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| OM-1 | Own status succeeds while another user's/unknown ID stays non-disclosing | Authenticate every control request, project bounded invocation fields and distinguish live activity from storage-confirmed terminal status |
| OM-2 | Admin runtime pages carry observation/consistency labels and revocation stops later pages | Bounded invocation/transaction/wait/job/memory/counter registries with opaque IDs, no raw paths/pointers/query values and no SQL exposure |
| OM-3 | Registry absence after restart or retention expiry cannot become ABORTED | Query durable outcomes after lost replies, return UNKNOWN/EXPIRED and preserve committed-but-undelivered results |
| OM-4 | Both cancellation/commit orders return accepted or too-late/resolved outcome accurately | Route cancellation to synchronized storage state; an accepted control acknowledgement is not proof of rollback |
| OM-5 | Admin change and required audit/outbox either both persist or neither does | Atomic audit records for DDL, membership/grants/policies and credentials; bounded/rate-limited auth failure and admin cancel events, no secrets |
| OM-6 | Audit sink failure does not erase a committed action; optional metric failure cannot report data abort | Durable required outbox with bounded delivery queues/retention and separately best-effort telemetry |
| OM-7 | Saturated monitoring or denied requests cannot retain history/locks or starve commit/cancel | Bounded copies/short pins, release references before formatting/sends, deadlines and pressure tests |

## Acceptance and Handoff

Test control-path authorization, mid-page revocation, restart/status, audit storage versus sink failures, high-volume denials and secret-free errors/logs. Required audit persistence failure blocks its administrative change; optional telemetry failure does not change durable data outcomes. First behavior: own-status projection and a cross-user denial, backed by real identity checks.
