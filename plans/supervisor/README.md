# Supervisor Implementation Plan

## Contract and Dependencies

Implement [supervisor admission/lifecycle](../../specs/supervisor.md) between the separate gateway, storage and disposable workers. Depend on [IPC](../ipc-protocol/README.md), [sandbox](../linux-sandbox/README.md), [identity](../ddl-identity/README.md) and [transaction](../concurrency-mvcc/README.md) contracts. A minimal launcher precedes full admission; production admission requires the real control path, not fixed demonstration users.

## Decisions Before Implementation

Specify gateway-service version and sessions scoped by gateway generation/connection, staging ownership/quotas/expiry, manifest/scope validation, invocation state transitions, resource mechanisms and result relay limits. Align the synchronized commit-submission barrier with storage; gateway/worker roles never acquire finalization authority.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| SU-1 | Admission before storage READY or with revoked credentials fails; claimed principal fields cannot authenticate | Provision trusted channels and bind storage-validated identity, scope, class, deadline and cumulative budgets |
| SU-2 | Upload creates no snapshot; wrong digest/offset or changed bytes after seal cannot launch | Supervisor-owned bounded staging, opaque reserved ID before START, immutable sealing without loading artifacts, expiry/disconnect cleanup |
| SU-3 | Revocation between upload and START rejects; repeated START launches at most once | Revalidate admission, allocate snapshot/overlay only on execution, provision executor/planner/output channels and launch a fresh confined host |
| SU-4 | Forged worker terminal bytes, output flooding or slow gateway cannot forge commit or hold storage locks | Relay bounded provisional data; generate terminal envelopes solely from storage decisions; reserve status/cancel progress |
| SU-5 | Successful return commits only after work drains; caught errors/crashes before commit abort all writes | Stop new requests, finalize via supervisor-only channel and enforce synchronized cancellation/deadline/commit races |
| SU-6 | Gateway/supervisor loss around commit leaves an authenticated reconcilable outcome without replay | Resolve storage outcomes despite absent clients, invalidate sessions, cancel pre-barrier work and reap workers/descendants |
| SU-7 | Analytical load, planner failure and retention pressure preserve bounded operational progress | Global/per-principal queues, separate planning/estimation budgets, pinned-history limits and live registry integration |

## Acceptance and Handoff

Use real local service channels plus gateway TCP end-to-end tests. Cover unstarted staging deletion, pending versus too-late cancellation, drain/reclamation, admin routing and restart/status. Successful output delivery is not commit evidence; durable commit survives lost delivery. First behavior: reject launch until READY and required confinement are established, using a minimal launcher without native-code inspection.
