# Statistics Service Implementation Plan

## Contract and Dependencies

Implement [statistics](../../specs/statistics-service.md) as a trusted module within [storage](../storage-manager/README.md). Depend on [authorization](../authorization/README.md), [snapshots/overlays](../concurrency-mvcc/README.md), [dictionary validity](../data-dictionary/README.md) and planner-role [IPC](../ipc-protocol/README.md). A separate collector process and global statistics are not initial prerequisites.

## Initial Implemented Slice

The internal `InvocationTransaction::estimate_authorized_scan` API returns exact visible-row and requested-column VALUE/NULL/REDACTED counts from the pinned snapshot plus private overlay, or `Unknown` without partial counts. It scans at most 16,384 candidates and only returns exact results when row inclusion can be proven independent of row values. Value-dependent table SELECT policies and invalid contexts return `Unknown` before scanning. `planner_ipc` dispatches relation descriptions and estimates through connection-scoped kind-9 handles; the IPC role matrix denies estimate operations to executors and executor scan operations to planners. Supervisor provisioning of the planner stream remains unimplemented.

## Decisions Before Further Implementation

Specify initial estimate fields/populations, uncertainty/unknown/unsupported responses, bounded predicate support, work budgets and opaque context validity tokens. Define cache validity across principal/grant/policy/data/schema/private-write generation. Start without cross-principal reuse. Fix which logical access-path information is public; never release physical counts, global epochs/costs or raw samples indirectly.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| ST-1 | Executor calling estimate/refresh and planner requesting unrestricted data are denied | The operation matrix covers the pure role check. Provisioned-channel dispatch and dedicated planning handles remain |
| ST-2 | Scoped visible/readable/null/redacted populations match a reference authorized scan | The initial table-scan summary is implemented for row-value-independent populations; predicate selectivity and broader policy-filtered estimates remain |
| ST-3 | Changing hidden values or adding invisible rows leaves normalized released estimates unchanged | The bounded implementation returns no partial result for value-dependent row filters. Paired hidden-value/invisible-row tests and broader estimate fields remain |
| ST-4 | Estimate-specific exhaustion returns unknown, but context cancellation/deadline drains and terminates | The transaction estimate returns no partial result on exhaustion and checks cancellation/deadline at row and response checkpoints; asynchronous planner-task draining and heuristic integration remain |
| ST-5 | A private write or changed authorization context prevents stale summary reuse and cross-invocation leakage | Transaction-local write generation and complete validity keys; safe invalidation even when notifications are lost |
| ST-6 | Later private collection is bounded, undisclosed and cannot starve commits | Optional trusted committed-snapshot collectors, protected rebuildable stats store and separately admitted maintenance; no planner-triggered unrestricted refresh |

## Acceptance and Handoff

Use authorized-view reference fixtures with nulls/redactions and repeated chosen-predicate probes. A sample lacking redactions is not permission to execute an aggregate, and readable-only summaries never redefine SQL semantics. Record planning overhead/quality on selective policies and mixed workloads before richer sampling/caches. Timing/cache noninterference is not established by normalized-content tests. First behavior: a role-denied estimate request, then a bounded exact visible-row summary.
