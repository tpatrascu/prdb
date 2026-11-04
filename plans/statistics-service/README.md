# Statistics Service Implementation Plan

## Contract and Dependencies

Implement [statistics](../../specs/statistics-service.md) as a trusted module within [storage](../storage-manager/README.md). Depend on [authorization](../authorization/README.md), [snapshots/overlays](../concurrency-mvcc/README.md), [dictionary validity](../data-dictionary/README.md) and planner-role [IPC](../ipc-protocol/README.md). A separate collector process and global statistics are not initial prerequisites.

## Decisions Before Implementation

Specify initial estimate fields/populations, uncertainty/unknown/unsupported responses, bounded predicate support, work budgets and opaque context validity tokens. Define cache validity across principal/grant/policy/data/schema/private-write generation. Start without cross-principal reuse. Fix which logical access-path information is public; never release physical counts, global epochs/costs or raw samples indirectly.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| ST-1 | Executor calling estimate/refresh and planner requesting unrestricted data are denied | Dedicated planning handles/role, approved schema/access paths and explicit request limits |
| ST-2 | Scoped visible/readable/null/redacted populations match a reference authorized scan | Small on-demand trusted Rust summaries over invocation snapshot plus overlay; separate salary-readable population from visible-row population |
| ST-3 | Changing hidden values or adding invisible rows leaves normalized released estimates unchanged | Compare paired datasets, including predicate errors, sizes and metadata; no hidden-value evaluation or global-summary substitution |
| ST-4 | Estimate-specific exhaustion returns unknown, but context cancellation/deadline drains and terminates | Independent allowance, task-owned snapshot pins, cancellation cleanup and fixed-heuristic optimizer integration |
| ST-5 | A private write or changed authorization context prevents stale summary reuse and cross-invocation leakage | Transaction-local write generation and complete validity keys; safe invalidation even when notifications are lost |
| ST-6 | Later private collection is bounded, undisclosed and cannot starve commits | Optional trusted committed-snapshot collectors, protected rebuildable stats store and separately admitted maintenance; no planner-triggered unrestricted refresh |

## Acceptance and Handoff

Use authorized-view reference fixtures with nulls/redactions and repeated chosen-predicate probes. A sample lacking redactions is not permission to execute an aggregate, and readable-only summaries never redefine SQL semantics. Record planning overhead/quality on selective policies and mixed workloads before richer sampling/caches. Timing/cache noninterference is not established by normalized-content tests. First behavior: a role-denied estimate request, then a bounded exact visible-row summary.
