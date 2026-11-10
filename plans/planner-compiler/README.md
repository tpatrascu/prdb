# Optimizer Implementation Plan

## Contract and Dependencies

Implement the separate Rust [optimizer](../../specs/planner-compiler.md). It consumes structured requests and emits structured plans; client-side TinyCC compiles native artifacts. Depend on [IPC](../ipc-protocol/README.md), [metadata](../metadata-discovery/README.md), [statistics](../statistics-service/README.md) and shared formats with the [SQL interpreter](../sql-library/README.md).

## Decisions Before Implementation

Define bounded query/logical/physical IR, type/operator validation, stable object/schema references, plan compatibility and semantic validation scope. Clarify redaction/error ordering before permitting pushdown/reordering. Define planning budget separately from estimate allowance and invocation resources. Plans confer no permission or correctness guarantee for arbitrary user code.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| OP-1 | Forged/malformed structured input, excessive nesting and unsupported types fail independently of the C parser | Bounded Rust v1 single-table SELECT request decoder implemented; isolated optimizer process, bounded queues/work, and supervisor-bound submission channels remain |
| OP-2 | Hidden relation and nonexistent relation bind with non-disclosing behavior; planner cannot choose principal | `bind_select_request_via_metadata` uses the invocation-bound planner connection and binds schema/table IDs, versions/epochs, and typed column references; missing or mismatched relation names share one error. Isolated process integration and versioned plan output remain |
| OP-3 | Reference single-table read/write plans execute correctly through the SDK with independently resolved handles | Rust and C validation, ABI v4 submission, identity checking, handle acquisition, and bounded unfiltered scan/projection/LIMIT/cancellation are implemented; Filter execution, SQL result emission, real process/channel integration, and bounded mutation plans remain |
| OP-4 | Unknown estimate uses fixed heuristics while cancelled/expired planning terminates | Use dedicated role-bound storage estimates; cap each planner connection at 16 estimates independently of the 256-request stream cap, while preserving overall planning deadline/cancellation behavior |
| OP-5 | Index/predicate rewrites preserve authorized-view results and redaction errors | Add point/range access after scan equivalence; storage validates every access path/pushdown, and estimates never establish permission |
| OP-6 | Mutation generation, policy/schema differences or missed invalidation prevent stale/cross-user cache reuse | Initially avoid shared scoped caches; key any later reuse by complete validity context, private-write generation and dependencies |
| OP-7 | Required join/group/sort plans match reference semantics within admitted memory/work | Add analytical planning with bounded search/candidate counts and heuristics; measure operational planning latency under analytical load |

## Acceptance and Handoff

Fuzz structured inputs and invalid plans, test restricted metadata/estimate requests over real channels, and compare SDK execution with reference results. Mutate only hidden data to test normalized released estimates/metadata; record timing/inference limits separately. Planner compromise must not grant broader data access. First behavior: a bounded independently validated single-table request and rejection of an unsupported node.
