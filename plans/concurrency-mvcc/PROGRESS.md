# Transactions and MVCC Progress

## MV-1: stable read snapshots and private-write isolation — verified

- Contract: [concurrency and MVCC](../../specs/concurrency-mvcc.md). An invocation reads from its retained redb snapshot plus only its own overlay. Other transactions' staged writes remain invisible, and a concurrent commit does not advance an admitted reader's snapshot.
- Regression: [`invocation_snapshot_hides_private_writes_and_keeps_repeated_reads_stable`](../../engine/tests/transaction.rs) starts reader and writer snapshots from one committed row. It checks isolation of the writer's update and insert before commit, repeatable reads after that commit, visibility of both values to a later invocation, and durable COMMITTED/ABORTED outcomes.
- Verification: the focused real-redb test passed. Rust 1.95.0 workspace build, `cargo fmt --all -- --check`, warning-denied Clippy, and the full workspace tests passed. The suite retains two ignored sandbox SQL tests because this host lacks delegated cgroup support.
- Remaining: retain this regression with the own-write, same-row conflict, and write-skew cases; test aged-reader pressure and release after cancellation.

## MV-3: disjoint-row write skew — verified

- Contract: [concurrency and MVCC](../../specs/concurrency-mvcc.md). Snapshot isolation detects concurrent changes to rows in a transaction's write set; it does not validate arbitrary read sets or preserve cross-row application invariants.
- Regression: [`snapshot_isolation_allows_documented_disjoint_row_write_skew`](../../engine/tests/transaction.rs) seeds two `on-call` rows. Two barrier-coordinated real-redb invocations both read both rows from their pinned snapshots, then stage updates to distinct rows. Both commits receive distinct commit sequences, both durable outcomes are COMMITTED, and the final state has both rows `off-call`.
- Verification: the focused `cargo test --test transaction snapshot_isolation_allows_documented_disjoint_row_write_skew -- --exact --nocapture` passed in the required Nix shell. This increment adds acceptance evidence only; no production code change was needed. There was no observed failing run because the existing engine behavior already matched the documented snapshot-isolation contract.
- Remaining: add broader MVCC acceptance for write skew involving typed catalog rows, aged-reader pressure, and cancellation/commit ordering.

## MV-8: accepted cancellation discards writes and releases the snapshot slot — verified

- Contract: [concurrency and MVCC](../../specs/concurrency-mvcc.md). At the storage checkpoint, accepted cancellation aborts the invocation, discards its overlay, durably records ABORTED, releases the retained snapshot, and frees its active-invocation admission slot.
- Regression: [`storage_manager_audits_cancellation_before_commit_and_preserves_terminal_outcomes`](../../engine/tests/commit.rs) now stages a row before cancellation. With the active snapshot cap set to one, a cloned manager rejects a second admission before cancellation; after `check_deadline` observes cancellation and aborts, the staged row is absent and a new invocation can acquire the slot. Existing assertions verify the durable ABORTED outcome and accepted audit record.
- Verification: the focused real-redb cancellation test passed. The locked workspace build, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and full locked workspace tests all passed in the required Nix shell. The suite reports 106 commit tests passed and two sandbox SQL integration tests ignored because this host lacks a delegated cgroup.
- Remaining: exercise cancellation at both sides of the irreversible redb commit boundary and verify drain behavior for in-flight storage tasks.

## MV-8: synchronous worker-channel drain — verified

- Contract: [concurrency and MVCC](../../specs/concurrency-mvcc.md) and [memory storage](../../specs/memory-storage.md). No request may outlive its invocation snapshot; worker exit and finalization wait until planner and executor handlers drain.
- Implementation evidence: shared planner/executor dispatch holds the invocation mutex for each synchronous storage request. Each handler owns a channel lease, `serve_worker_channels` joins both handlers, and cancellation/deadline abort takes the same invocation mutex before dropping the transaction snapshot.
- Regression: [`worker_exit_waits_for_both_channel_handlers_to_drain`](../../engine/src/storage_manager.rs) keeps both worker channels open, verifies exit observation rejects while their leases remain active, closes them, then verifies exit observation succeeds.
- Limits: this confirms the synchronous worker-channel lifetime model. There are no detached storage tasks in this path; a future task queue or background scan needs an explicit owned snapshot pin and drain protocol. A test that pauses inside an actual storage operation while cancellation waits remains open.

## MV-6: cancellation arbitration at the durable commit boundary — in progress

- Contract: [concurrency and MVCC](../../specs/concurrency-mvcc.md). Cancellation remains admissible while redb validates and stages a write transaction. The synchronized transition to COMMITTING now runs immediately before `WriteTransaction::commit`; cancellation accepted before that transition aborts the staged redb transaction and records ABORTED separately.
- Implementation: [`InvocationTransaction::commit`](../../engine/src/transaction.rs) delegates its final transition to [`commit_storage_changes_with_gate`](../../engine/src/commit.rs), which invokes the admission gate only after all validation and record staging.
- Verification: the real-redb status regression holds a redb writer while a commit is attempted, accepts cancellation while the invocation remains ACTIVE, then verifies durable ABORTED and absence of staged data; a separate committed outcome survives reopen. A deterministic commit-gate unit test rejects the gate and proves row, outcome, and sequence remain unpublished, then grants it and proves all three publish together. A barrier test pauses immediately after the registry enters COMMITTING and verifies that `cancel_owned_invocation` returns TOO_LATE. Focused gate/cancellation tests, locked workspace build, formatting check, warning-denied Clippy, and full locked workspace tests passed. The suite reports 106 commit tests passed and two sandbox SQL integration tests ignored because this host lacks a delegated cgroup.
- Remaining: inject and reconcile an ambiguous redb commit result, and verify in-flight storage task pins drain before snapshot release.
