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
