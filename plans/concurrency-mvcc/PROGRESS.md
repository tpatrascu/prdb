# Transactions and MVCC Progress

## MV-3: disjoint-row write skew — verified

- Contract: [concurrency and MVCC](../../specs/concurrency-mvcc.md). Snapshot isolation detects concurrent changes to rows in a transaction's write set; it does not validate arbitrary read sets or preserve cross-row application invariants.
- Regression: [`snapshot_isolation_allows_documented_disjoint_row_write_skew`](../../engine/tests/transaction.rs) seeds two `on-call` rows. Two barrier-coordinated real-redb invocations both read both rows from their pinned snapshots, then stage updates to distinct rows. Both commits receive distinct commit sequences, both durable outcomes are COMMITTED, and the final state has both rows `off-call`.
- Verification: the focused `cargo test --test transaction snapshot_isolation_allows_documented_disjoint_row_write_skew -- --exact --nocapture` passed in the required Nix shell. This increment adds acceptance evidence only; no production code change was needed. There was no observed failing run because the existing engine behavior already matched the documented snapshot-isolation contract.
- Remaining: run the full workspace gates and add broader MVCC acceptance for write skew involving typed catalog rows, concurrent readers, and cancellation/commit ordering.
