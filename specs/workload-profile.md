# Operational Workload and Analytical Queries

## Primary Objective

This is an operational database: prioritize short transactions, point lookups, small range reads, frequent inserts/updates/deletes, predictable commit latency, and recovery correctness. Analytical queries are supported on the same authorized MVCC data, but do not drive the primary storage layout or receive permission exemptions.

Use redb key/value tables and logical indexes under the operational scheduler. Do not build a custom heap, buffer manager, or WAL. Do not introduce a mandatory column store, warehouse ingestion pipeline, materialized analytics replica, or distributed execution layer. Those require separate future decisions.

## Analytical Scope

The first storage milestone retains the existing single-table SQL subset and can perform bounded scans and native computation over authorized batches. Supporting SQL analytics is an explicit follow-on deliverable: inner equijoins, GROUP BY, COUNT/SUM/MIN/MAX/AVG, ORDER BY, and HAVING. These are required roadmap capabilities, not claims that the initial parser implements them. Window functions, recursive queries, and distributed analytics are deferred.

Execute analytical operators in the untrusted executor over sanitized batches. SQL aggregates use declared null semantics and fail when they require redacted values; they do not silently omit denied cells. COUNT(*) counts only visible rows. No privileged aggregates over hidden data are implied. Hash joins/sorts have bounded memory; initially reject excess work rather than enabling uncontrolled spill. A future spill implementation must use invocation-private scratch files, budgets, cancellation, and deletion rules.

The initial SDK sort kernel accepts at most 128 authorized keys for one declared scalar column. It places NULL before non-NULL values in either direction, preserves input order among ties, rejects REDACTED or malformed keys, and writes no partial index permutation on failure. SQL `ORDER BY` now supports one source-column key through the v3 parser/binder and bounded SELECT runner. The runner materializes at most 128 matching rows and 1 MiB of row bytes, closes the scan before sorted results are delivered, and applies `LIMIT` after sorting. Exceeding either materialization bound fails the statement before sorted output; spill and multiple keys remain deferred.

The initial grouping kernel and v4 runner compute stable `COUNT(*)` groups for at most 128 matching authorized rows and 1 MiB of copied key bytes for one scalar key. `WHERE` is evaluated before grouping; equal values group together, NULL values form one group, and groups follow first appearance order. `COUNT(*)` counts every matching visible row, including NULL-key rows. The runner closes the scan before group callbacks and applies `LIMIT` after aggregation. Scan, filter, key validation, and materialization errors produce no group callbacks. The runner reports groups through its synchronous SDK callback and the SQLR v1 grouped writer. Spill, multiple keys, HAVING, and ORDER BY on grouped output remain unsupported.

The SDK also has bounded MIN/MAX, SUM, and AVG kernels and versioned grouped SELECT runners. The v5 runner accepts `MIN(column)` and `MAX(column)`, scans and filters at most 128 matching authorized rows, and caps combined copied group-key and aggregate bytes at 1 MiB. It closes the scan and validates all groups before callbacks; NULL operands are ignored, all-NULL groups return NULL, and REDACTED, malformed, or mismatched operands fail before output. SQLR output uses the declared group-key field followed by a `min` or `max` field with the aggregate column's declared type metadata; writing uses bounded provisional chunks. V6 parsing and binding accept `SUM(column)` for numeric columns, and the v6 runner applies the same row and copied-byte bounds. SUM widens integer inputs to BIGINT and DECIMAL(p,s) to DECIMAL(38,s), applies exact final-result overflow checks, and emits a SQLR `sum` field using the result type metadata. V7 accepts numeric `AVG(column)` and executes it under the same row and copied-byte bounds; the result is DECIMAL(38,r), with exact averages rounded to nearest, ties to even. The runner validates all groups before callbacks and emits an `avg` SQLR field with the derived scale, including for all-NULL groups. The maintained confined SQL artifact exercises grouped MIN, SUM, and AVG through the result relay.

The SDK has a bounded inner equijoin pair kernel for already-authorized key arrays: up to 128 values per side and at most 16,384 result pairs. It emits stable left-major matches, treats NULL as non-matching, and fails on REDACTED, malformed, or mismatched values.

The v8 parser/binder and runner accept one INNER JOIN with relation-qualified projections and one equijoin predicate over two separately authorized relation shapes. The runner resolves both relations independently, scans sequentially, and closes both before result callbacks. It bounds each side to 128 rows and combined copied key/projection bytes to 1 MiB, then emits joined projection values in stable left-major match order. NULL keys do not match; REDACTED keys fail before callbacks. SQLR encoding and confined end-to-end acceptance remain open.

## Mixed-Workload Scheduling

The supervisor admits an invocation into an operational or analytical resource class, bounded by the caller's configured allowance. A client label cannot obtain unlimited priority or override budgets. Both classes retain snapshot isolation and the same permission model; read-only analytical grants are the default, not a mechanism to prohibit explicitly authorized writes.

Reserve queue capacity and progress for commits, redb commits, point reads, cancellation, and reclamation. Limit analytical queue depth, prefetch, CPU, output bytes, and snapshot age. Use scan-resistant cache admission so sequential scans do not evict the entire operational working set. Permit larger analytical budgets only through bounded admission configuration; never silently refresh their snapshots to relieve history pressure.

DDL/index builds and redb compaction/maintenance are separately budgeted maintenance work. Long analytical readers can retain old versions and schema definitions, so history retention and admission limits are part of performance policy, not optional cleanup.

## Performance Evidence

Measure point/range latency, update throughput, conflict rate, commit p50/p95/p99, redb bytes/commit, point-read latency, retained history, and memory-copy bytes. Add mixed workloads that measure operational latency while scans, joins, or maintenance run. No quantitative performance target is established until a repeatable benchmark and hardware/profile are recorded.

redb supplies the physical commit/recovery path. Measure its transaction/write amplification on representative operational workloads before considering another backend; never weaken durability or authorization checks for throughput.
