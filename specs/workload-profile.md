# Operational Workload and Analytical Queries

## Primary Objective

This is an operational database: prioritize short transactions, point lookups, small range reads, frequent inserts/updates/deletes, predictable commit latency, and recovery correctness. Analytical queries are supported on the same authorized MVCC data, but do not drive the primary storage layout or receive permission exemptions.

Use redb key/value tables and logical indexes under the operational scheduler. Do not build a custom heap, buffer manager, or WAL. Do not introduce a mandatory column store, warehouse ingestion pipeline, materialized analytics replica, or distributed execution layer. Those require separate future decisions.

## Analytical Scope

The first storage milestone retains the existing single-table SQL subset and can perform bounded scans and native computation over authorized batches. Supporting SQL analytics is an explicit follow-on deliverable: inner equijoins, GROUP BY, COUNT/SUM/MIN/MAX/AVG, ORDER BY, and HAVING. These are required roadmap capabilities, not claims that the initial parser implements them. Window functions, recursive queries, and distributed analytics are deferred.

Execute analytical operators in the untrusted executor over sanitized batches. SQL aggregates use declared null semantics and fail when they require redacted values; they do not silently omit denied cells. COUNT(*) counts only visible rows. No privileged aggregates over hidden data are implied. Hash joins/sorts have bounded memory; initially reject excess work rather than enabling uncontrolled spill. A future spill implementation must use invocation-private scratch files, budgets, cancellation, and deletion rules.

The initial SDK sort kernel accepts at most 128 authorized keys for one declared scalar column. It places NULL before non-NULL values in either direction, preserves input order among ties, rejects REDACTED or malformed keys, and writes no partial index permutation on failure. This kernel is not SQL `ORDER BY` support until parser, row materialization, and `LIMIT` integration are implemented.

## Mixed-Workload Scheduling

The supervisor admits an invocation into an operational or analytical resource class, bounded by the caller's configured allowance. A client label cannot obtain unlimited priority or override budgets. Both classes retain snapshot isolation and the same permission model; read-only analytical grants are the default, not a mechanism to prohibit explicitly authorized writes.

Reserve queue capacity and progress for commits, redb commits, point reads, cancellation, and reclamation. Limit analytical queue depth, prefetch, CPU, output bytes, and snapshot age. Use scan-resistant cache admission so sequential scans do not evict the entire operational working set. Permit larger analytical budgets only through bounded admission configuration; never silently refresh their snapshots to relieve history pressure.

DDL/index builds and redb compaction/maintenance are separately budgeted maintenance work. Long analytical readers can retain old versions and schema definitions, so history retention and admission limits are part of performance policy, not optional cleanup.

## Performance Evidence

Measure point/range latency, update throughput, conflict rate, commit p50/p95/p99, redb bytes/commit, point-read latency, retained history, and memory-copy bytes. Add mixed workloads that measure operational latency while scans, joins, or maintenance run. No quantitative performance target is established until a repeatable benchmark and hardware/profile are recorded.

redb supplies the physical commit/recovery path. Measure its transaction/write amplification on representative operational workloads before considering another backend; never weaken durability or authorization checks for throughput.
