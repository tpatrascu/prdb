# Operational Metadata, Monitoring, and Diagnostics

## Consistency and Access

Operational state is live, not part of the invocation's repeatable MVCC data snapshot. Expose it through authenticated control APIs, not normal executor SQL or `information_schema`. Each response includes an observation time and consistency label; a bounded snapshot of one registry is not a transactionally consistent snapshot of all processes.

An authenticated user may inspect status for their own invocations. Database administrators may inspect global registries and request administrative cancellation. Object-level DDL privileges do not imply access to other users' queries, credentials, or global workload statistics. A delegated monitoring role can be designed later; the initial global privilege is reserved database administration.

## Registries

| Surface | Example fields | Persistence / exposure |
| --- | --- | --- |
| Invocations | Opaque ID, owner, ACTIVE/COMMITTING/terminal state, start/deadline, cancellation state, budget usage | Live registry plus durable final outcome; own or administrator |
| Storage transactions | Internal transaction ID, state, write-set bytes, age, pin count | Administrator only; IDs/epochs never leaked through optimizer APIs |
| Waits/fences | Wait category, duration, opaque resource, blocker relationships | Administrator only; no raw addresses or hidden object names to owners |
| Jobs | Type, owner/controller, queued/running/completed/failed, bounded progress, failure category | DDL/index/checkpoint/reclamation work; runtime registry, with durable phase markers when required for recovery |
| Memory/history | Queue usage, pinned history, cache size, oldest-pin age | Global administrator summaries, not per-query private-data estimates |
| Counters | Admissions, conflicts, policy denials, cancellations, bytes, latency buckets | Administrator aggregates; own-invocation usage is limited to that invocation |

Do not include query text, SQL parameters, native source, row values, token secrets/verifiers, filesystem paths, or arbitrary exception dumps by default. Diagnostics containing object names require authority to see those objects. Even aggregate global metrics reveal workload and data-volume information, so ordinary users and the optimizer do not receive them.

## APIs and Ordering

```text
get_invocation_status(invocation_id) -> authorized_status
list_own_invocations(page_limit, continuation?) -> page
admin_list_runtime(kind, page_limit, continuation?) -> page
cancel_invocation(invocation_id, reason_code) -> accepted_or_terminal
```

Authenticate and authorize every control call against current state. Use bounded pages/cursors and opaque identifiers. Recheck authority between pages; fail if a cursor expires or privilege changes. Runtime pages may show changes between reads and must not expose a global total to a caller limited to their own invocations. Define registry-generation tokens for coherent pagination where supported; otherwise explicitly mark best-effort enumeration.

An accepted cancellation is not proof of rollback. Resolve its race with commit through storage's durable transaction state. A COMMITTED outcome remains committed even if result delivery, executor exit, or telemetry collection fails. Registry absence after restart is not ABORTED: consult the [durable outcome store](physical-metadata.md), and return UNKNOWN/EXPIRED when the retained record cannot establish a result.

## Audit Events

Record administrative DDL, membership/grant/policy changes, token lifecycle operations, authentication failures, and administrative cancellation with actor/target IDs, bounded action/result codes, and correlation IDs. Do not log credential secrets, query contents, row values, or hidden constraint keys. A successful administrative state change must have its audit record or audit outbox entry included in the same durable commit; external log delivery may follow asynchronously.

### AUD1 Payload v1

The initial `audit_v1` value is a fixed header followed by target IDs: ASCII `AUD1`; little-endian u16 version `1`; little-endian u16 action code; little-endian u16 result code; little-endian u16 target count; 16-byte nonzero event ID; 16-byte actor ID (all zero means authentication did not produce a principal); 16-byte nonzero correlation ID; signed little-endian i64 UTC microseconds; then exactly `target_count` raw 16-byte nonzero target IDs in strict unsigned-byte order. The redb key is the 16-byte event ID and must match the value. The fixed header is 68 bytes; target count is at most 256 and total payload at most 4,164 bytes. Duplicate or unsorted targets, unknown codes, zero required IDs, nonpositive timestamps, and any non-exact length fail closed. Action codes are DDL=1, authority change=2, token lifecycle=3, authentication failure=4, administrative cancellation=5. Result codes are succeeded=1, denied=2, failed=3, accepted=4. This envelope deliberately contains no query text, credential material, row values, or arbitrary diagnostic text.

Keep audit storage separate from ordinary catalogs and data reads. Only database administrators can inspect it initially. Bound retention, sink queues, event sizes, and authentication-failure rates. If a required administrative audit record cannot be committed, fail that administrative action; do not claim the action was audited. Routine metrics failure must not convert committed database writes into reported aborts.

Audit is not tamper-proof against an administrator controlling database files; external tamper-evident collection is a future feature. Do not retain plaintext tokens to improve debugging or recovery.

## Resource Safety and Tests

Monitoring must not hold commit/catalog locks while formatting or waiting for a client. Use bounded copies or pinned registry snapshots and release references promptly. Telemetry observers cannot pin data history indefinitely or bypass queue/admission budgets. Separate required commit/outcome bookkeeping from optional high-volume metrics.

Test cross-user status denial, unknown IDs, revocation mid-page, commit/cancel races, restart with committed-but-undelivered results, retention expiry, high-volume denied requests, audit sink failure, partial registry snapshots, and the absence of secrets in every error/log path. No SQL-visible live system views are promised in the initial implementation.
