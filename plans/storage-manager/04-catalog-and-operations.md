# P7–P10: Catalog, Maintenance, and Disk I/O

## P7 — Complete Initial Catalog and Administrative DDL

Sources: [dictionary](../../specs/data-dictionary.md), [schema DDL](../../specs/ddl-schema.md), [identity](../../specs/ddl-identity.md), [permission DDL](../../specs/ddl-permissions.md).

1. Complete each specified catalog record family, stable-ID lookup, dependency edge, and version-aware index. Keep credentials, protected policies, physical maps, and derived statistics outside ordinary catalog disclosure.
2. Implement schema/table/index creation and supported alterations/drops; add-column defaults retain the definition effective at the addition, not the current default. Reserve built-in namespaces and reject unsupported DDL explicitly.
3. Add secondary B-trees with version-aware entries and golden composite-key behavior. Retain old entries needed by snapshots; compare optimized access with the authorized reference scan before enabling it.
4. Complete NOT NULL, CHECK, primary/unique, and RESTRICT foreign-key constraints. Validate final private changes against current committed state under the gate, including both parent and child directions. Null-bearing unique/FK keys follow the specified SQL rules.
5. Implement per-object DDL fences in stable order. Fence affected write commits, capture a current snapshot, build/validate privately, then durably publish. Long builds do not hold the global commit gate; unrelated tables must progress. Revalidate old writers against schema/write epochs.
6. Complete user/role/token and permission administration, role-cycle/dependency rejection, grant expansion rules, current-authority validation, and atomic audit. Treat an administrative batch as all-or-nothing.
7. Complete metadata filtering, privileged inspection, and cache validity keys. Correctness must survive a dropped cache-invalidation notification.

**Red tests:** simultaneous duplicate keys; child insertion versus parent deletion in both commit orders; a key changed twice within one transaction; failed CHECK with hidden fields; a new column accidentally inheriting old field grants; stale writer after index publication; old reader after rename/drop; cancelled index build; atomic admin batch rollback. Test oversize existing keys during index creation.

Index acceleration must preserve authorization failures as well as returned rows. If a predicate can encounter redacted cells, skipping candidates by their hidden raw values may suppress a required error. Keep the reference path unless equivalence is established for that operation; an apparently faster index is not sufficient evidence.

**Exit:** every initial supported DDL operation has success, denial/invalid-input, concurrency, and recovery coverage. Indexes appear only after complete durable publication. No current catalog cache grants authority by itself.

## P8 — Checkpoints, Reclamation, Startup, and Lifecycle

Sources: [WAL](../../specs/wal-recovery.md), [physical metadata](../../specs/physical-metadata.md), [lifecycle](../../specs/catalog-lifecycle.md).

1. Capture checkpoint frontier C, root map, dirty revisions, and replay start R at a WAL transaction boundary. Pin required generations and release the gate before flushing.
2. Flush captured obligations or newer valid revisions with WAL-before-data and ordered writes per physical address. Sync data and newly created directory entries, then manifest, CHECKPOINT_END, and alternate root in the specified order.
3. Validate root selection against complete manifests/files/logs. Torn newest roots may use the older recoverable checkpoint; a valid root referencing corruption must not silently discard acknowledged history.
4. Account for both valid root slots when retiring manifests, base files, and WAL. Add retention for outcomes and maintenance; checkpoint completion alone does not authorize history reclamation.
5. Coordinate snapshot admission with the safe horizon. Include data/catalog readers, optimizer work, background jobs, encoders, and kernel I/O pins. Retain schema definitions still needed by stored versions. Reclaim obsolete row/index/overflow/allocation state transactionally.
6. Implement pressure policy: bounded waits, admission rejection, or explicit cancellation/abort of selected invocations. Never silently advance snapshots. Separate terminal context state from final resource release.
7. Complete startup integrity checks and bootstrap interruption recovery before listeners open. Reject unsupported formats without modifying files. If a source-to-target upgrade is actually supported, implement offline generation migration and interruption fixtures; otherwise reject that transition and document the limitation.

**Red tests:** checkpoint races a split/update/drop; crash at every manifest/root transition; an older root still needs a seemingly obsolete file; a later page write completes before an older one; optimizer cancellation leaves a pin; slot reuse races an admitted snapshot; outcome expiry is mistaken for abort. Repeated restart must converge on the same committed state.

**Exit:** bounded-memory recovery, concurrent checkpoints, both-root retention, and safe reclamation pass deterministic fault tests. Offline-upgrade claims are limited to tested transitions; version rejection is always covered.

## P9 — io_uring Backend and Safe Copy Reduction

Sources: [disk I/O](../../specs/disk-io.md), [memory storage](../../specs/memory-storage.md).

1. Verify current primary documentation for the chosen Rust crate and kernel/filesystem profile, pin dependencies, and document unsupported capability behavior. Start with buffered I/O.
2. Implement one bounded submission/completion owner with explicit offsets, opaque operation IDs/generations, stable owned buffers, and reserved progress capacity. Keep raw ring operations/unsafe ownership code small and documented.
3. Reuse the reference backend's dependency protocol: all WAL writes complete before sync submission, sync completes before durable-frontier advancement, data flush waits for its WAL. Directory sync has an explicit supported path.
4. Handle short operations, failed submissions, late completions, cancellation races, queue exhaustion, and shutdown draining. Cancellation completion alone cannot free a buffer still used by the original operation.
5. Select and report the backend at startup. Unsupported acceleration can select the declared reference fallback; ambiguous writes cannot trigger a silent backend switch or automatic retry of the transaction.
6. Instrument copy bytes and ownership lifetimes. Borrow validated page slices internally, gather authorized output once, and use vectored WAL writes. Do not claim buffered io_uring avoids kernel page-cache copies.

**Red tests:** reordered completions advance durability too early; a recycled operation ID consumes an old completion; a cancelled read buffer is reused; an older data image overwrites a newer one; full queues prevent commit/cancellation progress.

**Exit:** both backends pass the same storage/recovery contract suite, plus real io_uring integration tests on the recorded host profile. Model tests do not replace kernel integration. If io_uring is unavailable, report this gate unverified rather than weakening it. Direct I/O and shared memfd batches remain optional separate work.

## P10 — Scoped Statistics, Observability, and Workload Control

Sources: [statistics](../../specs/statistics-service.md), [operational metadata](../../specs/operational-metadata.md), [workload profile](../../specs/workload-profile.md).

1. Implement bounded authorized-view estimates on the planner channel. Declare readable/null/redacted populations; return unknown when an estimation-specific allowance is exhausted. Invocation cancellation or deadline expiry terminates work instead of triggering continued heuristic work.
2. Bind caches to principal/grant, pinned policy/data/schema, requested expression/population, and private-write generation. Start without cross-principal cache reuse. Global physical statistics remain internal.
3. Add own-invocation status and administrator registries with current-authority checks, bounded pagination, observation time, and explicit live consistency. Separate live state from durable outcomes and snapshot metadata.
4. Add durable audit/outbox delivery and bounded retention. Required audit failure blocks administrative changes; optional metrics failure cannot turn a committed write into a reported abort.
5. Integrate operational/analytical admission classes, scan-resistant cache admission, bounded prefetch, snapshot-age limits, and reserved commit/cancel/reclaim capacity. A client label cannot override allowances.

**Red tests:** change only hidden rows/values and observe leaked estimate changes; reuse an estimate after private writes; revoke monitoring authority between pages; inspect another user's status; block audit delivery; saturate scans while operational commits and cancellation need progress.

**Exit:** estimates match reference authorized populations, no global data reaches the planner, registry responses contain no secrets, and mixed-load tests demonstrate bounded progress. Quantitative latency targets require a recorded benchmark/profile, not invented thresholds.
