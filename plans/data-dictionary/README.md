# Data Dictionary Implementation Plan

## Contract and Dependencies

Implement the storage-owned [dictionary](../../specs/data-dictionary.md) and [catalog payload v1](../../specs/catalog-payloads.md). Depend on [storage table/codecs](../storage-manager/README.md) and [toolchain schemas](../toolchain/README.md). Bootstrap/DDL consumers use these primitives without exposing raw catalog relations to executors.

## Decisions Before Implementation

Map canonical catalog family keys/payloads to redb tables and secondary lookups. Preserve envelope/TLV, field/family registries and bootstrap IDs; resolve obsolete ROW1/page/version-chain references before code assumes physical wrappers. Specify missing expression/verifier/outcome encodings in their owning contracts rather than inventing unchecked blobs. Choose bounded dependency traversal/cache limits.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| DD-1 | Hand-checked family key/payload fixtures accept canonical bytes and reject bad lengths, duplicates and unknown mandatory fields | Explicit bounded catalog codec with field registry, optional-field handling only as allowed by the version, no native struct serialization |
| DD-2 | Lookup by ID/name/parent/membership/grant/policy/dependency matches an independent snapshot scan | redb logical records and required secondary indexes, database UUID scoping, opaque non-reused IDs and naming-domain uniqueness |
| DD-3 | Renames preserve references; drop/recreate produces a different ID; invalid/dangling records cannot publish | Atomic parent/child/dependency/index validation, typed definitions and reserved namespace/ID checks |
| DD-4 | Concurrent dependency introduction blocks a stale RESTRICT drop; bounded graph exhaustion does not allow partial validation | Deterministic reverse walks and latest epoch rechecks inside final administrative publication |
| DD-5 | Old snapshots retain definitions after rename/drop and rows retain decoding/add-column defaults | Snapshot-aware decoded records and retention ownership coordinated with redb and transaction pins |
| DD-6 | Missed invalidation and cross-user cache substitutions do not change authorization or schema resolution | Immutable definition keys plus complete scoped metadata/policy/plan validity checks, bounded caches and generation-aware estimates |
| DD-7 | Corrupt mandatory catalog/index state makes startup unavailable rather than recreating authority | Lifecycle integrity validation and complete version fixtures with multiple users, grants, policies and populated rows |

## Acceptance and Handoff

Cover every catalog family and required index in a fixture matrix, including atomic changes/epochs/audit/outcome and old-reader behavior. Never expose credential verifier bytes or raw dictionary records through discovery. First behavior: one canonical catalog family fixture with truncated/unknown-mandatory-field rejection on the selected redb mapping.
