# Catalog Lifecycle Implementation Plan

## Contract and Dependencies

Implement [bootstrap/startup/offline upgrades](../../specs/catalog-lifecycle.md) using the pinned [redb backend](../storage-manager/README.md), [dictionary](../data-dictionary/README.md) and [credential profile](../ddl-identity/README.md). Initialization never requires SQL, SDK, optimizer or user code. Supervisor admission waits for validated READY.

## Decisions Before Implementation

The local directory profile and Rust API flow are now specified and implemented for INITIALIZING/READY persistence, exclusive instance ownership, initial administrator creation, one-time secret transfer, and interrupted-handoff token rotation. Still define the protected setup socket/supervisor channel and delivery acknowledgement, supported version matrix, operator diagnostics, backup/retained-source verification, migration progress/publication and manual administrator recovery commands. Distinguish redb format, engine/catalog, policy IR, verifier and built-in semantics versions from network/ABI/plan versions.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| CL-1 | Bootstrap descriptor resolves reserved records without SQL; duplicate initialization cannot replace committed identities | Minimal trusted descriptor, database UUID, tables/indexes and recoverable initialization transaction |
| CL-2 | Failure at every handoff/state boundary resumes safely without a default credential or plaintext persisted secret | Protected local first-admin/token delivery, explicit lost-delivery reconciliation and one-time verifier persistence |
| CL-3 | Unknown version, required record/index corruption or redb recovery error blocks READY | Open with pinned supported redb, validate logical tables/catalog/dependencies/authority and publish readiness only after success |
| CL-4 | An older binary/unsupported verifier or IR cannot silently read-write a newer directory | Explicit independent compatibility gates and populated cross-version fixtures; ordinary startup performs no automatic migration |
| CL-5 | Offline migration interruption before publication preserves source; after publication startup uses completed target | Stop admission/drain, exclusive access, verified backup/source generation, trusted bounded conversion and redb-supported atomic generation publication |
| CL-6 | Upgrade preserves IDs, data, grants/policies and verifier semantics; post-upgrade writes prevent unsafe automatic rollback | Validate target before switch, retain source for documented window, test reverse migration/explicit restore boundaries and rebuild disposable derived caches |

## Acceptance and Handoff

The CL-1 bootstrap core and the Rust API portion of CL-2 are implemented with real isolated-file tests for duplicate initialization, lock exclusion, lost handoff and pre-commit recovery. Exercise all remaining interruption points, deliver and acknowledge through the protected supervisor channel, and cover multiple identities/policies plus populated data. Use supported redb open/upgrade procedures; never edit private headers, roots or checkpoint metadata. The generation publication design must be reconciled with redb ownership in specs before implementation. Do not claim upgrade safety before a supported transition graph and executable fixtures exist.
