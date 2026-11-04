# Catalog Bootstrap, Versions, and Upgrades

## Bootstrap Without SQL Dependencies

The Rust engine contains a minimal, versioned bootstrap descriptor for system record types, reserved IDs, primitive encodings, and required catalog indexes. This descriptor is sufficient to read the catalog root and authenticate startup; it does not depend on querying SQL, user-defined policies, or loading the C SDK.

Initialization requires exclusive ownership of an empty explicitly selected data directory. Never initialize over an existing, corrupt, or unknown-format database. Acquire the instance lock, create private temporary files, and write a database UUID, format metadata, built-in type/schema/object records, reserved administrative role, and initial identity through the normal durable machinery.

Reserve internal catalog and `information_schema` namespaces. No user statements can replace them. A catalog may describe its own built-ins for inspection, but engine bootstrap descriptors remain the authoritative decoder for its format; self-description cannot redefine how arbitrary bytes are interpreted.

## Initialization and Credential Handoff

Use `UNINITIALIZED -> INITIALIZING -> READY` state recorded durably in engine metadata. Bind bootstrap to the local permission-restricted setup owner. Do not expose ordinary query or administrative listeners while initializing. Publish READY only after redb opens/recoveries successfully, mandatory catalog integrity checks pass, and bootstrap credential handoff completes.

Generate the first token in trusted code, store its verifier only, and deliver the secret once over the protected setup connection. If delivery/acknowledgement is lost, resume setup under the same exclusive local ownership, revoke the unclaimed token, and issue a replacement; never recover plaintext from storage or turn setup into anonymous login. A crash between durable creation and handoff leaves INITIALIZING and no ordinary service access. READY databases cannot reenter bootstrap to bypass authentication; administrator recovery needs an explicit offline operator procedure.

## Startup and Recovery

1. Obtain the exclusive instance lock and read validated format/root metadata.
2. Reject unsupported versions/features before changing database files.
3. Open redb through its supported recovery path; then validate required engine/catalog state.
4. Check mandatory built-ins, catalog references/indexes, credential verifier formats, and commit/outcome consistency.
5. Publish a recovered committed frontier and enable admission only after validation.

Do not silently repair missing privileges, reconstruct users with default credentials, or discard committed catalog changes to start successfully. Unsupported or corrupt databases stay unavailable with bounded operator diagnostics that contain no secrets. Read-only inspection/recovery tools also need a supported decoder and exclusive/offline access where appropriate.

## Version Domains

Persist independent versions for redb's file format, logical catalog schema, expression/policy IR, credential verifier algorithms, and built-in semantics. Distinguish them from SDK release, host ABI, IPC, and physical-plan versions. Compatibility is explicit; a newer application version is not automatically a compatible database reader. The engine does not edit redb headers or checkpoint metadata.

Unknown mandatory record types/fields or unsupported required flags fail closed. Unknown optional fields may be skipped only where that format version defines safe length-delimited skipping. Checksums detect accidental corruption, not malicious tampering. Bind roots/logs to the database UUID to reject accidental mixing.

## Upgrade Baseline

Initial upgrades are offline. Stop admission, drain or explicitly abort active invocations, complete recovery, and take exclusive ownership. Require an independently verified backup or preserved source generation before destructive conversion. No automatic migration during a normal server start.

For each supported version transition, ship trusted Rust migration code, source/target version declarations, validation routines, and deterministic test fixtures. Migrations cannot execute SQL functions or native code supplied by database users. Preserve object/principal IDs, grant semantics, committed data, and token verifier behavior unless the migration explicitly declares a supported change.

Build a new catalog/storage generation alongside the old one, with durable migration progress. Validate all records, dependencies, constraints, policies, and root references, then atomically switch the durable root to the completed generation. Before the switch, restart resumes or discards only incomplete target files; after it, startup uses the new generation. Retain the old generation until upgrade verification and the documented rollback window complete.

Rollback before publication may discard the target. After accepting writes in the new format, pointing at the old generation would lose those writes and is forbidden as an automatic rollback. Recovery must use a supported reverse migration or an explicit backup restore with acknowledged data-loss boundaries. Never let an older binary write an upgraded directory.

## Cache and Artifact Compatibility

Restart clears ephemeral metadata caches. Durable derived statistics carry source schema/format validity and may be invalidated rather than migrated. Recompile stored typed expressions from validated canonical definitions only through trusted engine code; reject definitions that cannot preserve semantics. Existing client artifacts are admitted only if their independent ABI/protocol/plan compatibility still holds; changing the catalog does not grant old artifacts new authority.

## Tests and Remaining Choices

Exercise every initialization interruption point, lost credential handoff, duplicate initialization, redb open/recovery errors, unknown engine versions, redb upgrade compatibility, old-binary rejection, and preservation of policy semantics. Version fixtures must include populated data and multiple identities, not just empty catalogs.

The [redb backend](redb-storage.md) owns physical format compatibility and crash recovery. [Catalog payloads](catalog-payloads.md) and engine metadata have independent logical versions. Pin an exact redb release and validate its supported upgrade path. Executable fixtures, expression IR and verifier profiles, outcome/audit schemas, supported migration graph, and offline administrator recovery commands remain separate work. Their absence must block claims of an upgrade-safe release, not justify permissive startup behavior.
