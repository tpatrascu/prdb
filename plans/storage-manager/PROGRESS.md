# Storage Manager Progress

## Signed index components — in progress

- Contract: [index-key encoding v1](../../specs/index-key-encoding.md), SQL tuple components and normative examples. Non-null signed 16/32/64-bit values use marker `01`, sign-bit normalization, and big-endian bytes. These bytes are comparable only within the same index definition.
- Red evidence: `cargo test --test index_key` ran against compiling zero-filled placeholder functions and both the example and numeric-order assertions failed. The placeholder was then replaced.
- Implementation: the Rust workspace encodes these three component widths. This is a partial codec, not an index or storage implementation.
- Verification: on x86_64 NixOS, Rust 1.95.0 and Cargo 1.95.0, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `cargo build --workspace --release --locked` passed. Two integration tests cover spec examples and selected i64 ordering boundaries.
- Limitations at that increment: no decoder, remaining component types, tuple validation, length limit, redb integration, or scan equivalence tests existed. The ordering test is not exhaustive. TinyCC 0.9.27-unstable-2025-01-06 was available for inspection, but no SDK or C test exists and its build and target profile are not pinned or validated.

## SM-1: redb foundation — in progress

- Pinned redb 4.4.0 and its transitive dependency in `Cargo.lock`; redb declares Rust 1.90 minimum and MIT OR Apache-2.0 licensing. The workspace uses Rust 1.95.0.
- Disposable real-redb tests check explicit Immediate/two-phase commit followed by reopen, explicit abort, retained snapshot visibility after a later commit, and unsigned lexicographic byte-key iteration. These are API experiments, not an engine adapter or a crash/power-loss test. Local filesystem: ext4 at `/tmp`, x86_64 NixOS.
- `cargo test --test redb_foundation --locked` passed both experiments; the workspace build, formatting check, Clippy, tests and release build also passed with this dependency.
- Reconciled stale custom-page index and catalog text with the redb table mapping. Logical value encodings, limits, migration and production filesystem policy still need decisions and tests.

## SM-2: variable-length index components — in progress

- Contract: index-key v1 non-null BYTEA escaping and prefix bounds. A compiling placeholder produced an assertion failure for the empty-value encoding; after implementation, tests cover empty and embedded-zero bytes, prefix ordering, decoding a component from a longer input, invalid markers/escapes and truncated inputs. A separate placeholder failed the finite successor example before implementation.
- This is not a complete key codec: nullable and typed component validation, limits, tuple decoding, and comparator properties remain. Callers must bound input and validate UTF-8 before encoding text.

## SM-2: fixed-width index components — in progress

- Added Boolean, UUID, and signed 128-bit coefficient encoding/decoding and signed 16/32/64-bit decoding. The focused test first failed against compiling placeholders, then passed. It checks decimal examples, integer extremes, malformed booleans, truncated payloads, and markers. Decimal precision/scale and complete-tuple validation belong to a schema-aware layer that does not yet exist.

## SM-3: bounded private overlay primitive — in progress

- Contract: [memory storage](../../specs/memory-storage.md), Transaction Memory. A compiling placeholder failed both staging and limit tests; the implementation now copies staged values, counts distinct row IDs and encoded value bytes, and clears and terminally aborts after a limit failure. Replacement and deletion update accounting. Tests also check independent overlays and explicit abort.
- This is a foundational container, not an authorized transaction: it has no snapshot reads, field patches, row versions, schema/policy checks, commit, or outcome integration. The limits do not cover map overhead or invocation-wide budgets. Do not attach it to executor requests as a write path.

## SM-3: own-write read resolution — in progress

- Contract: [concurrency/MVCC](../../specs/concurrency-mvcc.md), Version Visibility, and [memory storage](../../specs/memory-storage.md), Transaction Memory. Staged values shadow the invocation's pinned snapshot; staged deletes hide a snapshot row; absent overlay entries read from that snapshot. The resolver accepts the snapshot lookup from its caller, so this primitive does not own or refresh a redb transaction.
- Added a focused integration test covering snapshot fallback, updated value visibility, delete visibility, inserted value visibility, and unchanged snapshot data.
- Red/green evidence: Cargo could not resolve `redb` because crates.io is unreachable, both online and offline (`cargo test -p prdb-engine --test overlay --locked` failed to fetch index config; `--offline` reported no cached package). Compiling the actual overlay module with the actual integration test file as a standalone `rustc --test` harness first exposed a borrow-lifetime compile error and then a test quota mistake; after correcting both, all five overlay tests passed.
- Implementation: `Overlay::read` resolves overlay-first and invokes the snapshot closure only for an unstaged row. Its result distinguishes overlay and snapshot borrows so a short-lived snapshot read does not unnecessarily hold the overlay borrow across subsequent mutation. This preserves the existing boundary: the overlay alone does not authorize reads or writes and is not an executor write path.
- Remaining evidence: run Cargo focused test, formatting, Clippy and workspace checks when crates.io or a local redb source becomes available. No redb-pinned snapshot adapter or authorization path exists yet.

## SM-3: retained redb row snapshot — in progress

- Contract: [redb storage](../../specs/redb-storage.md), Invocation Transactions, and [memory storage](../../specs/memory-storage.md), redb Ownership. An invocation snapshot owns a redb read transaction; reads copy one encoded row value out of its guard, and own-write overlay resolution takes precedence. A later commit must not change what the retained snapshot returns.
- Added `RowSnapshot` over the named `rows_v1` byte table and a real-redb integration fixture using the documented 32-byte table-ID/row-ID key shape. The test checks copied snapshot reads, overlay replacement/deletion precedence, and old-versus-current visibility across a subsequent write.
- Verification: the current exact Rust 1.95 compiler and the prebuilt redb 4.4.0 artifact in `target/debug/deps` compiled the engine and the actual `overlay.rs` and `snapshot.rs` integration test files directly. All five overlay tests and the real-redb retained-snapshot test passed. Rustfmt was applied to changed Rust files; `git diff --check` passed.
- Cargo remains unavailable for dependency resolution: crates.io cannot be reached and redb 4.4.0 source/index data are not cached. Direct rustc linkage verifies these focused tests, not Cargo metadata, Clippy, or the workspace suite.
- Remaining: snapshot admission/context, key codec enforcement, authorization, bounded reader admission/age, statement stream rules, row versions/conflict validation, and final atomic commit are not implemented. This is still a foundational test fixture, not an executor-accessible transaction path.

## SM-1: versioned engine table schema — in progress

- Contract: [redb storage](../../specs/redb-storage.md), Database Layout. Added definitions for the initial seven tables (`engine_meta_v1`, `rows_v1`, `indexes_v1`, `catalog_v1`, `outcomes_v1`, `audit_v1`, and `stats_v1`), plus one helper that creates them in the caller's write transaction. The later index-maintenance increment added `row_indexes_v1` as an eighth table.
- A real-redb test commits the table set and metadata fixture, closes and reopens the database, then opens every named table and reads the metadata value.
- Verification: Rust 1.95 direct compilation against the existing redb 4.4.0 artifact succeeded; the schema reopen test passed. The direct method is needed because Cargo cannot reach crates.io or resolve uncached redb source. The named tables use byte-slice key/value definitions only; versioned payload codecs and the index table's final key mapping remain open.
- Remaining: catalog/row/metadata/outcome/audit codecs, schema-aware limits, initialization validation and tests for partially aborted bootstrap remain. This does not complete SM-1.

## SM-1: engine metadata codec — in progress

- Contract: [redb storage](../../specs/redb-storage.md), Database Layout, and [catalog lifecycle](../../specs/catalog-lifecycle.md), Version Domains. ENG1 is a fixed 46-byte record with a nonzero database UUID, independent catalog/expression/built-in-semantics versions, commit sequence, explicit initialization lifecycle, and zero reserved bytes.
- Added canonical encode/decode, exact-length/version/lifecycle/reserved/UUID/version rejection, hand-checked endian fixture, and integration with the `engine_meta_v1` bootstrap table fixture. The redb schema test now round-trips the encoded record through commit and reopen.
- Red/green evidence: direct Rust 1.95 compilation initially found that `MetadataError` could not propagate through test results; it now implements `Display`/`Error`. Two metadata codec tests and the real-redb schema/reopen test pass against the locally available redb 4.4.0 artifact. Rustfmt and whitespace checks are run after the final edit.
- Remaining: this format records versions but does not implement startup compatibility policy or bootstrap recovery. Row/catalog/index/outcome/audit payload codecs and schema/value limits remain open; normal Cargo, Clippy and workspace validation remain blocked by unavailable crates.io package data.

## SM-1 / SM-3: versioned row envelope — in progress

- Contract: [redb storage](../../specs/redb-storage.md), Database Layout, and [concurrency/MVCC](../../specs/concurrency-mvcc.md), Version Visibility. RWR1 stores a nonzero row version and an exact-length schema-aware payload inside a 1 MiB maximum record. Successful commits will assign their commit sequence as the row version; this increment defines the value contract, not the commit path.
- Added bounded encode/decode with magic/version/flags checks, checked length arithmetic, exact payload framing and maximum-size enforcement. Added canonical, malformed and boundary fixtures. The real-redb snapshot test now stores RWR1 rows and confirms old snapshot and newer committed row versions remain distinct while the overlay exposes its staged value.
- Verification: all focused row-record and real-redb snapshot tests passed when compiled directly with Rust 1.95 and the available redb 4.4.0 artifact. Cargo dependency resolution, Clippy and workspace checks remain unverified due unavailable crates.io/source cache.
- Remaining: default materialization, schema migration compatibility, row-local constraints, authorization, index maintenance and required audit are not implemented. Cargo-based verification remains unavailable.

## SM-4: atomic row commit and conflict check — in progress

- Contract: [concurrency/MVCC](../../specs/concurrency-mvcc.md), Commit Protocol, and [physical metadata](../../specs/physical-metadata.md). The storage commit coordinator opens one Immediate/two-phase redb write transaction, requires READY metadata, checks unique invocation and row keys, compares every current row version with the staged expected version, assigns one new engine commit sequence, and publishes row records, metadata, and a COMMITTED outcome atomically.
- Added OUT1 fixed-width outcome encoding for COMMITTED and ABORTED states. Successful row commits publish COMMITTED atomically; proven row conflicts and statement failures after invocation admission publish a durable ABORTED outcome after all private writes are discarded. Invalid identities, sequence/status combinations, retention values and lengths are rejected.
- The real-redb commit test inserts then updates two rows, confirms both receive one sequence and outcome, then submits a stale multirow write and confirms Conflict leaves both latest row payloads and the commit sequence unchanged while persisting ABORTED for the rejected invocation.
- Verification: changed engine and integration tests were compiled with Rust 1.95 against the available redb 4.4.0 artifact; the complete suite at that point passed all 26 tests across 10 integration files. Rustfmt `--check` and `git diff --check` passed. `cargo test --workspace --locked` failed at crates.io index configuration fetch, before compiling project code.
- Remaining: transaction admission through an authenticated identity path, constraints, index maintenance, required audit, cancellation barrier, and commit ambiguity resolution are absent. Principal-bound lookup is implemented at the storage helper level, but authenticated IPC/status endpoint integration is not. Cargo/Clippy/workspace checks still require crates.io source resolution.

## SM-3 / SM-4: invocation snapshot, overlay, and finalization slice — in progress

- Added `InvocationTransaction` to hold one redb snapshot, bounded private overlay, expected base row versions, and invocation outcome context. Reads return the most recent private value/delete before falling back to the snapshot. Staging captures the base version once; statement errors abort, clear earlier staged writes, and record ABORTED; insert-then-delete removes the staged row and finalizes as an outcome-only commit.
- Its finalization calls the serialized row commit path, which validates latest row versions and atomically writes changed rows, metadata sequence, and COMMITTED outcome. The API takes invocation/principal/retention values directly and currently has no trusted admission or authorization caller; do not expose it to executor requests yet.
- Real-redb tests cover own-write visibility, snapshot fallback, another invocation committing a disjoint row while the snapshot is retained, stale same-row abort, staged deletion visibility, and no-op insert/delete outcome.
- Admission now reads metadata and outcomes from the pinned snapshot, rejects non-READY databases, validates invocation/principal/deadline fields, and rejects an already-used invocation ID. The authenticated source of that identity context is still outside this slice.
- A barrier-coordinated two-thread same-row race now verifies both invocations stage from one snapshot before finalization; exactly one commits, the other records ABORTED, and the data sequence advances once.
- Final verification after this increment: Rustfmt `--check` passed for all engine sources/tests and all 31 tests across 11 integration files passed when directly compiled against the cached Rust 1.95/redb 4.4.0 artifacts. `git diff --check` passed.
- Added `lookup_outcome`, which reopens durable COMMITTED/ABORTED records by invocation ID, returns them only to the recorded principal, and treats expired/missing/other-principal records as absent. A real-redb test verifies lookup after close/reopen, wrong-principal filtering, expiry, and missing outcome.
- Remaining: bounded snapshot age/count, identity authentication beyond caller-supplied context, cursor lifecycle, authorization/policy epoch revalidation, constraints, indexes, required audit, cancellation/commit barrier, authenticated IPC status endpoint, and recovery ambiguity.

## SM-1 / SM-4: typed tuple payload and patch merge — in progress

- Contract: [redb storage](../../specs/redb-storage.md), TUP1/Scalar encoding, [schema DDL](../../specs/ddl-schema.md), and [authorization](../../specs/authorization.md), Mutation Permissions. TUP1 binds typed field bytes to a pinned schema version, validates scalar/null/range constraints, and applies a set of field patches atomically while retaining every unchanged value.
- Fixed scalar storage encodings for all initial SQL types, DATE years 0001–9999 and UTC microsecond timestamps over the matching Gregorian range. TUP1 is bounded by the RWR1 payload budget, has exact field framing, and rejects malformed/noncanonical states. Text/byte values are length-checked before copying into the encoded tuple.
- `InvocationTransaction::stage_patch` decodes the pinned row tuple, applies typed patches, and stages the merged tuple while preserving unmodified fields. A real-redb transaction test verifies this across final commit; pure codec tests cover all scalar families, null, malformed headers/states, precision, date range, patch atomicity and exact size limit.
- Verification for this increment: all 37 tests across the 12 integration files passed when directly compiled with Rust 1.95 against the available redb 4.4.0 artifact. Rustfmt `--check` for all engine files and `git diff --check` passed. Full Cargo workspace/Clippy validation remains unavailable because crates.io index/source resolution fails before project compilation.
- Limits: callers must supply the snapshot's trusted column schema and perform per-column read/write authorization before patch requests; those guarantees are not implemented here. SQL text/parameter parsing, default materialization, row-local constraints, indexes, schema migration and authenticated executor wiring remain open.

## SM-2 / SM-4: typed index keys and atomic locator maintenance — in progress

- Contract: [index-key encoding v1](../../specs/index-key-encoding.md), SQL tuple components and Complete Keys and Values, plus [redb storage](../../specs/redb-storage.md), Database Layout and Invocation Transactions. Typed SQL tuples are validated against the same column/null/range rules as TUP1, then encoded with ordered key bytes; complete keys concatenate index ID, tuple, and row UUID.
- Defined ILC1 locator bytes (32 bytes: magic, LE version, reserved zeros, row UUID, nonzero LE row version), exact decoder validation, and a key-aware decoder that rejects a locator whose UUID disagrees with the key suffix. `commit_rows_and_indexes` checks the current locator versions and applies supplied index adds/removes with row writes and outcomes in the same Immediate/two-phase redb transaction. A stale locator conflicts and leaves both row and index state unchanged while recording ABORTED.
- Red evidence: locator integration test initially failed to compile because its typed decode error did not implement `std::error::Error`; added the standard error implementation. The direct real-redb suite then exposed no remaining failures.
- `InvocationTransaction::stage_index_membership` now stages key membership against the pinned locator version and passes those mutations to final commit. Its test inserts and indexes a row, then deletes and unindexes it under consecutive commit sequences.
- `RowSnapshot` now reads registered CAT1 records by canonical key from its retained redb snapshot, copies and validates them, and rejects malformed payload/key mismatches. Index staging has separate bounded key-byte accounting and a cap of 256 distinct key changes per staged row; budget overflow aborts the whole invocation and records ABORTED.
- Verification: direct Rust 1.95 compilation against the available redb 4.4.0 artifact passed all 55 integration tests across 14 files. Direct `rustfmt --check` for all engine sources/tests and `git diff --check` passed. Direct `clippy-driver -D warnings` passed for the engine library and all integration targets after fixing the warnings it found. Cargo test/clippy could not resolve redb because crates.io `config.json` was unreachable; the installed Cargo has no `fmt` subcommand.
- Limits: invocation staging still lacks catalog-derived complete index definitions; uniqueness semantics are caller supplied; PK null rejection, foreign-key/check constraints, catalog secondary indexes and graph/authority validation, and authorization dependency derivation remain unimplemented. Generic catalog read guards now revalidate explicitly read records, but do not establish that every relevant authority/schema record was read. Cancellation/commit ambiguity handling, production outcome status IPC, and gateway/sandbox wiring remain unimplemented. Do not expose the low-level commit API to executor requests.
- Adjacent dependency: a shared schema-driven CAT1 envelope codec now validates typed catalog TLVs, includes field registries for all 16 catalog families, and preserves the existing policy-target fixture. It does not yet provide catalog persistence, semantic definitions, index lookup, or transactional DDL.
- The unified `commit_storage_changes` uses one `CommitRequest` to publish rows, indexes, catalog records, AUD1 audit values, metadata sequence, and outcome in one redb transaction. Catalog changes require a successful administrative audit event for the same principal. Unique index additions are checked against current state plus the final staged additions; NULLS DISTINCT exemptions are caller supplied.

## SM-4 / SM-5: explicit catalog read-set revalidation — in progress

- Contract: [data dictionary](../../specs/data-dictionary.md), Catalog Reads and Commit Validation. `InvocationTransaction::read_catalog_record` copies and validates one CAT1 record from its retained snapshot and captures its exact bytes; missing lookups capture an absence guard. Final write validation compares every captured guard with the current catalog inside the same Immediate/two-phase redb transaction that would publish rows, indexes, metadata, and outcome.
- A changed record or a record created after an absent lookup causes a conflict, persists ABORTED, and publishes no staged row. Both the changed-record and phantom-record cases have real-redb integration coverage.
- Verification: Rust 1.95 directly compiled and ran all 57 tests across 14 integration targets against the available redb 4.4.0 artifact. Direct `clippy-driver -D warnings` passed for the engine library and all 14 targets; rustfmt `--check` and `git diff --check` passed. `cargo test --workspace --locked` was attempted and failed before compilation because `https://index.crates.io/config.json` was unreachable. The configured Cargo binary is present in the Nix store but not on the shell PATH.
- Limits: this provides a generic guard mechanism, not dependency discovery or authorization. Callers must still identify every schema/authority epoch that affects the write. Full catalog/bootstrap integrity, DDL fences, constraint validation, row/cell policy enforcement, and the service integration stages remain outstanding.

## AU-1 dependency: bounded policy composition — in progress

- Contract: [authorization](../../specs/authorization.md), Policy Evaluation. Added trusted typed expressions for field reads, principal/role context, comparisons, Boolean composition, and NULL checks. Work is bounded to 256 nodes and 32 nested levels; runtime errors fail closed. Authorization independently requires an invocation-scope capability and direct/inherited grant. Applicable ALLOWs are alternatives, every RESTRICT must pass, and an UPDATE ALLOW must pass both old/new predicates on that same policy.
- Red evidence: the new integration target first failed to compile because the authorization module did not exist. Implemented the evaluator and added truth-table, missing-grant/scope, role/principal, NULL, type-error, depth, and UPDATE-pairing cases.
- Verification: Rust 1.95 directly compiled and ran all 73 tests across 15 integration targets against the available redb 4.4.0 artifact. Direct `clippy-driver -D warnings` passed for the engine library and all 15 targets; rustfmt `--check` and `git diff --check` passed.
- Limits: callers currently supply trusted pinned roles, grants, policies, targets, and typed rows. Catalog-derived loading, authenticated admission, authorized cell projection/redaction, mutation integration, and policy dependency epoch wiring remain required before the evaluator can authorize production requests.

## AU-2 dependency: snapshot row projection and cell redaction — in progress

- Contract: [authorization](../../specs/authorization.md), Authorized View and Mutation Permissions. `InvocationTransaction::read_authorized` selects the row from the private overlay or pinned redb snapshot before decoding it with the pinned schema and invoking the policy projector. It requires schema USAGE and at least one relevant column grant; row policy denial makes the row absent; cell denial emits a `Redacted` enum variant with no value or length; readable NULL remains distinct. Runtime policy errors abort and persist ABORTED.
- A real-redb invocation fixture shows a principal-dependent variable-length field as VALUE for one user and REDACTED for another while returning the same permitted field; another real-redb test verifies an evaluation error becomes a durable terminal abort. Pure projection tests also cover row denial and NULL/value/redaction distinctions.
- Verification: all 73 tests across 15 integration files passed via direct Rust 1.95 compilation against redb 4.4.0. Direct Clippy with warnings denied passed for the engine library and every integration target; rustfmt and whitespace checks passed. Cargo workspace verification remains blocked by crates.io reachability.
- Limits: this is a trusted engine API fixture, not authenticated user provisioning, catalog-derived policy/grant loading, sanitized IPC serialization, or proof of hostile-client non-disclosure. Updates/inserts/deletes are not yet routed through the evaluator.

## AU-4 dependency: policy-checked field patch — in progress

- Contract: [authorization](../../specs/authorization.md), Mutation Permissions. `InvocationTransaction::stage_patch_authorized` reads the private/snapshot row, decodes the pinned tuple, computes the complete patched tuple, then requires row SELECT visibility, row UPDATE authority, and both SELECT and UPDATE authority on each changed cell. UPDATE policies evaluate old and proposed values in one policy; unchanged hidden fields are preserved. Patches that touch policy-driving columns from the pinned dependency set are denied.
- A real-redb integration test commits an authorized name change while preserving an inaccessible UUID/secret field and proves a policy-driving-field patch is rejected without changing the stored row.
- Verification: all 73 tests across 15 integration targets passed after this addition. Direct Clippy with warnings denied passed for the library and all integration targets; rustfmt and whitespace checks passed. Cargo remains unable to reach crates.io.
- Limits: schemas, grants, policies, and policy-driving column IDs are supplied as trusted inputs rather than loaded from authenticated catalog state. Insert/delete/defaults and policy dependency capture/revalidation remain incomplete.

## AU-5 dependency: insert/delete authorization — in progress

- `stage_insert_authorized` accepts a fully materialized typed row, checks table INSERT and each non-generated field's INSERT grant/policy, then stages it atomically; generated fields skip cell checks but remain subject to table creation policy. `stage_delete_authorized` checks row visibility through the authorized projector and then requires explicit table DELETE grant/policy; it does not demand readability for every field.
- Real-redb coverage denies missing field INSERT and table DELETE grants with durable ABORTED outcomes, then verifies generated-column insertion and authorized deletion. No row write is published on denied attempts.
- `stage_insert_authorized_with_defaults` now distinguishes explicit NULL from DEFAULT and materializes trusted literals or `CURRENT_PRINCIPAL_ID()` before row policies and CHECK constraints. An unsupported expression aborts with no row publication.
- Limits: generated UUID allocation/collision checks, catalog-derived defaults/grants/policies/context, full constraint loading, and DDL enforcement remain unimplemented. These methods are trusted engine APIs, not the user-facing SQL path.

## SM-4 / AU-5 dependency: INSERT default materialization — in progress

- Added an explicit `InsertInput::{Value, Default}` request form. Supported default expressions in this slice are typed literals (including NULL) and the admitted principal ID; unsupported expression shapes fail closed. Missing defaults materialize to NULL and tuple schema validation enforces nullability and types.
- Real-redb coverage confirms a current-principal default is visible to both the INSERT policy and CHECK expression before the row is committed. An unsupported field-dependent default yields a durable ABORTED outcome and publishes no row.
- Verification: all 79 tests across 15 integration targets passed with direct Rust 1.95 compilation against cached redb 4.4.0. Direct Clippy with warnings denied, rustfmt `--check`, and `git diff --check` passed.
- Remaining: schema-bound defaults still arrive from a trusted caller and must be tied to captured catalog definitions; generated UUID allocation, omitted/DEFAULT column mapping from SQL, DDL, and process integration remain incomplete.
- Next dependency: validate EIR type binding and declared column/object dependencies, then derive schema, grants, and policies from their pinned catalog records.

## SM-4 / SM-5 / DD-1 dependency: catalog expression IR and pinned loading — in progress

- Red evidence: the expression-codec integration target initially failed because no `expression` module existed. Added EIR1 with a hand-checked prefix fixture, complete operator/scalar round trips, strict framing, canonical scalar validation, and limits of 256 KiB, 256 nodes, 32 levels, and field ordinals 0–4095.
- Family-7 decoding now validates the expression purpose, IR version, result metadata widths, dependency ID set, and EIR body. Catalog writes reject malformed/unsupported expression records; snapshot and invocation reads validate them. Invocation reads capture exact catalog bytes as commit guards.
- Authorized INSERT can load DEFAULT and CHECK expressions by ID from the pinned catalog, enforce purpose, materialize defaults, and evaluate CHECK and row policies against the resulting tuple. A real-redb race changes the stored default expression after it is read; the invocation durably aborts and publishes no row. The insert integration fixture also loads its CHECK expression from the catalog.
- Verification: all 85 tests across 16 integration targets passed with direct Rust 1.95 compilation against cached redb 4.4.0. Direct Clippy with warnings denied, rustfmt `--check`, and `git diff --check` passed. Cargo workspace commands remain unable to resolve the crates.io index in this environment.
- Limits: EIR decoding checks structure, not schema-aware operand/result types or whether declared dependencies match field references. Callers still provide column schemas, grants, policies, and expression IDs; generated UUID allocation, DDL authorization/fences, complete catalog loaders, and service integration remain outstanding.

## AU-6 dependency: authorized catalog dependency guards — in progress

- `AuthorizedProjection` carries the exact catalog key/value set used for the supplied principal/roles, grants, policies, and schema. Before projection or mutation, `InvocationTransaction` verifies the context principal equals the admitted invocation principal, checks each dependency byte value (including absence) against its pinned redb snapshot, and adds it to the same catalog guard map used by final commit. A mismatch rejects the operation. At final commit, concurrent catalog changes produce conflict plus durable ABORTED, with no data publication.
- The real-redb race test changes a principal authority record after an authorized field patch; the pending writer conflicts and the old row remains intact. Catalog dependency values are validated as registered CAT1 records with matching logical keys.
- Limits: callers still provide dependency sets, so omitted grant/policy/membership/schema records cannot be detected. Role closure and permission resolution are not loaded from the catalog; token revocation, schema DDL fences, and authenticated admission remain incomplete.

## AU-1 dependency: bounded role closure — in progress

- `resolve_role_closure` computes the transitive member-to-role closure from pinned membership edges using an iterative bounded traversal. It rejects zero IDs, duplicate edges, reachable cycles, and more than 4096 edges. This produces the role set consumed by policy recipient matching without recursive stack growth.
- Integration fixtures cover nested inheritance, shared inherited roles, cycle rejection, duplicate rejection, and limit failure. Administrative commit-time validation of the complete membership graph is now implemented; CAT1 membership loading for runtime authorization remains outstanding.

## SM-4 / AU-5 dependency: row-local CHECK enforcement — in progress

- Added bounded evaluation of schema-bound CHECK expressions over complete proposed tuples in the authorized insert and patch paths. SQL TRUE and UNKNOWN pass; FALSE returns a generic constraint violation; malformed/type-invalid expressions fail closed and abort. These expressions are trusted inputs, and their defining catalog records belong in the existing guarded dependency set.
- Tests cover TRUE/FALSE/NULL semantics and real-redb insert and update attempts. Rejected attempts discard staged data and record ABORTED; valid writes still commit.
- A regression test exposed that the shared permission helper checked grants but omitted the invocation scope. Updated it so every grant lookup also requires the narrowed capability; this applies consistently to table and cell authorization.
- Verification: all 74 tests across 15 integration targets passed when directly compiled with Rust 1.95 against cached redb 4.4.0. Direct Clippy with warnings denied passed for the library and every integration target; rustfmt `--check` and `git diff --check` passed. `cargo test --workspace --locked` again stopped before compilation because crates.io `config.json` was unreachable.
- Limits: expressions are not loaded or bound from catalog definitions; low-level trusted mutation methods bypass this authorized check path. NOT NULL, primary/unique and foreign-key validation, DDL fences, defaults/materialization, and commit-time cross-row constraint checks remain outstanding.

## SM-5 / DD-1 / AU-1 dependency: catalog membership graph integrity — in progress

- Membership catalog commits now read the latest family-11 records and overlay the proposed catalog changes inside the serialized redb write transaction. The final graph is checked for live member/role endpoints, role target kinds, zero IDs, cycles in any component, and the 4096-edge bound before publication. Malformed or cyclic final graphs publish no catalog/audit changes and record ABORTED.
- Red evidence: role-closure coverage showed an unreachable cycle was accepted; full-graph validation now rejects it. Real-redb tests also reject a user as a role target and a missing member, with no sequence/catalog/audit publication and durable ABORTED outcomes.
- A barrier-coordinated race submits opposite edges from the same starting catalog; exactly one commits, while the second validates against the newly committed edge and durably aborts.
- Verification: all 77 tests across 15 integration targets passed with direct Rust 1.95 compilation against the cached redb 4.4.0 artifact. Direct Clippy with warnings denied, rustfmt `--check`, and `git diff --check` passed.
- Broader DDL authority, dependency-graph, bootstrap, and catalog-index validation remain open; principal-object correspondence outside the principal records is not yet checked.

## SM-4 / AU-5 / DD-1 dependency: schema-bound catalog expressions — in progress

- Contract: [catalog payloads](../../specs/catalog-payloads.md), EIR1 and expression dependency binding. Added schema-aware checks for result type IDs and parameters, operator operand types, field ordinals, role IDs, and exact stable dependency sets. Defaults are limited to root literals and the admitted principal ID; CHECK and policy results must be Boolean.
- Catalog-loaded CHECK expressions and INSERT defaults now pass through the binder before evaluation/materialization. The loader requires ordered column IDs, schema, and expected type IDs as trusted catalog-derived inputs; malformed or mismatched expressions abort and publish no row. Updated the real-redb INSERT fixture to bind its CHECK and DEFAULT definitions.
- Red evidence: the binding tests first failed to compile because the validator API did not exist. After implementation, focused expression tests and the full direct real-redb suite passed: 87 tests across 16 integration targets using Rust 1.95 and cached redb 4.4.0. Direct Clippy with warnings denied found one cloned-reference lint, which was fixed; final lint/format/whitespace checks are being rerun.
- Spec decision: EIR1 DECIMAL literals are i128 coefficients at scale zero. The spec now requires DDL and invocation binding before execution, exact declared-vs-discovered dependencies, SQL type compatibility rules, and the bounded default expression forms.
- Remaining: schema/type/column IDs still come from trusted callers rather than a catalog-derived table-schema loader. Policies are not loaded from catalog or bound through this invocation API. Full DDL, constraints, bootstrap, gateway/sandbox IPC, and bounded snapshot-reader lifecycle remain incomplete. Cargo dependency resolution is currently unavailable because crates.io cannot be reached.
- Next: complete pinned-snapshot catalog-derived authority and policy loading, DDL epoch enforcement, and the missing service boundary stages.

## SM-4 / DD-1 dependency: pinned catalog table schema loading — in progress

- Contract: [data dictionary](../../specs/data-dictionary.md), snapshot lookups and write-time dependency guards. Added a bounded catalog-family scan (65,536 records) and `load_catalog_table_schema`, which reads the table definition, discovers its column records, sorts/checks contiguous ordinals, resolves built-in type records, validates type profiles and parameters, and returns stable column/type IDs, nullability, default masks, default expression IDs, generation modes, schema version, and write epoch.
- Every table, matching column, and referenced type record read by the loader is added to the invocation's exact-byte commit guards. The real-redb fixtures validate unsorted record insertion, typed UUID/VARCHAR decoding, type parameters and defaults metadata; one then changes a referenced type and confirms the stale schema reader commits ABORTED with a conflict. Catalog-loaded INSERT now takes the bound schema value, uses its default-expression IDs/type IDs/default masks, and derives generated columns from generation modes. Its end-to-end fixture loads both schema and CHECK/default expressions from the pinned catalog before writing.
- Red evidence: the first real-redb schema fixture failed with `CorruptCatalog` because a non-null VARCHAR column was probed using NULL while validating type shape. The type-shape check now uses a nullable probe schema while retaining the catalog nullability in the returned schema; the focused regression passes.
- Spec decision: column discovery is a bounded family scan until catalog secondary indexes are implemented. DDL must increment table schema version atomically for column changes; DDL enforcement itself is still outstanding.
- Catalog commit now rejects any existing table column add/change/drop unless the table schema version advances atomically; it also rejects moving a column between tables. The regression fixture proves a new column cannot publish without a version update.
- Remaining: bounded family scan may fail closed for larger catalogs. Catalog loader does not yet derive grants/policies/role closure or bind policy expressions (later increments below address those parts). Full DDL, control-channel, gateway/sandbox IPC, long-reader lifecycle, and recovery acceptance remain open.
- Verification after this increment: all 88 tests across 16 integration targets pass via direct Rust 1.95 compilation against cached redb 4.4.0. Direct Clippy with warnings denied, rustfmt `--check`, and `git diff --check` pass. `cargo test --workspace --locked` still fails before project compilation because crates.io `config.json` is unreachable through the configured proxy.

## AU-1 / SM-5 dependency: pinned catalog role closure — in progress

- `load_catalog_role_closure` now scans the bounded pinned membership family, validates membership payloads/epochs and user/role principal endpoints, and computes the transitive closure with the existing cycle/edge limits. It records exact guards for the membership and principal records it uses. Catalog-loaded authorization no longer needs a caller-supplied membership list for this behavior.
- A real-redb test loads a user-to-role-to-role chain, then updates one membership record; finalization of the old invocation detects the changed guard and durably conflicts. Existing pure tests continue to cover duplicate/cycle/limit behavior, while catalog commit tests enforce endpoint kinds and full-graph cycle integrity.
- Catalog commit now rejects a membership add/change/removal for an existing member unless the same transaction increases that member's principal authority epoch. This closes the unseen-edge case for snapshot role closure; a regression assertion rejects membership publication without the epoch advance.
- Remaining: grants and policy definitions/targets still need catalog loaders and bound expression execution. Catalog DDL is not yet a complete authenticated control channel.
- Verification for the combined schema/role loader and membership-epoch increment: 89 tests across 16 integration targets pass by direct Rust 1.95 compilation against redb 4.4.0; Clippy warnings-as-errors, rustfmt check, and whitespace check pass. Cargo workspace verification remains blocked before compilation by unreachable crates.io index configuration.

## AU-6 dependency: catalog-derived data grants — in progress

- `load_catalog_grants` now derives the admitted principal's role closure and resolves matching schema USAGE, table, and column data grants from pinned family-12 records. It validates scope/object/column relationships, grantor identity and authority epochs, and retains exact guards for matching grants plus referenced principal/column records. Non-query administrative privileges do not expand query authority.
- Extended the real-redb catalog fixture with direct schema USAGE, table SELECT, and column UPDATE grants and verifies all three resulting authorization entries. Catalog commit rejects a grant change for an existing grantee unless it advances that grantee's principal authority epoch in the same transaction.
- Remaining: catalog loaders still need complete privilege/target semantic validation and DDL authority checks. The authenticated administrative DDL path, control IPC, gateway/sandbox integration, and operational hardening remain open.

## AU-1 / AU-6 dependency: catalog-derived row and cell policies — in progress

- `load_catalog_policies` resolves applicable family-13 definitions and family-14 user/role/column targets from the pinned snapshot. It enforces row/cell target shape, operation clause requirements, recipient kinds, and schema column membership; it schema-binds USING/WITH CHECK expressions to the unique catalog BOOLEAN type and exact field/role dependency IDs. Policy, target, recipient, type, column, and expression records are guarded before the context is returned.
- Catalog commit now requires an existing policy's authority epoch to advance when its definition or targets change, and requires the owning existing table's write epoch to advance for any policy change. It rejects deleting a policy while target rows remain. A real-redb case checks a loaded row SELECT policy and rejects adding a target without the required epochs.
- A low-level catalog/audit test previously used an orphan policy-target row. It now uses a dependency record so the fixture remains valid under policy-target referential checks.
- The same real-redb fixture now also loads transitive role membership, a role-granted column INSERT privilege, and role policy recipients alongside direct user grants/targets.
- The catalog-loaded projection fixture combines a row ALLOW, cell ALLOW, and cell RESTRICT policy; `project_row` confirms the restricted cell stays redacted while the row remains visible. It also loads a transitive role, role-granted INSERT, and role policy recipient.
- Remaining: startup integrity validation, authenticated DDL/control IPC, gateway/sandbox integration, and SM-6/SM-7 acceptance remain open.
- Verification after the policy loader/epoch changes: all 89 tests across 16 integration targets pass via direct Rust 1.95 compilation against cached redb 4.4.0. Direct Clippy with warnings denied, rustfmt `--check`, and `git diff --check` pass. `cargo test --workspace --locked` is still blocked before compilation by the unreachable crates.io index configuration.

## DD-1 / SM-5 dependency: constraint-definition commit validation — in progress

- Contract: [catalog payloads](../../specs/catalog-payloads.md) constraint kind invariants and [data dictionary](../../specs/data-dictionary.md) atomic catalog validation. A malformed FOREIGN KEY record with a nonempty source list and empty referenced list was accepted before the change (regression assertion returned `Ok(1)`).
- Catalog commit now validates kind-specific shape for changed constraints: NOT NULL, PRIMARY KEY, UNIQUE, CHECK, and FOREIGN KEY each require/forbid the corresponding expression, reference, backing index, column lists, action/match values, and VALID state. The catalog table write epoch fence remains required for existing tables.
- Regression: `engine/tests/commit.rs::catalog_commit_rejects_malformed_constraint_semantics` now proves malformed FK publication is rejected without advancing the commit sequence. Existing real-redb CHECK loader fixture covers a valid CHECK record.
- Limits: this increment does not yet resolve column ownership, referenced key/index definitions, CHECK dependency validity, or implement PK/UNIQUE/FK enforcement and delete/update cascades. Those remain required for DDL acceptance.
- Follow-up reference validation now resolves changed constraints against the transaction's final catalog view. It requires the owning table and local columns, checks NOT NULL against column nullability, validates CHECK expression purpose, verifies PK/UNIQUE backing-index table/columns/uniqueness/ownership/state, and confirms FK target constraint kind and ordered referenced columns. A structurally valid CHECK against a missing table also failed before implementation (`Ok(1)`) and is now rejected atomically.
- Invocation constraint loading now admits valid NOT NULL records when they name exactly one existing non-nullable table column; the real-redb fixture contains this case alongside CHECK and proves the combined catalog loads.
- Verification after both regression cases: 91 tests across 16 real-redb integration targets passed with direct Rust 1.95 compilation against cached redb 4.4.0. Direct Clippy with `-D warnings`, rustfmt `--check`, and `git diff --check` pass.
- Limits: CHECK dependency binding still occurs on invocation load, not all DDL publication paths. Constraint updates/deletions must preserve dependent constraints and indexes; PK/UNIQUE/FK runtime maintenance and FK delete/update behavior are not yet complete. DDL authorization remains unimplemented.
- Environment: direct Rust 1.95 compilation against cached redb 4.4.0 is available. `cargo test --workspace --locked` fails before compilation because crates.io `config.json` cannot be fetched through the configured proxy. TinyCC is not installed/on PATH, so C SDK and sandbox artifact acceptance cannot yet be run. These are validation/toolchain blockers, not design questions.
- Verification after constraint references and NOT NULL loading: 91 tests across 16 real-redb integration targets passed via direct Rust 1.95 compilation against cached redb 4.4.0. Direct Clippy with `-D warnings`, rustfmt `--check`, and `git diff --check` pass.
- Next: startup/bootstrap integrity validation, then an authenticated DDL control path.

## SM-5: record-level startup integrity — in progress

- Contract: [catalog lifecycle](../../specs/catalog-lifecycle.md), Startup and Recovery, [catalog payloads](../../specs/catalog-payloads.md), [data dictionary](../../specs/data-dictionary.md), [redb storage](../../specs/redb-storage.md), Database Layout and Validation, and [index-key encoding](../../specs/index-key-encoding.md), Complete Keys and Values. `validate_persisted_records` validates all required engine tables and the single `format` metadata record within one pinned redb snapshot, requires supported formats and READY lifecycle, bounds each catalog family at 65,536 records, and checks CAT1/EIR1 framing, RWR1 keys/versions, index definitions, bidirectional `indexes_v1`/`row_indexes_v1` membership, exact row/locator version agreement, OUT1 key/frontier consistency, and AUD1 key identity. It validates that exactly one database bootstrap record identifies the engine metadata UUID and repeats its catalog, expression, and built-in semantics versions. The mapping must contain exactly the 30 required sorted codes, with unique nonzero IDs and zero reserved bytes. Codes 1 and 2 resolve to schema records and kind-1 objects; code 3 resolves to the `database_admin` role principal and kind-9 object; codes 101–111 resolve to primitive type records with matching `builtin_kind` and kind-2 objects. Codes 201–216 remain opaque storage-object IDs pending a catalog target/ownership contract. Dependency records resolve their dependent endpoint to a family 2 object; referenced endpoints also resolve there except kind-5 database-scoped system-grant edges, which may reference the family 1 database ID. Kind-1 owned-child edges exactly match parent IDs and the parent graph is acyclic. Kind-2 edges exactly match column default and add-column-default, CHECK, policy-clause, and expression `dependency_ids` references, with matching typed expression/column/role endpoints and no duplicate expression dependencies. Kind-3 edges exactly match FK `referenced_constraint_id` values and connect FK constraints to PRIMARY KEY or UNIQUE constraints. Kind-4 edges exactly match PK/UNIQUE backing-index IDs and index owning-constraint IDs. Kind-5 edges exactly match membership, direct-grant, and policy-recipient authority pairs; grantors resolve to principals, and system grants target the database record. Policy column targets require a cell policy, a matching family 6 column attached to its family 5 table, and the expected object kinds. Constraint tables and source columns resolve to matching typed records/object kinds, source columns belong to the constraint table, and NOT NULL names a non-nullable column. Indexes resolve their table and ordered columns to matching typed records/object kinds, with every column attached to the indexed table. Unknown edge kinds fail closed, and catalog commits apply these rules to the final object, typed-record, constraint, index, and dependency sets. Every object owner resolves to a family 10 user/role; commit batches changing objects or principals validate all owners in the final catalog. Typed records from families 3–9 and 13 must match an objects row's ID and kind, family 10 principals must map user/role to object kinds 8/9, and column/principal normalized names must match their objects rows. Object kinds must be in 1–10. Startup and catalog commits now require a complete one-to-one mapping between family 2 objects and typed rows; commits revalidate the final mapping for every affected ID across families 3–10 and 13, including applicable name agreement. It validates principal authority epochs/login state and membership grantor/member/role endpoints, epochs, edge bounds, and full-graph cycle freedom.
- `row_indexes_v1` stores a reverse membership key formed by the 32-byte row key followed by the index key without its duplicate row UUID suffix. This lets commit refresh unchanged index locators in time proportional to the indexes on the changed rows; deleting a row removes all its index memberships atomically. Explicit index additions/removals update both tables in one redb transaction.
- Red evidence: startup tests first failed against the placeholder validator, and dedicated regressions then showed the validator accepted a locator older than its row, row commits left that locator unchanged, startup accepted cyclic/user-target membership graphs, READY startup accepted a missing database bootstrap descriptor, dependency edges could name absent objects, typed index/principal rows could lack or disagree with their generic object record, owned-child edges could be absent, disagree with object parent IDs, or form parent cycles, objects could name missing owner principals, column/principal names could disagree with object names, FK target edges could be omitted or point at a different constraint, PK backing-index edges could be omitted or point at a different index, kind-2 edges could be omitted or point at the wrong expression, and kind-5 edges could be missing or extra for membership, grants, or policy recipients; policy column targets could name missing columns, columns on another table, or columns on row policies; startup accepted constraints with missing tables/source columns, cross-table columns, or nullable NOT NULL columns; and startup/commit accepted indexes with missing or cross-table columns; startup also accepted an FK whose referenced column differed from its target key. Startup and commit accepted syntactically valid credential verifier records while no algorithm profile was defined. Commit regressions also showed dangling dependencies, missing typed object mappings, FK definitions without kind-3 dependency edges, backed PK/UNIQUE definitions without kind-4 edges, and column/expression records without kind-2 edges were accepted before validation, including a schema row with no object. Startup accepted missing mapped schema/type targets, a primitive target with the wrong `builtin_kind`, a mapped role renamed together with its object, and an extra family 2 object without a typed row; commit accepted deletion of a mapped primitive type, a paired role/object rename, and creation of an object without its typed row. The 36 startup tests, catalog dependency, owner-reference, owned-child, typed-object, name-agreement, FK-target, backing-index, expression-reference, authority-edge, index-reference, bootstrap-target commit regressions, and retained-index update/delete test now pass against real redb. Cases cover consistent reopen state, bootstrap identity/version/mapping and target corruption, missing dependency endpoints, same-transaction endpoint creation, rejection of endpoint deletion with a surviving edge, typed-object identity/kind checks across all typed families, non-READY metadata, malformed key/value identity, missing definitions/rows/reverse membership, invalid principal/membership graph shape, zero table ID, and records ahead of the commit frontier.
- Verification after bootstrap-target and two-way object/typed mapping checks: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed in the prescribed Nix shell using Rust 1.95.0 and redb 4.4.0; all 163 integration tests across 17 targets passed. Changed Markdown relative links and `git diff --check` also pass. TinyCC is available in the shell, but SDK and sandbox harness sources do not exist, so C/query acceptance is not yet runnable.
- Limits: startup and catalog commits validate mapped schema, role, and primitive-type targets; codes 201–216 still have no catalog target records whose ownership can be checked. Historical tuple adaptation, supported verifier-profile issuance, exclusive instance locking, and authenticated startup remain open. Index key derivation from changed typed row values and complete catalog-derived index delta validation remain unimplemented. No startup result should enable service admission until the remaining lifecycle and authentication checks are implemented.
- Next: derive typed index keys from row values and validate catalog-derived index deltas. Credential issuance remains gated until an authentication profile assigns supported verifier algorithms and encodings.

## SM-5 dependency: built-in type profile integrity — in progress

- Startup now requires one family-4 record per built-in kind, with the assigned parameter rule, scalar encoding, comparison version, default mask, and cast profile. Catalog commits validate every changed type profile, enforce unique built-in kinds across the final family-4 set, and verify mapped type rows still match their bootstrap codes.
- Red evidence: startup accepted individually altered parameter, encoding, comparison, default-mask, and cast-profile fields; commit accepted a wrong cast profile and two rows claiming the same built-in kind. Regression tests now reject each case.
- The catalog-schema guard fixture now uses canonical profiles and advances the table schema version when changing a column parameter, preserving the catalog mutation and stale-snapshot conflict case.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed in the prescribed Nix shell with Rust 1.95.0 and TinyCC 0.9.27-unstable-2025-01-06; all 166 integration tests across 17 targets passed. `git diff --check` and changed Markdown relative links passed.
- Remaining: typed row payload/schema agreement, verifier-profile issuance, exclusive instance locking, authenticated startup, catalog-derived index deltas, storage-object ownership for bootstrap codes 201–216, and the service admission lifecycle remain open.
- Next: validate typed row payload/schema agreement.

## SM-5 dependency: persisted tuple/schema agreement — in progress

- Startup now resolves persisted family-5 table versions and ordered family-6 columns to their family-4 built-in type profiles. It validates column ordinals, introduction versions, and type parameters, then decodes every RWR1 payload as TUP1 against that schema. Missing table/type definitions and invalid catalog schemas fail closed; malformed tuples, column-count or schema-version mismatches, scalar errors, and invalid NULLs return `InvalidRow`.
- Red evidence: a READY database containing an RWR1 record with arbitrary non-TUP1 bytes was accepted. Regressions now cover malformed tuple bytes, mismatched table schema version, column-count mismatch, NULL in a non-nullable field, and a row whose table has no catalog schema.
- Shared the catalog-to-scalar-type decoder between startup validation and invocation schema loading. Updated the recovery fixture to use a real built-in BYTEA column and TUP1 values.
- Spec decision: persisted TUP1 rows must match the table's current schema version exactly. Historical tuple adaptation is unsupported and remains a prerequisite for schema-changing DDL over existing rows.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed in the prescribed Nix shell with Rust 1.95.0 and TinyCC 0.9.27-unstable-2025-01-06; all 168 integration tests across 17 targets passed. `git diff --check` and changed Markdown relative links passed.
- Remaining: historical tuple adaptation, commit-time schema enforcement for low-level row APIs, typed index-key derivation/delta validation, bootstrap storage-object semantics, verifier-profile issuance, exclusive instance locking, authenticated startup, and service admission remain open.
- Next: derive typed index keys from row values and validate catalog-derived index deltas.

## SM-5 dependency: typed row to catalog index key — in progress

- Contract: [index-key encoding](../../specs/index-key-encoding.md), Complete Keys and Values. The derivation primitive accepts ordered opaque 16-byte schema IDs/types, a schema version and TUP1 payload, and an ordered 16-byte index column-ID list. It rejects zero/duplicate/misaligned schema IDs, empty/zero/duplicate/missing index IDs, and invalid TUP1 data; it projects only the indexed fields in catalog order and returns the complete index-ID/tuple/row-ID key plus an indexed-NULL flag.
- Red evidence: the two new regressions failed against the compiling placeholder with `InvalidDefinition`. They now cover non-leading and reordered projected columns, embedded-zero text, NULL projection, invalid definitions, malformed TUP1 length, and schema-version mismatch.
- Verification: the prescribed Nix shell provided Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06. Workspace build, formatting check, Clippy with warnings denied, and all 170 integration tests across 17 targets passed.
- Catalog-ID correction: switched the schema/index column-ID interface to opaque 16-byte IDs, matching catalog encoding. The focused `index_key` target (10 tests), formatting, and whitespace checks pass after that API correction.
- Limits: this is a derivation primitive only. Transaction commit still accepts caller-supplied index deltas; it does not yet prove complete catalog-derived membership changes, enforce uniqueness against the final row set, or prevent index-definition changes from publishing inconsistent memberships.
- Next: use the pinned schema/index definitions to derive and enforce complete index deltas for changed rows.

## SM-5 dependency: pinned catalog index definitions — in progress

- Added `load_catalog_indexes` to resolve family-9 indexes against a catalog-loaded table schema. It checks method, uniqueness encoding, NULLS DISTINCT rule, storage ID, VALID state, key version, schema version, and nonempty ordered distinct column IDs belonging to the same table. Each matching index record and the schema/type records are guarded for final commit.
- The real-redb schema loader regression checks a valid unique index, verifies a foreign-table column fails closed, and changes the index record after loading to confirm the stale invocation conflicts.
- Verification: workspace build, format check, Clippy with warnings denied, and all 170 integration tests across 17 targets passed in the prescribed Nix shell. The changed Markdown relative links and `git diff --check` pass.
- Limitation at that step: the lower-level explicit-write API still did not derive complete membership changes; see the following increment for the catalog-loaded invocation gate.

## SM-5 dependency: catalog-derived index delta validation — in progress

- Red evidence: a catalog-loaded typed row insert with three valid indexes and no index writes committed before the change. The regression now expects rejection and confirms no row is published. Additional cases reject a row write after schema loading without index loading, and reject a forged key.
- Invocation finalization now derives the expected old/new key and NULL state for every index loaded from the pinned catalog. It requires the staged mutations to match those keys, row IDs, expected locator versions, uniqueness flags, and NULL-exemption flags. Existing locator versions must match the row version; an already-present key cannot be silently overwritten as a new membership. Catalog schema/index guards are still checked by the serialized commit.
- Real-redb coverage now exercises all three catalog indexes through insertion, indexed-value update, and deletion, and verifies locator versions. A catalog-backed authorized INSERT also advances the index schema versions and publishes its index memberships atomically.
- Verification: prescribed Nix-shell workspace build, formatting check, Clippy with warnings denied, and all 170 integration tests across 17 targets passed. Changed Markdown relative links and `git diff --check` pass.
- Limitation: callers still stage the exact derived `IndexWrite`s after loading schema/index definitions. Index membership is not yet synthesized automatically from the row overlay. Low-level explicit-write commit helpers and projection-only test paths remain trusted scaffolding, not executor-facing paths.
- Next: synthesize index deltas during invocation finalization and make catalog-backed row mutation APIs independent of caller-supplied index writes.

## SM-5 dependency: automatic catalog index maintenance — in progress

- Invocation finalization now synthesizes index additions and removals from the pinned catalog schema/index definitions and the prior/final typed row. Optional caller-staged index writes are accepted only when they exactly match the derived mutations; omitted changes are generated automatically. Unchanged indexed values continue to receive refreshed locators in the serialized storage commit.
- Generated writes are checked against the invocation's index-change and key-byte budgets. A budget failure aborts the invocation without publishing the row.
- Real-redb coverage exercises automatic insert, indexed-value update, and delete maintenance, verifies locators, rejects missing index definitions and forged writes, and checks atomic rejection when synthesized writes exceed the budget.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and all 170 integration tests across 17 targets passed in the prescribed Nix shell. `git diff --check` passed.
- Remaining: the low-level explicit-write API and projection-only helpers remain trusted scaffolding. Direct low-level row commit helpers do not yet enforce the catalog schema. Index-definition DDL/rebuild behavior, uniqueness validation beyond the current commit path, and service integration remain open.

## SM-5 dependency: schema-loaded tuple commit validation — in progress

- Red evidence: after removing all secondary index definitions from a fixture table, an invocation could load its schema and finalize an arbitrary byte payload because no index derivation decoded the row.
- Finalization now decodes every inserted or updated row against the pinned catalog schema even when the table has zero secondary indexes. It also validates an existing row's tuple before using it as the prior image. Malformed tuples abort with `InvalidRow` and persist ABORTED without publishing the row.
- A real-redb regression removes the table's index definitions, loads its schema, stages a non-TUP1 payload, and verifies the row remains absent with a durable ABORTED outcome.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and all 171 integration tests across 17 targets passed in the prescribed Nix shell. `git diff --check` passed.
- Remaining: the low-level direct row commit helpers still lack catalog-schema validation and remain trusted-only; historical tuple adaptation and service admission remain open.

## SM-5 dependency: fail-closed index definition changes — in progress

- Red evidence: catalog commit could publish a new index definition for a table that already had rows, without building the corresponding memberships, which could silently produce incomplete indexed query results.
- Catalog commit now rejects adding, changing, or dropping an index definition while its current/final table has persisted rows, the same commit inserts rows for the table, or the index has stored memberships. Changes remain allowed for empty tables until transactional backfill/rebuild exists.
- A real-redb regression commits a row and its existing index membership, attempts to add a second index, and confirms the DDL aborts without changing the catalog, audit, or commit sequence.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and all 172 integration tests across 17 targets passed in the prescribed Nix shell. `git diff --check` passed.
- Remaining: transactional index backfill/rebuild and online schema migration remain open.

## SM-5 dependency: startup index membership coverage — in progress

- Red evidence: startup accepted a valid typed row with a catalog-defined index but no corresponding `indexes_v1` or `row_indexes_v1` membership; it also accepted a stored key whose tuple component disagreed with the row.
- Startup now derives each persisted row's expected key for every catalog index, checks the locator and reverse membership, and verifies every stored index key equals the derivation. Missing memberships, mismatched keys, and extra entries fail with `InvalidIndex`.
- Real-redb regressions cover missing index coverage and a locator/reverse entry attached to the wrong typed key; a valid persisted row/index image still passes.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and all 173 integration tests across 17 targets passed in the prescribed Nix shell. `git diff --check` passed.
- Remaining: startup validation still needs uniqueness checking for persisted unique-index duplicates and authenticated service admission.

## SM-5 dependency: persisted unique-index integrity — in progress

- Red evidence: startup accepted two rows with the same non-NULL tuple in a catalog-defined unique index, even though invocation commits enforce uniqueness.
- Startup now validates the catalog uniqueness flag, tracks derived tuple prefixes for unique indexes, and rejects duplicate non-NULL keys. NULL values follow the catalog's NULLS DISTINCT rule and do not collide.
- Real-redb coverage rejects duplicate persisted non-NULL keys and accepts repeated NULL keys under a unique index.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and all 175 integration tests across 17 targets passed in the prescribed Nix shell. `git diff --check` and changed Markdown link checks passed.
- Remaining: authenticated service admission and foreign-key constraint lifecycle integration remain open.

## SM-5 dependency: PRIMARY KEY and UNIQUE write enforcement — in progress

- Red evidence: catalog-backed invocation loading rejected every PRIMARY KEY/UNIQUE constraint as an unsupported kind, although commit already enforces uniqueness through the corresponding unique index.
- Invocation constraint loading now validates each key constraint's ordered source columns and owned backing index, including table, key shape, owner, uniqueness flag, state, and schema version. PRIMARY KEY columns must also be non-nullable. CHECK expressions load the BOOLEAN profile only when needed.
- A real-redb regression loads a catalog UNIQUE constraint and its owned unique index, stages duplicate tuples, and confirms the whole invocation aborts without publishing either row.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and all 176 integration tests across 17 targets passed in the prescribed Nix shell. `git diff --check` and changed Markdown link checks passed.
- Remaining: foreign-key runtime enforcement and authenticated service admission remain open.

## SM-5 dependency: primary-key catalog integrity — in progress

- Red evidence: catalog commit and READY startup accepted a PRIMARY KEY on a nullable source column despite its valid owned unique backing index.
- Catalog commit and startup validation now reject nullable PRIMARY KEY columns. Startup also verifies that each PRIMARY KEY/UNIQUE backing index is unique and matches the ordered source columns.
- Real-redb regressions construct coherent catalog object, constraint, dependency, and backing-index records, then reject the invalid primary key at commit and startup.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and all 178 integration tests across 17 targets passed in the prescribed Nix shell. `git diff --check` and changed Markdown link checks passed.
- Remaining: foreign-key runtime enforcement and authenticated service admission remain open.

## SM-5 dependency: catalog-backed foreign-key enforcement — in progress

- Added invocation loading for valid catalog FOREIGN KEY definitions and their referenced PRIMARY KEY/UNIQUE constraints, backing indexes, source/target schemas, ordered columns, and exact built-in scalar types/parameters. All definitions used by the invocation are guarded for commit.
- The serialized commit checks final source and target rows after applying the staged write set logically, enforcing MATCH SIMPLE, ON UPDATE RESTRICT, and ON DELETE RESTRICT. It checks both child and parent changes, so concurrent child insertion and parent deletion cannot both commit. Combined persisted and staged row visits are capped at 65,536 per commit; exceeding the cap aborts.
- Real-redb regressions cover dangling child rejection, matching child insert, NULL exemption, restricted parent delete, atomic child-plus-parent delete, a barrier-synchronized child-insert/parent-delete race, and exceeding the row-visit budget with an ABORTED outcome and no row publication.
- Verification: with Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06 in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all 183 integration tests across 17 targets passed. Changed Markdown local links and `git diff --check` pass.
- The change includes Rust implementation/tests and updates to [schema DDL](../../specs/ddl-schema.md) and [concurrency/MVCC](../../specs/concurrency-mvcc.md).
- Remaining: authenticated service admission, catalog DDL creation/fencing for foreign keys, and an indexed validation path for larger commit-time scans remain open.

## SM-5 dependency: persisted foreign-key startup validation — in progress

- Startup now validates FK action/match/state fields, the referenced unique backing index, ordered source/target columns, and exact built-in scalar types and parameters. It accepts at most 256 FK constraints.
- After validating index membership, startup checks each non-NULL child tuple against the referenced unique index; NULL values pass under MATCH SIMPLE. Combined child-row visits are bounded at 65,536, and exceeding the bound rejects startup.
- Real-redb startup coverage accepts matching child/parent rows, rejects a dangling child, accepts NULL under MATCH SIMPLE, rejects mismatched source/target types, and rejects a database over the row-visit limit.
- Verification: with Rust/Cargo 1.95.0, rustfmt 1.9.0, and Clippy 0.1.95 in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all 183 integration tests across 17 targets passed. Changed Markdown local links and `git diff --check` pass.
- The implementation uses the existing unique backing index to validate references without materializing the full parent key set.
- Remaining: foreign-key DDL creation/fencing and authenticated service admission remain open.

## SM-5 dependency: bounded foreign-key comparison memory — in progress

- The commit-time validator now caps the distinct encoded comparison keys retained for each FK check at 16 MiB, in addition to the 65,536 combined row-visit limit. The transaction aborts with the same generic FK validation limit error before publication when either limit is exceeded.
- A real-redb regression with 128 distinct 70 KiB parent/child keys exceeds the comparison-key budget and confirms the child row version and commit sequence remain unchanged.
- Verification: with Rust/Cargo 1.95.0, rustfmt 1.9.0, and Clippy 0.1.95 in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all 184 integration tests across 17 targets passed. Changed Markdown local links and `git diff --check` pass.
- Remaining: FK DDL creation/fencing and authenticated service admission remain open.

## SM-5 dependency: catalog-commit foreign-key type compatibility — in progress

- Final-catalog constraint validation now rejects an FK whose ordered source and referenced columns have different built-in type IDs or parameter bytes. It applies to catalog commits before publication, matching invocation loading and startup checks.
- A real-redb regression submits otherwise coherent FK, PRIMARY KEY, UNIQUE index, and dependency records with mismatched source/target types; catalog commit rejects the change and leaves the commit sequence unchanged.
- Verification: with Rust/Cargo 1.95.0, rustfmt 1.9.0, and Clippy 0.1.95 in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all 184 integration tests across 17 targets passed.
- Remaining: validating pre-existing child rows when creating an FK, FK DDL fencing/lifecycle, and authenticated service admission remain open.

## SM-5 dependency: catalog-commit foreign-key creation validation — in progress

- Catalog commits now load the final source and referenced table schemas for changed FK definitions and validate existing non-NULL child rows against the latest target rows plus any row changes in the same serialized commit. The existing 65,536 combined row-visit and 16 MiB per-FK comparison-key limits apply, and invalid data leaves the constraint unpublished with an ABORTED outcome.
- FK definition changes on an existing table require write-epoch advances on both the referencing and referenced tables. Catalog commits also preserve the 256 live-FK limit enforced by invocation loading and startup.
- Real-redb coverage rejects adding an FK over an orphan child, accepts matching pre-existing parent/child rows, and confirms missing the referenced-table epoch advance aborts publication.
- Verification: with Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06 in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all 186 integration tests across 17 targets passed. `git diff --check` and changed Markdown link checks passed.
- Contract updates: [schema DDL](../../specs/ddl-schema.md) defines validation of existing rows on FK creation; [data dictionary](../../specs/data-dictionary.md) requires epoch publication for both related tables.
- Remaining: a separate per-table DDL fence manager, online validation/backfill, and authenticated service admission remain open.

## SM-5 dependency: per-table DDL fence primitive — in progress

- Added a clonable `DdlFenceManager` with shared guards for data writers and exclusive guards for DDL. Multi-table requests are sorted, deduplicated, and acquired all at once; waiting DDL blocks new writers on affected tables. Deadline failures release all waiter state, and RAII guards release acquired fences.
- Red evidence: the first focused run had two failures because the placeholder admitted exclusive DDL alongside active writers; both exclusion regressions now pass.
- Integration tests cover existing-writer exclusion, DDL-writer exclusion, multi-table timeout cleanup, duplicate IDs, shared-writer coexistence, zero-ID rejection, and empty fence sets.
- Verification: with Rust/Cargo 1.95.0 and Clippy 0.1.95 in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all 189 integration tests across 18 targets passed. `git diff --check` and changed Markdown link checks passed.
- The implementation refines the locking decision in [schema DDL](../../specs/ddl-schema.md).
- Added `StorageManager` entry points that route invocation commits through shared table fences and catalog commits through exclusive fences derived from changed tables, constraints, indexes, columns, policies, and expression dependents. Fence timeouts durably record an ABORTED outcome. Raw writer helpers remain unfenced trusted primitives.
- Verification after coordinated entry-point integration: with Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06 in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all 191 integration tests across 18 targets passed. `git diff --check` and changed Markdown link checks passed.
- Remaining: production service admission must use `StorageManager` consistently; online validation/backfill and authenticated service admission remain open.

## SM-5 dependency: bounded active invocation snapshots — in progress

- Behavior: reject new runtime admissions once a configured per-manager snapshot cap is reached, including admissions through cloned `StorageManager` handles; release capacity when the transaction drops or ends its snapshot through commit, abort, or expiry. This protects redb from unbounded retained read snapshots.
- Red evidence: the new real-redb regression initially failed because a second snapshot was admitted while the configured limit was one.
- Implementation: `StorageManagerConfig` exposes the DDL-fence timeout and positive active-invocation cap (default 128); atomic admission permits are shared across manager clones and retained by each managed invocation transaction. Failed transaction construction releases its reserved permit through RAII. A zero cap is rejected at configuration time. Low-level transaction constructors remain explicitly outside manager admission.
- Tests: `engine/tests/commit.rs` verifies zero-limit rejection, cross-clone capacity enforcement, permit release immediately after abort while the transaction object remains alive, and subsequent admission.
- Verification: with Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06 in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all 192 integration tests across 18 targets passed. `git diff --check` and changed Markdown local-link checks passed.
- Remaining: bounds for scans, response bytes, and total work still require service lifecycle integration.

## SM-5 dependency: validated StorageManager startup — in progress

- Behavior: do not expose invocation admission from a runtime manager until the recovered image passes the engine's full implemented startup integrity validation and is READY.
- Red evidence: the new real-redb regression inserted a malformed row envelope into an otherwise READY database; initially, constructing a StorageManager still succeeded.
- Implementation: `StorageManager::new` and `with_config` now return a typed initialization error and call `validate_persisted_records` before constructing fence/admission state. The check covers table definitions, bootstrap/catalog, typed rows, indexes, constraints, outcomes, and audit, but it is not client authentication or service identity validation.
- Test: `engine/tests/commit.rs` verifies invalid row persistence rejects manager construction with `StartupError::InvalidRow`; the existing manager routing test verifies a valid recovered image is accepted.
- Verification: with Rust/Cargo 1.95.0, rustfmt 1.9.0, and Clippy 0.1.95 in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all 193 integration tests across 18 targets passed. `git diff --check` and changed Markdown local-link checks passed.
- Remaining: authenticated service admission and lifecycle cancellation remain separate service work.

## SM-5 dependency: managed invocation age limits — in progress

- Behavior: bound managed invocation lifetime at snapshot reads, staged-write checks, and commit; allow the supervisor/service to poll idle invocations and cancel expired work.
- Red evidence: with the deadline check temporarily disabled, the real-redb expiry regression failed because `check_deadline()` returned success after the configured maximum age.
- Implementation: configuration defaults to a 15-minute maximum age and rejects zero. A managed invocation checks age when it uses its pinned snapshot, when it stages against a cached base version, and before and after waiting for a shared DDL fence. Expiry durably records ABORTED, drops the retained redb snapshot, and releases the admission slot immediately. `InvocationTransaction::check_deadline` supports service-side polling during idle periods.
- Tests: `engine/tests/commit.rs` checks idle polling expiry, the durable ABORTED outcome, immediate snapshot-slot reuse while the expired transaction object remains alive, and commit-time expiry.
- Verification: with Rust/Cargo 1.95.0, rustfmt 1.9.0, and Clippy 0.1.95 in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all 194 integration tests across 18 targets passed. `git diff --check` and changed Markdown local-link checks passed.
- Remaining: no supervisor exists yet to poll idle transactions automatically; scan, result, and total-work budgets also remain service lifecycle work.

## SM-5 dependency: owner-scoped outcome lookup — in progress

- Behavior: provide the trusted service layer a durable outcome query for resolving lost commit replies, exposing only unexpired outcomes belonging to the already authenticated owner.
- Implementation: `StorageManager::lookup_invocation_outcome` delegates to the canonical outcome reader, which maps unknown IDs, owner mismatches, and expired records to absence. The method accepts an authenticated principal ID from its trusted caller; it does not authenticate network clients or grant administrator visibility. The manager no longer exposes a raw redb handle accessor.
- Test: `engine/tests/commit.rs` verifies ABORTED and COMMITTED lookup, owner isolation, expiry, and unknown invocation behavior through `StorageManager`.
- Verification: with Rust/Cargo 1.95.0, rustfmt 1.9.0, and Clippy 0.1.95 in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all 195 integration tests across 18 targets passed. `git diff --check` and changed Markdown local-link checks passed.
- Remaining: the supervisor/gateway authentication path is not implemented; administrator outcome visibility remains separate.

## SM-5 dependency: bounded existing-row index creation — in progress

- Coordinated catalog DDL now builds new index memberships from a pinned redb read snapshot while holding the table's exclusive DDL fence, without holding the serialized writer during the scan. It rechecks all scanned row keys and versions in the write transaction, then publishes forward/reverse memberships, catalog definition, audit, sequence, and outcome atomically.
- Builds are limited to 65,536 rows, 65,536 generated memberships, and 64 MiB combined stored-row and generated-key bytes per commit. Unique conflicts, malformed rows, stale snapshots, and limit failures leave no catalog or index publication. Combining a populated-table schema change with index creation remains rejected. Trusted unfenced catalog helpers still fail closed for populated-table index changes.
- Real-redb regressions cover successful backfill, atomic rejection of duplicate values for a new unique index, and failure above the row cap; the low-level rejection regression remains.
- Verification: in the prescribed Nix shell, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, Clippy with warnings denied, and all 198 integration tests across 18 targets passed. `git diff --check` and changed Markdown link checks passed.
- Contract updates: [schema DDL](../../specs/ddl-schema.md) and [redb storage](../../specs/redb-storage.md).
- Remaining: action-specific authorization for other DDL and service admission remain open.

## SM-5 dependency: populated index drop — in progress

- Coordinated DDL now scans a dropped index's physical memberships under the table's exclusive fence, validates each locator against its row version and matching reverse entry, and rechecks the complete index prefix before deletion. Forward/reverse memberships and catalog/object removal publish in one commit; pinned readers keep the pre-drop redb state.
- Removal shares the 65,536-membership and 64 MiB index-key/locator work bounds. Unique/constraint-owned indexes remain subject to final catalog dependency checks. Unfenced low-level catalog commits still reject drops when rows or memberships remain.
- The real-redb lifecycle regression creates an index over an existing row, proves the trusted low-level helper rejects dropping it, then drops it through `StorageManager`; the old snapshot still resolves the old membership and startup accepts the new image.
- Verification: in the prescribed Nix shell, workspace build, format check, Clippy with warnings denied, and all 198 integration tests across 18 targets passed. `git diff --check` and changed Markdown link checks passed.
- Remaining: action-specific authorization for other DDL and service admission remain open.

## SM-5 dependency: populated index rebuild

- Coordinated DDL can update an existing index definition on a populated table under its exclusive table fence. It derives old and new keys from a pinned row snapshot, validates every existing forward locator and matching reverse entry, and rechecks row keys/versions and the complete old index prefix under the serialized writer.
- Changed memberships are removed and inserted in one durable commit with the updated catalog definition, audit event, sequence, and outcome. Unchanged keys retain their row versions. New unique definitions are checked against each other and the current index contents; duplicate values abort without changing the catalog or membership set. Rebuilds share the 65,536-row, 65,536-membership, and 64 MiB DDL bounds. Combining a populated-table schema change with index creation or rebuild remains unsupported.
- Real-redb regressions cover a successful populated-index re-key and nonunique-to-unique change with an old pinned reader, rejection by the unfenced low-level helper, startup validation after publication, and rejection of a unique rebuild with duplicate values. The existing populated-index drop and create regressions remain in place.
- Verification: with Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06 in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all workspace tests passed. `git diff --check` and changed Markdown local-link checks passed.
- Contract updates: [schema DDL](../../specs/ddl-schema.md) and [redb storage](../../specs/redb-storage.md).
- Remaining: action-specific authorization for other DDL and service admission remain open.

## SM-5 dependency: ordinary index DDL authorization — in progress

- `StorageManager::commit_authorized_index_ddl` is a narrow route for an actor already authenticated by a trusted administrative caller. It accepts only index-definition and matching index-object changes, verifies the actor is a live login-enabled user, then checks database-administrator membership, table ownership through the inherited role closure, or an `INDEX` grant on every affected table.
- The route carries the actor, complete membership graph, role records, applicable grants, and table-owner records into the serialized commit as catalog guards. Revoked grants, changed memberships, disabled users, or transferred ownership invalidate a stale authorization. The generic coordinated catalog helper remains trusted-only and has no authorization check.
- A real-redb integration test covers a direct `INDEX` grant, table ownership, inherited membership in the reserved administrator role, denial without authority, rejection of an index-object owner transfer through this route, durable ABORTED outcomes, and absence of catalog/audit publication on denial.
- Verification: in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and all workspace tests passed. `git diff --check` and changed Markdown local-link checks passed.
- Contract update: [schema DDL](../../specs/ddl-schema.md) and [redb storage](../../specs/redb-storage.md).
- Remaining: the administrative channel must authenticate the actor before calling this route; deterministic authority-change races, authorization rules for constraints/policies/identity/grants, and token-backed gateway/supervisor admission remain open.

## SM-5 dependency: managed user admission — in progress

- `StorageManager::begin_authenticated_invocation` accepts an identity from a trusted caller, validates that the principal is a live `LOGIN` user in the invocation's pinned catalog snapshot, and returns the pinned role closure. Identity and membership records used for admission are guarded through commit. Unknown and disabled users are denied with durable ABORTED outcomes.
- Token verification and expiry/revocation checks are now handled by `begin_token_authenticated_invocation`; the caller-supplied-identity method remains available only after trusted external authentication. The gateway/supervisor authentication boundary remains outside the storage manager, and `begin_invocation` remains a lower-level trusted path.
- A real-redb integration test covers successful admission, disabled and unknown-user denial, durable outcomes, and release of the admission slot after failed attempts.
- Verification: in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and workspace tests passed. Changed Markdown links and `git diff --check` passed.
- Contract update: [users and roles](../../specs/ddl-identity.md).
- Remaining: token issuance/inventory APIs, gateway/supervisor authentication, and action-specific authorization for other DDL remain open.

## SM-5 dependency: role-membership DDL authorization — in progress

- `StorageManager::commit_authorized_membership_ddl` accepts only membership edges, their exact authority-dependency edges, and affected principals' authority-epoch increments. It authorizes ordinary membership changes through inherited `MANAGE_ROLES` or current administrator membership; changes to the reserved administrator role require a current administrator.
- The route guards the actor identity, complete membership graph, applicable system-grant records, and role records through serialized publication. Added membership edges must name the authenticated actor as grantor, and successful audit events must identify that actor.
- It blocks direct or inherited membership changes that grant or remove database-administrator membership without an administrator, and preserves at least one login-enabled administrator with a current credential.
- A real-redb regression covers authorized membership add/remove, denial without `MANAGE_ROLES`, administrator-only changes to the reserved role, final-administrator preservation, authority epochs, and durable outcomes.
- Verification: in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and workspace tests passed. Changed Markdown links and `git diff --check` passed.
- Contract update: [users and roles](../../specs/ddl-identity.md).
- Remaining: authorization for identity/grant/policy/constraint DDL, token-backed gateway admission, and deterministic authorization-change race coverage remain open.

## SM-5 dependency: system privilege DDL authorization — in progress

- `StorageManager::commit_authorized_system_privilege_ddl` handles only database-scoped `MANAGE_USERS`, `MANAGE_ROLES`, and `CREATE_SCHEMA` grant/revoke records, their authority dependencies, and grantees' authority-epoch updates. It denies system privilege changes unless the actor is a current database administrator.
- The route guards the actor's login state, complete membership graph, role identities, and successful audit actor through serialized publication. New grant records identify the authenticated administrator as grantor.
- A real-redb regression covers administrator grant/revoke, denial for a non-administrator who holds `MANAGE_ROLES`, atomic dependency removal, epoch advances, and durable outcomes.
- Verification: in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and workspace tests passed. Changed Markdown links and `git diff --check` passed.
- Contract update: [users and roles](../../specs/ddl-identity.md).
- Remaining: ordinary object grant authorization, user/role lifecycle DDL, token-backed gateway admission, and deterministic authorization-change race coverage remain open.

## SM-5 dependency: object grant DDL authorization — in progress

- `StorageManager::commit_authorized_object_grant_ddl` accepts ordinary family-12 grant/revoke changes, their authority dependencies, and affected grantees' authority-epoch increments. It excludes database-scoped system privileges.
- Each schema/table target is checked against its current owner, inherited `MANAGE_GRANTS` on that same object, or current database-administrator membership. Actor identity, membership closure, owner records, and applicable `MANAGE_GRANTS` records are guarded through publication; new grants identify the actor as grantor.
- Scope validation permits schema-only `USAGE`/`CREATE`, table or column data grants, table-only `DELETE`, and schema/table administration privileges. Column grants resolve ownership through their table.
- A real-redb regression covers owner and `MANAGE_GRANTS` authority, denial without either, grant/revoke publication, authority-epoch updates, and durable outcomes.
- Verification: in the prescribed Nix shell, workspace build, formatting check, Clippy with warnings denied, and workspace tests passed. Changed Markdown links and `git diff --check` passed.
- Contract update: [permission DDL](../../specs/ddl-permissions.md).
- Remaining: identity lifecycle DDL, policy/constraint authorization, token-backed gateway admission, and deterministic authorization-change race coverage remain open.

## SM-5 dependency: shared principal namespace integrity — in progress

- Startup validation rejects persisted duplicate normalized names across users and roles. Catalog publication checks the complete final principal-name set under commit serialization, applying renames and deletions before additions and bounding the scan to the catalog-family limit.
- Real-redb tests cover rejection of a user name colliding with the reserved role, startup rejection of persisted duplicates, and an atomic rename that frees a name for a newly created role.
- Verification: the prescribed Nix-shell workspace build, formatting check, Clippy with warnings denied, and workspace tests passed. Changed Markdown links and `git diff --check` passed.
- Contract update: [users and roles](../../specs/ddl-identity.md).
- Remaining: durable identity tombstones now enforce ID non-reuse; DROP remains unexposed until authorized RESTRICT dependency cleanup is implemented.

## SM-5 dependency: identity lifecycle DDL authorization — in progress

- `StorageManager::commit_authorized_identity_ddl` now supports user creation, rename, and LOGIN/NOLOGIN changes plus role creation and rename. User changes require inherited `MANAGE_USERS`; role changes require inherited `MANAGE_ROLES`; current database administrators may do either.
- The route accepts complete principal/object creation pairs, matched rename pairs, principal-only login-state changes, and one-way token revocations. Disabling a user with active tokens requires their revocation records in the same batch; active tokens cannot remain attached to a NOLOGIN user. It rejects identity deletion and unrelated catalog writes. Actor identity, membership closure, role records, system grants, and the login-enabled administrator set are guarded through commit; successful DDL audit actors must match the authenticated caller.
- A real-redb regression covers MANAGE_USERS admission, MANAGE_ROLES denial, user rename and disable, rejection of disable without token revocation, atomic disable plus revocation, last-login-admin preservation, database-admin role creation, and committed sequences.
- Verification: the prescribed Nix-shell workspace build, formatting check, Clippy with warnings denied, and workspace tests passed. Changed Markdown links and `git diff --check` passed.
- Contract: [users and roles](../../specs/ddl-identity.md).
- Remaining: role/user deletion still needs authorized RESTRICT dependency cleanup, and deterministic authorization-change race coverage remains open.

## SM-5 dependency: credential verifier profile and token-backed invocation admission — in progress

- Family-16 verifiers use SHA-256 algorithm/version 1 over an exactly 32-byte uniformly random bearer secret. The profile validates verifier width, issue/expiry times, revocation metadata, labels, authority epochs, user ownership, and LOGIN state. This profile is not for human-chosen passwords.
- `StorageManager::begin_token_authenticated_invocation` verifies the secret and trusted current time against the invocation's pinned catalog snapshot, then loads the role closure. The verifier, user, and membership records are guarded through commit; a token revocation after admission makes pending writes conflict. Authentication failures do not distinguish unknown, malformed, wrong-secret, expired, revoked, or disabled-user credentials.
- `StorageManager::issue_token` creates random 32-byte secrets through the system CSPRNG, stores only the SHA-256 verifier, authorizes through `MANAGE_USERS`, records a `TokenLifecycle` audit event, and exposes the secret once through a zeroizing result type. `StorageManager::revoke_token` uses the same authorization route; unknown/already-revoked/unauthorized requests share an error. Identity DDL allows only one-way revocation of existing verifiers and requires all active tokens to be revoked atomically with a NOLOGIN change.
- Tests cover supported and malformed profiles, successful admission, wrong secret, expiry, authorized issue/revoke, one-time secret transfer, denied issuance, durable abort outcomes, concurrent revocation conflict, and the LOGIN disable path.
- Verification: Rust/C toolchain versions matched the configured Nix environment; locked workspace build, formatting check, Clippy with warnings denied, and all 216 tests across 18 integration targets passed. Changed Markdown local-link checks and `git diff --check` passed.
- Remaining: bootstrap secret handoff, gateway/supervisor integration, and authentication rate limits.

## SM-5 dependency: policy DDL authorization — in progress

- `StorageManager::commit_authorized_policy_ddl` authorizes row/cell policy and target changes on existing tables. The actor must own every affected table through its role closure or hold inherited `MANAGE_POLICIES` on the table or schema.
- The route accepts paired policy/target/expression/object/dependency writes, requires table write-epoch changes, checks that surviving policies have recipients, and guards the actor, membership graph, owner/schema records, and applicable grants through publication. Requests are bounded by `MAX_AUTHORIZATION_INPUTS`; audit events must identify the authenticated DDL actor.
- A real-redb regression covers table-owner and inherited schema-level `MANAGE_POLICIES` authorization, policy create/update/drop, denial without authority, empty-recipient rejection, epoch publication, and durable outcomes.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed in the prescribed Nix shell; all 210 tests across 18 test targets passed. Changed Markdown local links and `git diff --check` passed.
- Contract: [permission DDL](../../specs/ddl-permissions.md).
- Remaining: non-foreign-key constraint DDL authorization, durable identity tombstones, authenticated control-channel integration, and deterministic authorization-change race coverage remain open.

## SM-5 dependency: foreign-key DDL authorization — in progress

- `StorageManager::commit_authorized_foreign_key_ddl` accepts add/drop batches for foreign keys on existing tables. Source authority is table ownership or inherited `ALTER` on the table/schema. Creation separately requires inherited `REFERENCES` on the target table or every referenced key column; drop requires only source `ALTER`.
- The route requires paired constraint/object/target-dependency records and write-epoch advances for both source and referenced tables. It guards the authenticated actor, full membership graph, source/target table and schema records, referenced key/column records, and applicable authority grants through publication. Request, membership, column, and grant-lookup work is bounded.
- A real-redb regression covers column-level `REFERENCES` with table `ALTER`, denial when either permission is missing, removal without target `REFERENCES`, durable outcomes, and atomic source/target epoch publication.
- Contracts: [schema DDL](../../specs/ddl-schema.md) and [permission DDL](../../specs/ddl-permissions.md).
- Verification: the prescribed Nix-shell workspace build, formatting check, Clippy with warnings denied, and workspace tests passed; all 211 tests across 18 test targets passed. Changed Markdown local links and `git diff --check` passed.
- Remaining: authorization routes for NOT NULL, CHECK, PRIMARY KEY, and UNIQUE DDL; deterministic authorization-change race coverage; and authenticated control-channel integration remain open.

## SM-5 dependency: bounded NOT NULL validation — in progress

- A catalog commit that changes an existing column from nullable to non-nullable scans persisted rows under the table DDL fence, rejects NULL values, and aborts atomically when the 65,536-row or 64 MiB row-record bound is exceeded. Nullability-only changes preserve the tuple schema version and advance the table write epoch.
- A real-redb regression verifies rejection without publication when a stored row contains NULL, then accepts the DDL after the row is updated and confirms startup validation succeeds.
- Verification: prescribed Nix-shell build, formatting, Clippy with warnings denied, and all 212 workspace tests passed.
- Contract updates: [schema DDL](../../specs/ddl-schema.md) and [catalog payloads](../../specs/catalog-payloads.md).
- Remaining: action-specific CHECK, PRIMARY KEY, and UNIQUE authorization routes remain open.

## SM-5 dependency: NOT NULL, CHECK, PRIMARY KEY, and UNIQUE DDL authorization — in progress

- `StorageManager::commit_authorized_constraint_ddl` accepts paired NOT NULL, CHECK, PRIMARY KEY, and UNIQUE constraint changes on existing tables, with required object/dependency records and a table write-epoch advance. Key constraints include their owned unique backing-index definition, object, and kind-4 edge. It permits the table owner or inherited `ALTER` on the table/schema, verifies a login-enabled actor and successful audit attribution, and guards membership, ownership, and grant inputs through fenced publication. PRIMARY KEY columns must already be non-nullable.
- Red evidence: the managed DDL regression failed with `DdlAuthorizationDenied` for a user holding `ALTER` on the table before the authorized route was connected.
- CHECK additions bind a deterministic row-local Boolean expression to the final schema and capture row keys/versions under the table fence. The serialized writer rechecks those rows before publishing. Backing-index creation/removal uses the existing index planner and atomically publishes the index records, memberships, constraint, audit, sequence, and outcome; the planner permits a paired write-epoch-only table update while continuing to reject a schema change on populated tables.
- A real-redb regression covers denied constraint DDL without sequence publication, successful NOT NULL and CHECK add/drop, rejection of a CHECK that fails on a stored row, authorized UNIQUE and PRIMARY KEY add/drop with owned index backfill, and startup validation after each publication.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed in the prescribed Nix shell. Changed Markdown local-link checks and `git diff --check` passed.
- Contract updates: [schema DDL](../../specs/ddl-schema.md) and [permissions](../../specs/ddl-permissions.md).
- Remaining: deterministic authority-change race coverage and authenticated control-channel integration remain open.

## SM-5 dependency: token inventory — in progress

- `StorageManager::list_tokens` returns stable token IDs, labels, issue/expiry times, and revocation metadata from one redb catalog snapshot. The public metadata type has no verifier or secret field.
- Users can list their own tokens. Cross-user inventory requires current inherited `MANAGE_USERS` or database-administrator membership; missing target users and unauthorized cross-user requests share the same result.
- A real-redb regression covers self-inventory, inherited `MANAGE_USERS`, denial for an unrelated user, and visibility of revocation state after the revoke commit.
- Verification: in the prescribed Nix shell, locked workspace build, formatting check, Clippy with warnings denied, and all 216 workspace tests passed. Changed Markdown local links and `git diff --check` passed.
- Contract updates: [users and roles](../../specs/ddl-identity.md) and [storage manager](../../specs/storage-manager.md).
- Remaining: token request rate limits, bootstrap secret handoff, gateway/supervisor integration, authenticated control-channel integration, and user/role DROP RESTRICT remain open.

## SM-5 dependency: credential-backed last-administrator protection

- Red evidence: an identity DDL regression showed that revoking the only unexpired administrator token committed even though the remaining verifier was expired. The test expected `DdlAuthorizationDenied` and observed `Ok(3)` before implementation.
- The serialized writer now checks final membership, principal login state, and family-16 verifier state together. A usable administrator must be in the reserved database-admin closure, have `LOGIN`, and have at least one unrevoked verifier valid at trusted UTC commit time.
- The guard runs for membership changes, login disablement, and active credential revocation, then validates the serialized final catalog. Token issuance remains available to add or restore a credential before old-token revocation.
- Real-redb regressions cover refusal to disable/revoke one administrator when the only other login admin has expired credentials, refusal to change membership when the resulting administrators have only expired credentials, and successful replacement-first token rotation.
- Verification: in the prescribed Nix shell, the locked workspace build, formatting check, Clippy with warnings denied, and all 218 workspace tests passed. Changed Markdown links and `git diff --check` passed.
- Contract: [users and roles](../../specs/ddl-identity.md).
- Remaining: bootstrap secret handoff, authenticated control-channel/gateway integration, token request rate limits, user/role DROP RESTRICT, and deterministic authority-change race coverage remain open.

## SM-5 dependency: durable identity tombstones

- Red evidence: extending the catalog registry test to family 17 failed because the identity-tombstone family had no codec schema.
- Logical catalog schema version 2 adds family 17 for immutable ID-only tombstones and bootstrap storage-object code 217. Startup and serialized commits reject tombstones that overlap live object or principal IDs; identity creation rejects a tombstoned ID and guards the absent tombstone key through publication. Version-1 catalogs fail closed until an offline migration path exists.
- Revoked credential verifiers may remain attached to a tombstoned user ID, while unrevoked verifiers still require a live LOGIN user.
- Real-redb regressions cover accepted tombstones, startup rejection of ID reuse, identity-creation refusal, and catalog-commit refusal to tombstone a live principal.
- Verification: the prescribed Nix-shell locked workspace build, formatting check, Clippy with warnings denied, and all 220 workspace tests passed. Changed Markdown local links and `git diff --check` passed.
- Contract updates: [catalog payloads](../../specs/catalog-payloads.md), [catalog lifecycle](../../specs/catalog-lifecycle.md), [data dictionary](../../specs/data-dictionary.md), and [users and roles](../../specs/ddl-identity.md).
- Remaining: user/role DROP RESTRICT must publish the tombstone and remove blocked references atomically.

## SM-5 dependency: restricted user and role deletion

- Red evidence: the unreferenced-user deletion regression initially failed with `InvalidCatalog` because the authorized identity route rejected deletes and family-17 tombstones.
- The identity DDL route now accepts paired principal/object deletion plus a new tombstone, requires `MANAGE_USERS` or `MANAGE_ROLES`, rejects the reserved `database_admin` role, and performs bounded RESTRICT scans for object ownership/parent references, membership/grantor references, grants, policy targets, and dependency edges. Existing serialized final-catalog validation rejects concurrent references that were absent from the preflight snapshot.
- Deleting a user requires every active family-16 credential to be revoked in the same batch. The writer checks that at least one login-enabled administrator with a current credential remains. The principal/object removal, token revocations, tombstone, audit, outcome, and commit sequence publish atomically.
- Real-redb regressions cover unreferenced user and role drops with tombstone persistence, user-token revocation in the same batch, refusal to omit an active-token revocation, a grant reference that blocks `RESTRICT`, and persisted-catalog validation after deletion.
- Contract updates: [users and roles](../../specs/ddl-identity.md) and [storage manager](../../specs/storage-manager.md).
- Remaining: bootstrap secret handoff, authenticated control-channel/gateway integration, token request rate limits, deterministic authority-change race coverage, and role/user DROP integration through a SQL/control frontend remain open.

## SM-5 dependency: token lifecycle rate limiting

- Red evidence: with the limiter disabled, the real-redb control-path regression expected `RateLimited` on the sixth request across two manager handles but received token metadata.
- `issue_token`, `revoke_token`, and `list_tokens` now use a monotonic rolling 60-second budget per authenticated actor. The default is 30 requests per minute, configured through `StorageManagerConfig`; lower limits take effect across live handles for the same redb `Database` object. The actor-history map is bounded at 4,096 and stale entries are pruned. A full map and poisoned per-actor state fail closed.
- An integration regression verifies the budget across issue, inventory, and revoke calls and across independently constructed manager handles. Zero limits are rejected during configuration validation.
- Contract updates: [users and roles](../../specs/ddl-identity.md) and [storage manager](../../specs/storage-manager.md).
- Verification: prescribed Nix-shell workspace build, formatting check, Clippy with warnings denied, and all 222 workspace tests passed. Changed Markdown local links and `git diff --check` passed.
- Remaining: token-authentication failures still need gateway-level throttling because storage admission lacks peer/source context; bootstrap secret handoff, authenticated control-channel integration, deterministic authority-change race coverage, and SQL/control frontend integration remain open.

## CL-1 / CL-2 dependency: local bootstrap and credential handoff — verified core

- Red evidence: the focused bootstrap regression failed when the initial scaffold returned `NotImplemented` for initialization.
- `bootstrap::initialize` creates an owner-only directory, instance lock, setup marker, redb file, complete built-in catalog, reserved administrator role, first LOGIN user, membership, first token verifier, audit event, and `INITIALIZING` metadata in one immediate-durability transaction.
- `BootstrapSession::take_secret` transfers the random secret once and zeroizes its retained buffer. Acknowledgement requires prior transfer, validates the initializing image and active token, then durably publishes `READY`. `StorageInstance::open` holds the nonblocking exclusive lock and accepts only a validated READY image; its manager constructor reuses that validation.
- Resume rejects READY or corrupt images, recovers owner-only empty pre-marker/pre-commit directories, rotates an unclaimed token atomically, and refuses directories containing unknown files. Real-redb tests cover duplicate initialization, lock exclusion, permissions, verifier-only persistence, lost handoff, acknowledgement ordering, pre-marker recovery, and empty-file recovery.
- Verification: the prescribed Nix-shell workspace build, formatting check, Clippy with warnings denied, and all 227 workspace tests passed. `git diff --check` passed.
- Contract updates: [catalog lifecycle](../../specs/catalog-lifecycle.md), [storage manager](../../specs/storage-manager.md), and [credential lifecycle](../../specs/ddl-identity.md).
- Remaining: a protected setup socket/supervisor to deliver the secret and acknowledge delivery, power-loss fault injection at transaction boundaries, and normal service integration remain open. Direct `StorageManager` constructors are trusted low-level entry points and do not acquire the filesystem lock; service startup must use `StorageInstance::open`.

## SM-4 / SM-5 dependency: catalog authority-change race validation

- Added a barrier-coordinated real-redb race between an invocation staging a write under a pinned principal authority epoch and a catalog commit advancing that epoch. The row write either commits before the authority change, or the authority change commits first and the invocation records ABORTED; row visibility, outcome, and commit sequence are checked against that order. The existing sequential regression covers the change-first ordering directly.
- Verification: prescribed Nix-shell workspace build, formatting check, Clippy with warnings denied, and all 228 workspace tests passed.
- Contracts: [authorization](../../specs/authorization.md) and [concurrency/MVCC](../../specs/concurrency-mvcc.md).
- Remaining: concurrent schedule coverage across each action-specific grant, membership, policy, identity, and credential route remains open; the supervisor/control-channel integration is not implemented.

## IPC-1 dependency: bounded Unix-stream frame v1

- Red evidence: the hand-checked frame fixture failed against the compiling `NotImplemented` scaffold.
- Added a 20-byte big-endian v1 header with magic, operation, nonzero request ID and payload length. Encoding/reading caps payloads at 1 MiB; reading rejects malformed/unsupported headers and oversized lengths before body allocation, distinguishes clean boundary EOF from truncation, and preserves following frames in a stream.
- Tests cover the byte fixture, split/coalesced Unix-stream writes, malformed/oversized headers, zero IDs, truncation and writer limits.
- Contracts: [IPC protocol](../../specs/ipc-protocol.md) and [IPC plan](../ipc-protocol/README.md).
- Verification: prescribed Nix-shell workspace build, formatting check, Clippy with warnings denied, and all 233 workspace integration tests passed.
- Remaining: operation payload schemas and allowlists, server-bound role dispatch, local service authentication, C parity, and gateway/supervisor/storage integration remain open.

## CL-2 dependency: protected local bootstrap handoff

- Red evidence: both focused setup-socket regressions failed against the compiling `NotImplemented` listener scaffold.
- `BootstrapSession::bind_setup_listener` creates a mode-0600 Unix socket in the owner-only data directory. The one-shot listener verifies Linux peer credentials against the directory owner, sends token ID and secret in the bounded IPC v1 frame, requires the matching empty acknowledgement, removes the socket durably, then publishes READY. The client helper transfers the secret once and zeroizes retained buffers; disconnect, malformed ACK, or stream timeout leaves INITIALIZING for token rotation on resume. Resume and locked READY open remove a stale validated setup socket.
- Real-file tests cover successful delivery and acknowledgement, unacknowledged disconnect, mismatched request ID, stale-socket cleanup, and READY publication only after delivery.
- Contract updates: [catalog lifecycle](../../specs/catalog-lifecycle.md), [IPC protocol](../../specs/ipc-protocol.md), and [catalog lifecycle plan](../catalog-lifecycle/README.md).
- Verification: prescribed Nix-shell workspace build, formatting check, Clippy with warnings denied, and all 237 workspace integration tests passed.
- Remaining: production supervisor and operator-facing setup client integration, OS-account deployment policy, and process/power-loss fault injection remain open. Same-UID access to this local socket is not itself a database identity; possession of the issued token authenticates database use.

## MD-1 dependency: pinned authorized relation listing — in progress

- Red evidence: `authorized_relation_listing_uses_schema_usage_and_data_grants` failed against the empty listing scaffold because the granted schema returned no page.
- `InvocationTransaction::list_authorized_relations` now reads table IDs and definitions from the invocation's pinned catalog. It requires schema USAGE plus a persisted table SELECT/INSERT/UPDATE/DELETE or column SELECT/INSERT/UPDATE grant, validates the pinned role closure and each supplied grant, and omits inaccessible relations without reading rows or evaluating row policies. Stable relation IDs and names are returned in bounded pages (128 relations, 1 MiB); transaction-local cursors bind the invocation, schema, role/scope, grants and authority dependencies. A forged grant and a cursor from another invocation are rejected.
- Tests cover write-only discovery, column-grant discovery, inaccessible relations, schema USAGE denial, malformed hidden row bytes, catalog grant validation and continuation ownership.
- Checks: the locked workspace build, `cargo fmt --all -- --check`, Clippy with warnings denied, the full workspace test suite, and `git diff --check` passed in the prescribed Nix shell. Local Markdown links in the three changed documents resolved.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: serialized IPC cursors, built-in `information_schema` scans, relation resolution handles, and administrative discovery remain open.

## MD-1 dependency: authorized declared-column descriptions — in progress

- Red evidence: `authorized_column_description_returns_only_declared_shape` failed against the unavailable-result scaffold despite schema USAGE and a persisted table INSERT grant.
- `InvocationTransaction::describe_authorized_columns` validates the pinned role closure, actual grant records, schema usage, relation ownership in the schema, and the catalog table schema before returning stable column IDs, names, ordinals, scalar types, and nullability. It omits default and policy details and returns no partial shape when the 1 MiB response bound is exceeded.
- The regression verifies table write access permits declared-shape discovery and schema usage alone does not; the result type contains no default or policy fields.
- Checks: the locked workspace build, formatting check, warning-denied Clippy, the full workspace test suite, and `git diff --check` passed in the prescribed Nix shell. Focused transaction tests also passed.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: serialized IPC cursors, built-in `information_schema` scans, resolution handles, and administrative discovery remain open.

## MD-1 dependency: schema listing by pinned USAGE grants — in progress

- Red evidence: `schema_listing_includes_only_usage_grants_and_pages_by_schema_id` failed against the empty-page scaffold although two schemas had effective USAGE grants.
- `InvocationTransaction::list_authorized_schemas` validates the pinned role closure and supplied grant records, filters to schema USAGE within the admitted scope, and returns only live schema objects. Pages are bounded to 128 records and 1 MiB, ordered by stable schema ID, with a transaction-local cursor bound to the caller and authorization inputs.
- The real-redb fixture includes one ungranted schema between two granted schemas; the two visible schemas page in order and the hidden schema name is omitted.
- Checks: the locked workspace build, formatting check, warning-denied Clippy, full workspace tests, and `git diff --check` passed in the prescribed Nix shell. The focused transaction target passed.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: serialized IPC cursors, built-in `information_schema` scans, relation resolution handles, and administrative discovery remain open.

## MD-1 dependency: paginated declared-column descriptions — in progress

- Red evidence: `authorized_column_description_pages_bind_the_snapshot_and_order` failed against the unavailable page scaffold despite a table INSERT grant; it returned no description page.
- `InvocationTransaction::describe_authorized_columns_page` rechecks the pinned schema, role closure, grants, and dependencies on every page, then returns at most 128 declared columns and 1 MiB in ordinal order. Its continuation binds the invocation, relation, and authorization inputs; cross-invocation cursor reuse rejects. A too-large single column returns an error without a partial record.
- The real-redb regression pages a two-column relation, checks ordinals and ordering, and rejects a cursor in another invocation before successfully continuing the original snapshot.
- Checks: the locked workspace build, formatting check, warning-denied Clippy, full workspace tests, and `git diff --check` passed in the prescribed Nix shell. The focused transaction target passed as well.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: serialized IPC cursors, built-in `information_schema` scans, relation resolution handles, and administrative discovery remain open.

## MD-1 dependency: exact authorized relation resolution — in progress

- Red evidence: `authorized_relation_resolution_requires_visibility_and_exact_name` failed against the unavailable-result scaffold for the granted `metrics` relation.
- `InvocationTransaction::resolve_authorized_relation` walks filtered relation pages using an exact name match, then independently rechecks access before returning the stable relation ID, name, and declared columns. Hidden and nonexistent names both return no result. Input names and the combined description are bounded; the result does not authorize row reads.
- The real-redb regression verifies exact case-sensitive lookup, the declared shape, and indistinguishable unavailable results for a hidden relation and a missing name.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: serialized IPC cursors, built-in `information_schema` scans, connection-local executor handles, and administrative discovery remain open.

## MD-1 dependency: exact authorized schema resolution — in progress

- Red evidence: `authorized_schema_resolution_matches_only_visible_exact_names` failed against the unavailable-result scaffold for the USAGE-visible `app` schema.
- `InvocationTransaction::resolve_authorized_schema` walks the invocation's bounded USAGE-filtered schema pages and compares the exact requested name. Hidden and absent names return the same unavailable result; the resolved stable ID carries no authority by itself.
- The real-redb regression checks visible resolution, case sensitivity, and matching unavailable outcomes for a hidden schema and an absent name.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: serialized IPC cursors, built-in `information_schema` scans, connection-local executor handles, and administrative discovery remain open.

## MD-1 dependency: catalog-derived schema USAGE grants — in progress

- Red evidence: `schema_access_loader_uses_pinned_principal_schema_usage_grants` returned no grants against the unavailable-loader scaffold although the pinned catalog contains two schema USAGE records.
- `InvocationTransaction::load_catalog_schema_usage_grants` derives only schema USAGE grants for the admitted principal and its pinned role closure. It validates grantor and schema records and retains catalog guards for grants, schemas, grantors, and role membership so the authority is rechecked at finalization.
- The real-redb regression confirms that only the two granted schema records are returned; table and column grants do not enter the result.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: serialized IPC cursors, built-in `information_schema` scans, connection-local executor handles, and administrative discovery remain open.

## MD-1 dependency: invocation-bound schema listing — in progress

- Red evidence: `catalog_authorized_schema_listing_derives_the_invocation_scope` failed against the unavailable entry-point scaffold instead of returning the two schemas granted in the invocation's pinned catalog.
- `InvocationTransaction::list_catalog_authorized_schemas` derives the admitted principal's role closure and schema USAGE grants from the pinned catalog, constructs the authorization context internally, and delegates to the bounded page implementation. Caller-provided identities and grant scopes are not accepted.
- The real-redb regression pages the visible `app` and `public` schemas without a caller-supplied context or grant set.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: catalog-derived relation grants and reference resolution, serialized IPC cursors, built-in `information_schema` scans, connection-local executor handles, and administrative discovery remain open.

## MD-1 dependency: invocation-bound relation listing — in progress

- Red evidence: `catalog_authorized_relation_listing_derives_the_invocation_scope` failed against the unavailable entry-point scaffold rather than returning the two relations covered by persisted grants.
- `load_catalog_relation_discovery_grants` derives schema USAGE and table/column data grants for the admitted principal from its pinned role closure and one schema. It validates columns and grantors and captures authority guards. `list_catalog_authorized_relations` builds the context internally and delegates to bounded relation pages.
- The real-redb regression pages `metrics` and `secret` from their actual write-only/column grants and produces the same unavailable result for an ungranted schema and a missing schema ID.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: catalog-derived exact relation resolution, serialized IPC cursors, built-in `information_schema` scans, connection-local executor handles, and administrative discovery remain open.

## MD-1 dependency: invocation-bound qualified relation resolution — in progress

- Red evidence: `catalog_authorized_qualified_relation_resolution_uses_pinned_grants` returned no resolution against the unavailable-result scaffold for visible `app.metrics`.
- `InvocationTransaction::resolve_catalog_authorized_relation` resolves the schema name from pinned USAGE grants, then resolves the relation name with schema-scoped table/column grants. It constructs both authorization contexts internally and rechecks relation authority before returning the declared shape.
- The real-redb regression confirms exact qualified lookup and matching unavailable results for hidden and absent schemas and relations.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: serialized IPC cursors, built-in `information_schema` scans, connection-local executor handles, and administrative discovery remain open.

## MD-3 dependency: opaque schema cursor wire tokens — in progress

- Red evidence: the schema pagination regression failed after serializing and decoding a continuation because the in-process cursor's invocation and generation fields were absent from the wire form.
- `SchemaScanCursor` now serializes as a fixed 16-byte opaque token. The invocation retains its cursor position and authorization binding, consumes each issued token once, and rejects modified, replayed, and cross-invocation tokens. Token generation uses the operating-system randomness source and fails the invocation if secure randomness is unavailable.
- Tests cover byte round-trip through a later page, malformed token width, cross-invocation use, token modification, and replay after page completion.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: relation and column cursor wire forms, cursors across process restart, built-in `information_schema` scans, connection-local executor handles, and administrative discovery remain open.

## MD-3 dependency: relation and column cursor wire tokens — in progress

- The relation and column regressions now serialize continuations to fixed-width bytes and resume the same scan after decoding, so Rust struct fields are not part of the transport contract.
- Relation and column cursors now serialize as fixed 16-byte opaque tokens. Invocation-local state binds each token to its scan, authorization inputs, and next position; token consumption is one-use and uses OS randomness.
- Real-redb regressions round-trip both cursor types across page requests, reject cross-invocation use, and reject replay after a page completes. Schema cursor regressions also cover modified bytes and invalid token width.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: cursor lifetime across process restart, built-in `information_schema` scans, connection-local executor handles, and administrative discovery remain open.

## MD-3 dependency: cursor-kind binding

- Added a cursor-kind discriminator to the 16-byte schema, relation, and column tokens. Decoders reject tokens from the other cursor families; the remaining bytes use the operating-system random source.
- Regression assertions verify that each cursor encoding is accepted only by its matching decoder.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md) and [storage manager](../../specs/storage-manager.md).

## MD-4 dependency: authorized `information_schema.schemata` projection

- Red evidence: `information_schema_schemata_exposes_public_and_authorized_names` failed before the projection was implemented.
- `InvocationTransaction::scan_information_schema_schemata` reuses the catalog-authorized schema scan and maps each visible schema to a row containing only `schema_name`, preserving page bounds and continuation behavior.
- The regression pages the `app` and `public` schemas through one invocation and verifies ordering and completion.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: SQL planner/executor integration for `information_schema`, `tables` and `columns` projections, connection-local executor handles, and administrative discovery.

## MD-4 dependency: schema-scoped `information_schema.tables` projection

- Red evidence: `information_schema_tables_projects_authorized_schema_pages` failed against the empty-result scaffold for visible schema `app`.
- `InvocationTransaction::scan_information_schema_tables_in_schema` resolves an exact schema name through pinned USAGE grants, applies the existing table/column grant filter, and projects each visible relation to `table_schema`, `table_name`, and `table_type = BASE TABLE`. Projected schema/type bytes count toward the existing bounded page size; relation continuations cannot be reused through the raw relation-list API.
- The real-redb regression pages `app` relations, returns an empty page for visible empty `public`, and gives the same unavailable result for hidden `private` and absent `missing` schemas.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: iterate all visible schemas for an unfiltered tables scan, SQL binding/execution, the columns projection, connection-local executor handles, and administrative discovery.

## MD-4 dependency: relation-scoped `information_schema.columns` projection

- Red evidence: `information_schema_columns_projects_declared_shape_in_pages` failed against the empty-result scaffold for visible `app.metrics`.
- `InvocationTransaction::scan_information_schema_columns_for_relation` resolves schema and relation names through pinned grants, pages the relation's declared columns, and projects one-based ordinals, YES/NO nullability, built-in type names, and only applicable type parameters. Projected metadata bytes count toward the page cap, and the continuation is bound to this projection.
- Regressions cover two ordinal-ordered pages, nullable values, BYTEA's inapplicable parameters, hidden/missing object equivalence, and rejection of the projection cursor by the raw description API. Type mapping covers VARCHAR and DECIMAL parameters plus integer numeric precision/scale.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: iterate all visible relations for a complete columns scan, SQL binding/execution, connection-local executor handles, and administrative discovery.

## MD-4 dependency: all-visible-schema `information_schema.tables` scan

- Red evidence: `information_schema_tables_scan_walks_visible_schema_pages` failed against the empty scan scaffold.
- `InvocationTransaction::scan_information_schema_tables` walks schema pages in stable schema-ID order and relation pages in stable relation-ID order. Its kind-specific opaque cursor retains the current schema and both child continuations; the scan stays bounded to one schema's relation page per call.
- The real-redb regression decodes and resumes a 16-byte token, verifies that the cursor kind is distinct, omits the hidden `private` schema, advances through empty `public`, and rejects cross-invocation use and replay.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: iterate visible relations for a complete columns scan, fixed public self-description rows, SQL binding/execution, connection-local executor handles, and administrative discovery.

## MD-4 dependency: all-visible-relation `information_schema.columns` scan

- Red evidence: `information_schema_columns_scan_walks_visible_relation_pages` failed against the empty scan scaffold for visible `app.metrics`.
- `InvocationTransaction::scan_information_schema_columns` walks schema, relation, and column pages in stable schema-ID/relation-ID/ordinal order. Its kind-specific token retains each active child continuation, while each call returns at most one relation's bounded column page.
- The real-redb regression pages two columns of `app.metrics`, continues to `app.secret`, advances over empty `public`, verifies cursor-kind isolation, and rejects cross-invocation use and replay.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: fixed public self-description rows, SQL binding/execution, connection-local executor handles, and administrative discovery.

## MD-4 dependency: public `information_schema` self-description

- Red evidence: `information_schema_builtin_relations_have_public_self_descriptions` first returned the granted `app` schema instead of the public reserved namespace.
- Schema discovery now includes `information_schema` as a public exception to user-schema USAGE grants, represented without recursively reading its virtual relations from the catalog. Its fixed logical relations are `columns`, `schemata`, and `tables`; all are reported as `VIEW`.
- The relation resolver and schema-scoped scans return fixed column IDs, names, ordinals, scalar types, and nullability for all three shapes. Global tables and columns scans include those definitions and retain their existing bounded nested cursors.
- Real-redb regressions cover the complete `columns` shape, the other relation definitions, paged `VIEW` rows, public schema listing, and global scans across built-in and granted user relations.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [metadata discovery plan](../metadata-discovery/README.md).
- Remaining: SQL binding/execution for the virtual relations, connection-local executor handles, and administrative discovery.

## MD-3 dependency: executor connection relation handles

- Red evidence: the connection-handle regression did not compile because the invocation had no executor metadata session or handle API.
- `InvocationTransaction::open_executor_metadata_connection` creates a session-local registry. Resolution returns a random 16-byte kind-6 handle plus the authorized description; each revalidation resolves the stored qualified name through pinned catalog grants and checks its stable relation ID. Handles do not authorize data operations.
- Each session can issue at most 256 handles over its lifetime. Explicit close and session drop invalidate them. The regression covers byte round-trip, cursor-kind/width rejection, revalidation, close, and rejection from a replacement connection and another invocation.
- Contracts: [metadata discovery](../../specs/metadata-discovery.md), [storage manager](../../specs/storage-manager.md), and [IPC protocol](../../specs/ipc-protocol.md).
- Remaining: bind one session to each provisioned executor socket, implement scan/mutation request dispatch and reauthorization, SQL integration, and administrative discovery.

## SM-6 dependency: executor relation-resolution dispatch

- Behavior: executor role dispatch for `resolve_relation` decodes a versioned bounded payload, resolves through the connection-scoped session and returns a stable relation ID plus opaque handle. Hidden and absent qualified names share one unavailable response; planner role calls are denied before storage lookup.
- Changed code and fixture: [executor IPC dispatcher](../../engine/src/executor_ipc.rs), [transaction regression](../../engine/tests/transaction.rs), and [IPC protocol](../../specs/ipc-protocol.md).
- Red evidence: the new regression first failed to compile because the executor dispatch module/API did not exist. This established the missing implementation surface; it was not an assertion-level red test.
- Checks: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, full workspace tests, and `git diff --check` passed in the prescribed Nix shell. Changed Markdown local links passed.
- Remaining: supervisor socket provisioning, scan/cursor/mutation dispatch and reauthorization, SQL integration, and administrative discovery.

## SM-6 dependency: executor relation-resolution stream loop

- Behavior: a bound stream can serve repeated `resolve_relation` frames with one connection-owned handle registry. Duplicate request IDs and more than 256 requests fail closed; clean disconnect and protocol/request/write errors drop the registry and durably abort the bound invocation.
- Changed code and fixture: [executor IPC dispatcher](../../engine/src/executor_ipc.rs), [transaction socket regression](../../engine/tests/transaction.rs), and [IPC protocol](../../specs/ipc-protocol.md).
- Evidence: a real Unix stream pair sends an authorized resolution, half-closes, receives the matching response, and verifies the persisted ABORTED outcome on disconnect. The focused test passes in the prescribed Nix shell.
- Remaining: supervisor socket provisioning, scan/cursor/mutation dispatch and reauthorization, SQL integration, and administrative discovery.
- Next smallest task: add authorized `open_scan` and bounded cursor state to the same session before exposing row batches.

## SM-3 dependency: catalog-derived authorized row scanning

- Behavior: `InvocationTransaction::scan_catalog_authorized_rows` constructs its authorized projection from the pinned table schema, role closure, data grants, and applicable row/cell policies, then scans the pinned snapshot plus private overlay. Callers cannot supply grant or policy vectors to authorize the operation.
- Changed code and fixture: [transaction implementation](../../engine/src/transaction.rs), [catalog scan regression](../../engine/tests/transaction.rs), and [storage-manager contract](../../specs/storage-manager.md).
- Red evidence: the regression initially failed to compile because the catalog-derived scan API did not exist. During fixture completion, row-policy filtering and cell redaction behaved as specified; the valid real-redb fixture was completed with table SELECT and row/cell ALLOW policies.
- Verification: workspace build, `cargo fmt --all -- --check`, warning-denied workspace Clippy, full workspace tests, focused scan regression, and `git diff --check` passed in the prescribed Nix shell.
- Remaining: serialize/bind its continuation to executor IPC, enforce response-byte limits, and test this derived path against policy/grant changes before finalization.
- Next smallest task: add a connection-local scan cursor token that wraps `RowScanCursor` without exposing row IDs or accepting cross-connection tokens.

## SM-6 dependency: connection-local executor scan cursors

- Behavior: `ExecutorMetadataConnection` now issues kind-7 opaque scan cursors, consumes one token per page, revalidates the relation handle and delegates to the pinned-catalog authorized scan path. It rejects replay and cross-connection use, and can close a scan to release the active scan slot.
- Changed code and fixture: [transaction implementation](../../engine/src/transaction.rs), [cursor wire type](../../engine/src/metadata_discovery.rs), and [real-redb pagination regression](../../engine/tests/transaction.rs).
- Evidence: the regression scans two persisted rows across one-row pages, confirms the required final empty page for EOF, rejects replay and replacement-connection use, and rejects the cursor through another kind's decoder. Workspace build, format check, warning-denied Clippy, full workspace tests, and `git diff --check` all pass in the prescribed Nix shell.
- Red evidence: an early assertion expected a full one-row page to prove EOF; the existing scan contract requires an additional page to confirm EOF, so the regression was corrected to consume the continuation and final empty page.
- Remaining: encode/decode `open_scan`, `next_batch`, and `close_cursor` IPC payloads, enforce output-byte bounds, and test the derived path against concurrent policy/grant changes.
- Next smallest task: define the bounded wire payload for opening a scan from a resolved relation handle and validate requested row-count limits.

## SM-6 dependency: executor scan and batch IPC

- Behavior: executor resolve responses now include the authorized declared shape. Version-1 `open_scan`, `next_batch`, and `close_cursor` payloads dispatch through the connection-owned session. The wire path supports ordinal-ordered column subsets, candidate and byte caps, VALUE/NULL/REDACTED cells, and opaque row handles; it never serializes row IDs.
- Changed code and fixtures: [executor IPC codec/dispatcher](../../engine/src/executor_ipc.rs), [row-handle and cursor ownership](../../engine/src/transaction.rs), [real-redb IPC regression](../../engine/tests/transaction.rs), and [IPC protocol contract](../../specs/ipc-protocol.md).
- Evidence: the real-redb regression resolves the catalog shape, opens a selected-column scan, transfers two visible rows through one-use cursor batches under a 48-byte response cap, decodes canonical BYTEA values and opaque row handles, confirms the final empty EOF page, and verifies close/replay behavior.
- Red evidence: the first resolver-response decode rejected ordinal zero because the new decoder assumed one-based ordinals; the catalog stores zero-based ordinals. The decoder now preserves that contract.
- Verification: workspace build, format check, warning-denied Clippy, full workspace tests, all 168 changed-document local links, and `git diff --check` pass in the prescribed Nix shell.
- Remaining: test max-byte failure through the stream loop, exercise NULL/REDACTED wire states and hostile payloads, and connect the provisioned supervisor socket to this loop.
- Next smallest task: add StageInsert/StageUpdate/StageDelete request dispatch that consumes row handles and rechecks catalog authority.

## SM-6 dependency: catalog-authorized executor mutations

- Behavior: executor IPC now decodes full-shape INSERT values and unique ordinal UPDATE patches, and dispatches INSERT/UPDATE/DELETE through connection-local handles. Storage rebuilds write projection, checks, foreign keys, and index definitions from pinned catalog records; policy-referenced fields cannot be updated, and successful UPDATE/DELETE invalidates row handles.
- Changed code and fixture: [executor IPC](../../engine/src/executor_ipc.rs), [catalog-derived write projection and handle staging](../../engine/src/transaction.rs), [mutation codec regressions](../../engine/tests/executor_ipc.rs), [real-redb delete dispatch regression](../../engine/tests/transaction.rs), and [IPC contract](../../specs/ipc-protocol.md).
- Evidence: focused mutation codecs and a real-redb executor INSERT/UPDATE/DELETE regression pass. The fixture seeds indexed rows through authorized executor INSERT, stages a second INSERT, UPDATE, and DELETE through IPC, commits atomically, and verifies persisted tuple values plus row removal from a fresh snapshot. It also verifies catalog-derived grants/policies, staged visibility, and handle invalidation.
- Red evidence: an early mixed-fixture commit returned `InvalidIndex` because its existing rows came from the low-level writer and lacked catalog index locators; reseeding them through authorized INSERT made index maintenance valid. A first seed attempt also hit `IndexOverlayLimitExceeded` with an undersized fixture budget; the fixture now allows its catalog-derived index entries. Workspace build, format, warning-denied Clippy, full tests, local Markdown links, and diff checks pass.
- Remaining: generated identity allocation remains unsupported; broader FK/unique-index mutation cases still need dedicated executor-path fixtures.
- Next smallest task: bind the generic executor request loop to supervisor-provisioned socket lifecycle.

## SM-6 dependency: executor batch overflow abort

- Behavior: a single authorized row that exceeds the caller's `max_bytes` aborts the invocation before any partial row is returned.
- Changed fixture: [real-redb executor batch regression](../../engine/tests/transaction.rs).
- Evidence: the request is rejected at a 46-byte cap for a 47-byte encoded row, and the invocation's durable outcome is ABORTED. The 48-byte pagination case still succeeds.
- Verification: workspace build, format check, warning-denied Clippy, full workspace tests, all 158 changed-document local links, and `git diff --check` pass.
- Remaining: exercise this failure through a live Unix stream and validate NULL/REDACTED and malformed response edge cases.
- Next smallest task: add a stream-level multi-operation executor request regression before socket provisioning work.

## SM-6 dependency: multi-operation executor stream

- Behavior: the generic `serve_executor` entry point keeps connection-owned relation and cursor handles across `resolve_relation`, `open_scan`, and `close_cursor` requests on one Unix stream. Clean EOF ends only the executor session and leaves terminal outcome authority to the supervisor; protocol failures durably abort. The original resolver-named function remains as a compatibility wrapper.
- Changed code and fixture: [executor stream loop](../../engine/src/executor_ipc.rs) and [Unix-stream regression](../../engine/tests/transaction.rs).
- Evidence: the focused stream regression confirms a close acknowledgement, no outcome on clean EOF, and an ABORTED outcome only after the caller explicitly aborts.
- Red evidence: an initial attempt to include `next_batch` in the relation-discovery-only fixture failed with `CorruptCatalog`; that fixture lacks the scan policy records. The richer catalog-authorized regression already exercises `next_batch` through the same dispatcher, while this stream test covers persistent handle state and cursor lifecycle.
- Verification: workspace build, format check, warning-denied Clippy, full tests, all 159 changed-document local links, and `git diff --check` pass.
- The generic stream entry-point rename and compatibility wrapper also pass the full workspace validation; 160 local Markdown links resolve.
- Remaining: run scan and mutation operations through a live stream with the full authorization fixture; implement supervisor observation of worker completion and its trusted finalize/abort channel.
- Next smallest task: bind executor and planner streams to a supervisor-owned invocation lifecycle.

## SM-6 dependency: generated UUID identities

- Behavior: executor INSERT now uses DEFAULT markers independently from NULL, allocates UUID identities only for DEFAULT, rejects explicit generated values and updates to identity columns, and validates collisions against the serialized final table contents.
- Changed code and contract: [transaction and commit validation](../../engine/src/transaction.rs), [executor INSERT codec](../../engine/src/executor_ipc.rs), [IPC protocol](../../specs/ipc-protocol.md), and [schema DDL contract](../../specs/ddl-schema.md).
- Verification: workspace build, format check, warning-denied Clippy, and `git diff --check` pass. Tests were not run in this increment.
- Remaining: generated identity allocation has only a focused staged-value regression so far; add catalog-backed end-to-end coverage when test execution is requested.

## ST-1 / ST-2 dependency: planner metadata and estimate dispatch

- Behavior: planner-only connection handles resolve and revalidate authorized relation descriptions, and estimate requests return bounded visible/readable/null/redacted summaries or UNKNOWN. Planner projections derive read grants and policies without loading write constraints or indexes; executor handles cannot be decoded as planner handles.
- Changed code and contracts: [planner IPC](../../engine/src/planner_ipc.rs), [planner connection state](../../engine/src/transaction.rs), [planner handle](../../engine/src/metadata_discovery.rs), [IPC protocol](../../specs/ipc-protocol.md), and [statistics service](../../specs/storage-manager.md).
- Verification: workspace build, format check, warning-denied Clippy, and `git diff --check` pass. Tests were not run in this increment.
- Remaining: planner stream provisioning, planner task lifecycle/budget integration, and predicate/access-path estimates remain open.

## ST-1 / ST-2 dependency: planner stream lifecycle

- Behavior: `serve_planner` loops over one provisioned planner stream, enforces unique request IDs and a 256-request cap, and preserves kind-9 handle state across requests. Clean EOF closes only the planner session so execution can continue; malformed frames and protocol violations durably abort the invocation.
- Changed code and contract: [planner stream and dispatcher](../../engine/src/planner_ipc.rs) and [planner IPC contract](../../specs/ipc-protocol.md).
- Verification: format, workspace build, and warning-denied Clippy pass. Tests were not run in this increment.
- Remaining: supervisor provisioning, shared planner/executor invocation scheduling, and cancellation-aware planner task draining remain open.

## SM-4 / SU-5 dependency: supervisor terminal storage channel

- Behavior: role-checked Supervisor handlers finalize or abort one out-of-band-bound invocation, look up owner-scoped status, and request audited administrative cancellation with the administrator bound outside the payload. Terminal operations consume the invocation, finalize returns the durable commit sequence, and losing the terminal channel before a decision durably aborts. Executor EOF only closes its session.
- Changed code and contract: [supervisor IPC handlers](../../engine/src/supervisor_ipc.rs), [executor stream lifecycle](../../engine/src/executor_ipc.rs), and [IPC protocol](../../specs/ipc-protocol.md).
- Verification: format, workspace build, warning-denied Clippy, and `git diff --check` pass. Tests were not run in this increment.
- Remaining: implement worker exit observation, planner/executor multiplexing, and gateway cancellation wiring to the audited admin operation.

## SU-1 / SU-5 dependency: token-bound supervisor invocation owner

- Behavior: `StorageManager::admit_supervisor_invocation` verifies a database token, pins identity and role authority, and returns an owner that binds planner/executor channels to one invocation and exposes terminal and control channels. Dropping the owner without finalization attempts a durable abort.
- Changed code and contract: [storage manager admission wrapper](../../engine/src/storage_manager.rs) and [storage manager lifecycle contract](../../specs/storage-manager.md).
- Verification: format, workspace build, warning-denied Clippy, and `git diff --check` pass. Tests were not run in this increment.
- Remaining: launch/reap a confined worker, observe worker completion, and wire gateway cancellation; those remain supervisor scheduler responsibilities.

## SU-5 dependency: shared planner/executor invocation scheduling

- Behavior: planner and executor IPC streams may run concurrently against one supervisor-owned invocation. Each request takes the shared transaction lock only for dispatch, and per-stream opaque handle/cursor state survives between requests. Executor clean EOF releases its scans without deciding the invocation outcome; protocol failures abort.
- Changed code and contract: [shared IPC handlers](../../engine/src/planner_ipc.rs), [executor session state](../../engine/src/executor_ipc.rs), [transaction connection state](../../engine/src/transaction.rs), [supervisor invocation owner](../../engine/src/storage_manager.rs), and [storage-manager contract](../../specs/storage-manager.md).
- Verification: `cargo fmt --all`, workspace build, warning-denied Clippy, and `git diff --check` pass in the repository Nix shell. Tests were not run.
- Remaining: the supervisor must stop and drain worker channels before terminal finalization; process scheduling, worker exit observation, and gateway cancellation remain open.

## SU-5 dependency: fail-closed shared invocation lock poisoning

- Behavior: if a planner or executor request panics while holding the shared invocation lock, planner/executor handlers and the terminal owner consume the poisoned invocation and attempt durable abort; terminal handling cannot recover the lock and commit interrupted state.
- Changed code: [shared planner/executor handlers](../../engine/src/planner_ipc.rs), [executor request loop](../../engine/src/executor_ipc.rs), and [terminal owner](../../engine/src/storage_manager.rs).
- Verification: `cargo fmt --all`, workspace build, warning-denied Clippy, and `git diff --check` pass in the repository Nix shell. Tests were not run.
- Remaining: runtime tests should cover panic poisoning and abort outcome when test execution is authorized.

## SU-5 dependency: drain worker channels before terminal decisions

- Behavior: the supervisor-owned storage context tracks active planner and executor streams. A terminal request while either stream remains open returns `WorkerChannelsActive`; once terminal handling begins, no new worker stream is admitted.
- Changed code and contract: [supervisor invocation lifecycle](../../engine/src/storage_manager.rs) and [storage-manager process boundary](../../specs/storage-manager.md).
- Verification: `cargo fmt --all`, workspace build, warning-denied Clippy, and `git diff --check` pass in the repository Nix shell. Tests were not run.
- Remaining: supervisor worker-exit observation must close these streams before requesting commit or abort; runtime race coverage awaits an authorized test run.

## SU-5 dependency: poll idle invocation deadlines

- Behavior: `SupervisorInvocation::check_deadline` lets the trusted scheduler enforce transaction age and accepted cancellation while planner/executor streams are idle; expiration or cancellation aborts through the managed transaction and releases its snapshot/admission permit.
- Changed code and contract: [supervisor invocation API](../../engine/src/storage_manager.rs) and [storage-manager lifecycle contract](../../specs/storage-manager.md).
- Verification: `cargo fmt --all`, workspace build, warning-denied Clippy, and `git diff --check` pass in the repository Nix shell. Tests were not run.
- Remaining: an implemented supervisor scheduler must call this method while worker channels are idle; the storage API cannot make that scheduling call itself.

## SU-5 dependency: paired worker-channel service loop

- Behavior: `SupervisorInvocation::serve_worker_channels` serves provisioned planner and executor Unix streams on scoped threads, polls deadline/cancellation every 100 ms while waiting for messages, closes the sibling endpoint after handler failure or panic, aborts on a handler panic, and joins both handlers before returning.
- Changed code and contract: [storage-side worker channel runner](../../engine/src/storage_manager.rs) and [storage-manager process boundary](../../specs/storage-manager.md).
- Verification: workspace formatting, build, warning-denied Clippy, and `git diff --check` pass in the repository Nix shell. Tests were not run.
- Remaining: the supervisor must create/provision these streams to the correct worker processes, observe worker exits, and request the terminal decision only after a successful worker result.

## SU-1 dependency: read-only token session authentication

- Behavior: `StorageManager::authenticate_query_session` verifies bearer-token validity and the current LOGIN/role closure on one read snapshot, returning a non-secret identity context without allocating an invocation or outcome. Execution START still revalidates through token-bound supervisor admission.
- Changed code and contract: [storage-manager authentication path](../../engine/src/storage_manager.rs) and [access contract](../../specs/storage-manager.md).
- Verification: workspace formatting, build, warning-denied Clippy, and `git diff --check` pass in the repository Nix shell. Tests were not run.
- Remaining: connect the method to a live gateway TCP service.

## SU-1 / SU-2 dependency: bind staging owner to authenticated session

- Behavior: successful storage AUTH now generates the opaque connection/session ID, and its identity context creates a staging owner bound to that ID and the storage-resolved principal. Callers cannot construct staging owners from arbitrary principal IDs outside the engine crate.
- Changed code and contracts: [storage authentication and session identity](../../engine/src/storage_manager.rs), [staging owner](../../engine/src/supervisor.rs), [storage access contract](../../specs/storage-manager.md), and [client AUTH contract](../../specs/client-protocol.md).
- Verification: Nix-shell formatting, workspace build, warning-denied Clippy, and `git diff --check` pass. Tests were not run.
- Remaining: implement strict HELLO/AUTH request decoding and connect the authenticated session to a gateway TCP connection.

## SU-1 dependency: real-redb AUTH session and revocation regression

- Behavior: a query connection authenticates against a real redb catalog, receives a stable nonzero session identity and staging owner, and revalidates before a protected operation. Revoking the token causes the next operation to fail authentication without changing the connection ID.
- Changed fixture: [client session AUTH regression](../../engine/tests/commit.rs).
- Verification: focused `cargo test -p prdb-engine --test commit client_session_authenticates_and_revalidates_before_operations --locked` passed in the repository Nix shell; formatting, workspace build, and warning-denied Clippy passed after the session implementation.
- Remaining: live gateway TCP service, supervisor process provisioning, and sandboxed end-to-end persistence/reconciliation evidence.

## SU-1 dependency: strict AUTH wire credentials

- Behavior: strict HELLO/AUTH JSON decoding validates the only token credential representation accepted by storage session authentication and prevents duplicate/unknown fields or alternate token encodings.
- Changed code and fixtures: [gateway protocol decoder](../../engine/src/client_protocol.rs) and [decoder tests](../../engine/tests/client_protocol.rs).
- Verification: focused `client_protocol` integration suite passed all 4 tests in the repository Nix shell.
- Remaining: live gateway service, supervisor process provisioning, and sandboxed end-to-end persistence/reconciliation evidence.

## SU-5 verification: paired worker drain before terminal commit

- Behavior: one token-admitted supervisor invocation serves planner and executor Unix streams concurrently; clean EOF on both drains the channels, after which the supervisor terminal request commits and storage records a durable COMMITTED outcome.
- Changed fixture: [real-redb paired worker stream regression](../../engine/tests/commit.rs).
- Verification: focused `cargo test -p prdb-engine --test commit supervisor_drains_both_worker_channels_before_durable_finalize --locked` passed in the repository Nix shell.
- Remaining: provision the streams to launched workers, observe their exit status, and require the worker process result before terminal commit.

## NP-2 / SM-6 dependency: handshake request dispatch

- Behavior: the request dispatcher handles HELLO/AUTH/PING frame state, rejects pre-HELLO requests and duplicate HELLO, and returns correlated sequence-zero responses. AUTH and operation revalidation use the storage session API.
- Changed code and fixture: [client handshake state](../../engine/src/client_protocol.rs) and [Unix-stream/storage integration regressions](../../engine/tests/commit.rs).
- Verification: full `cargo test --workspace --locked` passes; `cargo fmt --all -- --check`, workspace build, warning-denied Clippy, and `git diff --check` pass in the repository Nix shell.
- Remaining: implement TCP/gateway service and trusted gateway-to-supervisor IPC; launch confined worker processes and reconcile their outcomes.

## SU-2 validation: staging lifecycle and cancellation race

- Evidence: four unit tests cover chunk/owner rejection, immutable sealing, one-shot START, release, cancellation during metadata validation, disconnect and expiry cleanup.
- Test: [staging state-machine tests](../../engine/src/supervisor.rs).
- Verification: `cargo test -p prdb-engine --lib supervisor::tests --locked` passed.
- Remaining: live gateway upload flow and concurrency/quota stress under multiple sessions.

## SU-1 integration: protocol AUTH dispatch into storage

- Behavior: the HELLO/AUTH/PING frame handler routes AUTH through `StorageManager`, correlates a sequence-zero response, returns a nonsecret server session reference, and honors the strict JSON/base64 decoder.
- Changed code and test: [client protocol session dispatcher](../../engine/src/client_protocol.rs) and [real-redb AUTH handshake regression](../../engine/tests/commit.rs).
- Verification: focused handshake regression, workspace format/build, warning-denied Clippy, and `git diff --check` pass.
- Remaining: use the dispatcher in a real loopback TCP gateway and provision authenticated gateway-service IPC.

## SU-2 storage dependency: retain admission budget fields

- Behavior: staged submissions preserve client-requested workload class and deadline through sealing and one-shot START, with a positive configured ceiling.
- Changed code and contract: [staging request state](../../engine/src/supervisor.rs) and [supervisor resource policy](../../specs/supervisor.md).
- Verification: focused staging test rejects an over-ceiling deadline and observes the accepted analytical class and deadline at START.
- Remaining: apply admission budgets to live worker and planner scheduling.

## SM-6 / SU-3 dependency: failed pre-execution admission has no outcome

- Behavior: failed token authentication during invocation admission now releases its snapshot and admission slot without persisting ABORTED. Revocation after successful admission remains guarded through commit and conflicts durably.
- Changed code and contracts: [transaction admission cleanup](../../engine/src/transaction.rs), [storage admission](../../engine/src/storage_manager.rs), [client START contract](../../specs/client-protocol.md), and [token admission contract](../../specs/ddl-identity.md).
- Verification: focused `token_admission_checks_expiry_secret_revocation_and_commit_guards` passes in the repository Nix shell, confirming no outcome for rejected admissions and durable abort after post-admission revocation.
- Remaining: exercise the no-outcome rule through the sealed submission START flow and gateway status path.
- Verification: `cargo fmt --all -- --check`, workspace build, warning-denied Clippy, full `cargo test --workspace --locked`, and `git diff --check` pass in the repository Nix shell.

## SU-3 dependency: bind sealed START to token admission and deadline

- Behavior: query `start_submission` moves a sealed staging reservation through its one-shot START callback, revalidates the token, then opens a storage invocation with the sealed deadline. Scheduler polling aborts an expired invocation. Revoked credentials reject START, delete staged bytes, and create no invocation status or durable outcome.
- Changed code and fixture: [client start integration](../../engine/src/client_protocol.rs), [deadline-bound storage admission](../../engine/src/storage_manager.rs), and [real-redb staging/start regression](../../engine/tests/commit.rs).
- Verification: focused `sealed_start_admits_only_current_credentials_and_enforces_request_deadline` passes in the repository Nix shell.
- Remaining: launch/reap a confined worker, require its successful exit before finalization, and connect real gateway result/status flow.
- Full verification: `cargo fmt --all -- --check`, workspace build, warning-denied Clippy, full `cargo test --workspace --locked`, and `git diff --check` pass in the repository Nix shell.

## NP-3 dependency: frame upload into authenticated staging

- Behavior: after storage AUTH, binary artifact and parameter chunk frames are bound to the session's staging owner; chunk IDs, offsets, data lengths, and acknowledgements follow the v1 wire contract.
- Changed code and tests: [frame dispatcher](../../engine/src/client_protocol.rs), [protocol bounds](../../engine/tests/client_protocol.rs), and [real-redb auth/upload integration](../../engine/tests/commit.rs).
- Verification: focused protocol and handshake/upload tests pass.
- Remaining: wire PREPARE/SEAL schemas to staging and expose the flow through a live gateway connection.

## NP-3 dependency: prepare and seal through the authenticated session

- Behavior: strict PREPARE decoding validates exact fields, u32 version fields, byte declarations, canonical lowercase SHA-256/base64, workload class, and metadata bounds before staging reservation. SEAL validates a canonical invocation ID and drives staging digest, count, and admission-metadata checks before returning `prepared`.
- Changed code and tests: [client request handlers](../../engine/src/client_protocol.rs), [protocol regressions](../../engine/tests/client_protocol.rs), and [real-redb handshake/upload integration](../../engine/tests/commit.rs).
- Verification: focused protocol suite and real-redb HELLO/AUTH/PREPARE/chunk/SEAL regression pass in the repository Nix shell.
- Remaining: START frame dispatch, actual supervisor/gateway IPC, TCP transport, worker process lifecycle, and sandboxed execution evidence.

## NP-4 dependency: START admission response

- Behavior: one strict START frame claims a sealed reservation, revalidates storage credentials/current authority before creating a snapshot, and returns sequence-zero `accepted` alongside the supervisor-owned invocation.
- Changed code and test: [START frame path](../../engine/src/client_protocol.rs) and [current-credential/deadline real-redb regression](../../engine/tests/commit.rs).
- Verification: focused START regression passes in the repository Nix shell.
- Remaining: process launch/exit observation, streamed result and trusted terminal delivery, and gateway integration.

## NP-5 dependency: session-bound status and prepared cancellation

- Behavior: protected staging and status requests revalidate the storage session on every operation. Owner STATUS consults bounded staging first, then storage's live/durable outcome lookup; prepared CANCEL releases only unstarted staging and creates no outcome. Foreign and unknown IDs share the same unavailable/unknown behavior.
- Changed code and regression: [client status/cancel dispatch and revalidation](../../engine/src/client_protocol.rs), [real-redb authenticated flow](../../engine/tests/commit.rs).
- Verification: focused authentication and handshake/status/cancel regressions pass in the repository Nix shell.
- Remaining: owner-initiated active cancellation, admin control mode, TCP gateway and supervisor IPC, worker result/finalization lifecycle.
