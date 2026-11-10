# Storage Manager Progress

## SM-6 dependency: C executor stage-delete bridge — partial

- Contract: [IPC protocol](../../specs/ipc-protocol.md) mutation payload v1 and [client SDK](../../specs/client-sdk.md). A C query can submit a connection-local kind-8 row handle for deletion and must accept only the version/status-0 acknowledgement. Authority and row-version checks remain in trusted storage.
- Red/green evidence: the maintained executor fixture first failed to compile because the operation constant and SDK function were absent. The bridge now encodes version plus the 16-byte handle, calls operation `0x1112`, and rejects malformed acknowledgements and non-row tokens.
- Verification: `nix-shell -p rustc cargo clippy rustfmt tinycc --run 'make -f engine/sdk/Makefile test'` passed with TinyCC 0.9.27-unstable-2025-01-06. Base PATH lacked the toolchain; initial Nix access was denied by the sandbox and the authorized Nix shell command succeeded.
- Limits/next: this is only the delete bridge; INSERT and UPDATE C encoders are still absent. The test uses a host callback fixture and does not claim a live IPC/storage or sandbox integration. Next implement full-shape INSERT with VALUE/NULL/DEFAULT encoding and response handle validation.

## SM-6 dependency: C executor stage-insert bridge — partial

- Contract: [IPC protocol](../../specs/ipc-protocol.md) mutation payload v1 and [client SDK](../../specs/client-sdk.md). INSERT sends every declared field in schema order; each is VALUE, NULL, or DEFAULT, and successful storage returns a connection-local row handle. Storage still derives defaults, permissions, constraints, and indexes from its pinned catalog.
- Red/green evidence: the executor fixture first failed to compile because the INSERT input record and bridge were absent. It now compares the exact request bytes for a typed INTEGER 42, NULL, and DEFAULT, rejects a short caller buffer before IPC, and rejects a response with the wrong handle kind without publishing it to the caller.
- Verification: `nix-shell -p rustc cargo clippy rustfmt tinycc --run 'make -f engine/sdk/Makefile test'` passed with TinyCC 0.9.27-unstable-2025-01-06. This is an SDK callback fixture, not live storage IPC.
- Limits/next: UPDATE's C bridge and end-to-end sequential SQL mutation execution remain open. Next add UPDATE patch encoding with unique ordinal checks and ack validation; later SQL-4 needs fixed target materialization before staging writes.

## SM-6 dependency: C executor stage-update bridge — partial

- Contract: [IPC protocol](../../specs/ipc-protocol.md) mutation payload v1 and [client SDK](../../specs/client-sdk.md). UPDATE sends one row handle and unique zero-based ordinals, each with VALUE or NULL; DEFAULT and REDACTED are invalid. The existing Rust dispatcher and transaction path enforce row authority, old/new update policy checks, and snapshot/conflict rules.
- Red/green evidence: the executor fixture first failed to compile because the patch record and bridge were absent. It now verifies exact bytes for a NULL patch and an INTEGER 99 patch, rejects a non-success acknowledgement, duplicate ordinal, out-of-range ordinal, and undersized request without another host callback.
- Verification: `nix-shell -p rustc cargo clippy rustfmt tinycc --run 'make -f engine/sdk/Makefile test'` passed with TinyCC 0.9.27-unstable-2025-01-06 after correcting fixture ordinal/value offsets found by the byte comparison.
- Limits/next: low-level C insert/update/delete bridges now exist, but SQL still does not produce mutation requests and no live SDK-to-storage mutation stream is tested. Next close the SQL execution gap with fixed target materialization and sequential own-write visibility, retaining statement-error abort behavior.

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

## SM-2: bounded persisted index keys — implemented

- Contract: [index-key encoding v1](../../specs/index-key-encoding.md). Complete SQL secondary-index keys are capped at `2 * MAX_ROW_RECORD_BYTES + MAX_COLUMNS + 32` (2,101,280 bytes at the current limits), sufficient for worst-case escaping and fixed prefixes while bounded well below redb 4.4.0's 3 GiB key limit.
- Regression: a compiling real-redb commit test first accepted a key one byte above the proposed limit and committed its row. It now verifies rejection as `InvalidIndex`, unchanged commit sequence, and no row publication.
- Implementation: transaction staging and the serialized storage commit both reject over-limit keys, so direct trusted commit callers cannot bypass the logical bound. The low-level key composer remains an encoding helper and may construct bytes that cannot be persisted.
- Verification: the focused real-redb regression passed both the over-limit rejection and exact-limit acceptance cases. The full TinyCC suite and `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check` all passed in the configured Nix shell. The commit integration target passed all 106 tests.
- Remaining: independent composite/null/suffix properties and index-scan equivalence remain open.

## SM-2: typed component ordering evidence — verified

- Contract: [index-key encoding v1](../../specs/index-key-encoding.md). Within one index definition, encoded component bytes must preserve the declared scalar's SQL order.
- Regression: the first test run was rejected during setup because `i128::MIN` exceeds `DECIMAL(38)` precision. Replacing it with signed 38-digit values made the focused property pass.
- Evidence: the integration regression independently sorts representative BOOLEAN, signed integer, DECIMAL(38), TEXT, VARCHAR, BYTEA, UUID, DATE and TIMESTAMPTZ values by their native Rust order, then checks the encoded key bytes increase strictly.
- Verification: all 11 tests in `cargo test -p prdb-engine --test index_key --locked` passed; `cargo fmt --all -- --check`, workspace Clippy with warnings denied, and `git diff --check` also passed. The regression adds ordering evidence only; it changes no runtime implementation.
- Remaining: broader composite/null/suffix properties and persisted index-scan equivalence remain open.

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
- Real-redb regressions cover column-level `REFERENCES` with table `ALTER`, denial when either permission is missing, removal without target `REFERENCES`, durable outcomes, and atomic source/target epoch publication. The same authorized DDL test stages writers against both source and referenced tables before adding the FK, then verifies both old writers conflict and publish no rows after the paired epoch increments. With the FK present, DROP COLUMN attempts on both its referencing and referenced columns return `ColumnHasDependencies`, and DROP TABLE on the referenced table returns `TableHasDependencies`; each records an ABORTED outcome and preserves the FK/catalog records.
- Contracts: [schema DDL](../../specs/ddl-schema.md) and [permission DDL](../../specs/ddl-permissions.md).
- Verification: focused real-redb FK authority, dual-table stale-writer, FK-endpoint DROP COLUMN RESTRICT, and referenced-table DROP RESTRICT regressions passed. In the configured Nix shell, workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed for referenced-table rejection. Tool versions were Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06.
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

- Behavior: one token-admitted supervisor invocation serves planner and executor Unix streams concurrently; a planner `describe_relation` request for `information_schema.tables` receives a correlated successful description through the provisioned channel, and clean EOF on both drains the channels before the supervisor terminal request commits and storage records a durable COMMITTED outcome.
- Changed fixture: [real-redb paired worker stream regression](../../engine/tests/commit.rs).
- Verification: the focused regression passes in the repository Nix shell.
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

- Behavior: PREPARE, chunk, SEAL, STATUS, and CANCEL requests revalidate the storage session. Owner STATUS consults staging then live/durable storage status; CANCEL releases pre-start staging or marks an active owned invocation cancelled under the storage commit barrier. Foreign and unknown IDs share unavailable/unknown behavior.
- Changed code and regressions: [client request revalidation and dispatch](../../engine/src/client_protocol.rs), [owner cancellation transition](../../engine/src/storage_manager.rs), and [real-redb authenticated flow](../../engine/tests/commit.rs).
- Verification: focused status/cancel and active cancellation regressions plus warning-denied Clippy pass in the repository Nix shell.
- Remaining: admin control mode, TCP gateway and supervisor IPC, worker process/result/finalization lifecycle.

## IPC-6 dependency: gateway-role status/cancel dispatch

- Behavior: the storage-side dispatcher accepts only gateway-authorized client status/cancel operation tags, binds identity from trusted channel setup, and preserves non-enumerating unknown/foreign responses. Executor role use is rejected.
- Changed code and regression: [gateway control dispatcher](../../engine/src/supervisor_ipc.rs) and [real-redb IPC role test](../../engine/tests/commit.rs).
- Verification: focused role/status/cancel regression and warning-denied Clippy pass in the repository Nix shell.
- Remaining: provision channels and sessions over Unix sockets; route PREPARE/chunks/SEAL/START through IPC; build the TCP gateway and worker lifecycle.

## IPC-6 dependency: bounded gateway control socket loop

- Behavior: the trusted gateway control stream accepts at most 16 distinct request IDs, dispatches only the gateway's status/cancel operations, returns correlated frames, and closes cleanly at EOF.
- Changed code and regression: [control socket loop](../../engine/src/supervisor_ipc.rs) and [UnixStream service test](../../engine/tests/commit.rs).
- Verification: focused real-socket test and warning-denied Clippy pass in the repository Nix shell.
- Remaining: credential-bound session setup/generation, staging operations over IPC, and the independent TCP listener process.

## SU-3 dependency: immutable submission handoff after START

- Behavior: the staging store returns artifact and parameter bytes only as an owner-checked sealed handle after START succeeds. The handle shares immutable buffers and does not load or execute native code.
- Changed code and regression: [started submission handoff](../../engine/src/supervisor.rs) and [real-redb START lifecycle](../../engine/tests/commit.rs).
- Verification: focused START/handoff regression passes in the repository Nix shell.
- Remaining: pass the handle to a supervisor process launcher that establishes the required sandbox before loading the artifact.

## IPC-6 dependency: per-client gateway Unix service loop

- Behavior: one trusted gateway stream owns a client session through the entire upload/start sequence. It preserves request ordering, writes correlated responses, hands START's admission/immutable bytes back to supervisor code, and discards unstarted reservations or cancels started work on EOF and protocol/write failure.
- Changed code and test: [gateway Unix service loop and disconnect cleanup](../../engine/src/supervisor_ipc.rs), [authenticated owner helper](../../engine/src/supervisor.rs), and [real-redb UnixStream lifecycle](../../engine/tests/commit.rs).
- Verification: focused UnixStream test covers AUTH, PREPARE, artifact chunk, SEAL, STATUS, START, active CANCEL, EOF cleanup, and durable ABORTED outcome.
- Remaining: bind sockets to authenticated gateway process identity/generation and monitor network disconnects while workers execute; the TCP listener, launcher, sandbox, and result relay remain absent.

## SU-1 / IPC-6 dependency: HELLO over gateway session IPC

- Behavior: the first request on a provisioned per-client gateway stream is `OpenClientSession` (`0x140a`), which runs the strict HELLO handler and binds the selected mode to the stream's session before AUTH or submission operations. Identity and staging ownership continue to come from authenticated server-side state.
- Contract and implementation: [gateway IPC protocol](../../specs/ipc-protocol.md), [operation authorization and dispatch](../../engine/src/ipc.rs), and [gateway service dispatcher](../../engine/src/supervisor_ipc.rs).
- Changed regression: [UnixStream handshake and upload lifecycle](../../engine/tests/commit.rs) exercises HELLO, AUTH, PREPARE, upload, SEAL, STATUS, START, and CANCEL through the real service loop.
- Commands actually run in the configured Nix shell: `cargo fmt --all`; focused `cargo test -p prdb-engine --test commit --locked client_handshake_requires_hello_first_and_sequences_responses`; and `cargo fmt --all -- --check && cargo build --workspace --locked && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked && git diff --check`. All passed.
- Remaining: provision authenticated socket identity/generation, monitor network disconnects during worker execution, and implement the TCP listener, launcher, sandbox, and result relay.

## GW-1 dependency: loopback-default TCP listener

- Behavior: `TcpGatewayListener` binds loopback by default and refuses a non-loopback plaintext address unless `allow_non_loopback_plaintext` is explicitly enabled. Its API exposes the bound address and TCP accept operation without storage access.
- Contract and implementation: [gateway deployment boundary](../../specs/client-gateway.md), [client gateway plan](../client-gateway/README.md), and [listener](../../engine/src/gateway.rs).
- Changed tests: [gateway listener policy](../../engine/tests/gateway.rs).
- Observed red: focused compile failed because `prdb_engine::gateway` did not exist before the implementation.
- Commands actually run in the configured Nix shell: `cargo fmt --all`; focused `cargo test -p prdb-engine --test gateway --locked` (3 passed); and `cargo fmt --all -- --check && cargo build --workspace --locked && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked && git diff --check`. All passed.
- Remaining at that increment: bounded accepted-connection handling and frame read/write deadlines. Those controls are recorded in the later gateway resource entry; session processing and a separately runnable gateway process remain open.

## GW-2 / GW-3 dependency: TCP client to gateway-service frame bridge

- Behavior: the TCP adapter decodes bounded client frames, translates the supported HELLO/AUTH/PREPARE/chunk/SEAL/START/STATUS/CANCEL/PING kinds to the dedicated gateway IPC operation allowlist, adds an explicit chunk-kind byte, and returns only correlated sequence-zero RESPONSE frames. Unsupported kinds, malformed/pipelined frames, service EOF, and mismatched IPC responses return errors for the caller to handle by closing the connection.
- Contract and implementation: [client protocol](../../specs/client-protocol.md), [gateway service IPC](../../specs/ipc-protocol.md), and [TCP adapter](../../engine/src/gateway.rs).
- Changed regression: [loopback TCP to Unix IPC bridge](../../engine/tests/gateway.rs) checks HELLO and PING correlation across real sockets.
- End-to-end regression: [real-redb gateway submission lifecycle](../../engine/tests/commit.rs) now drives HELLO, AUTH, PREPARE, upload, SEAL, STATUS, START, and CANCEL from a loopback TCP client through the frame bridge and per-client Unix service loop, then verifies disconnect cleanup and durable ABORTED.
- Observed red: the initial focused test failed to compile because the TCP bridge and `PingClientSession` IPC operation were absent.
- Commands actually run in the configured Nix shell: `cargo fmt --all`; focused `cargo test -p prdb-engine --test gateway --locked tcp_client_hello_and_ping_bridge_over_the_gateway_service_channel` passed; and `cargo fmt --all -- --check && cargo build --workspace --locked && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked && git diff --check`. All passed.
- Remaining: run bounded concurrent accepted connections with read/write deadlines; implement ADMIN and explicit sanitized ERROR responses; connect a separately runnable gateway process to authenticated service-channel provisioning and result relay.

## GW-1 dependency: bounded gateway connection admission

- Behavior: a shared atomic limiter issues RAII permits up to a positive configured connection maximum; bounded accept drops peers above that cap. The TCP/Unix stream serving helper sets positive idle read/write timeouts before frame processing.
- Changed implementation and regression: [listener limits and timeout bridge](../../engine/src/gateway.rs) and [shared connection-cap test](../../engine/tests/gateway.rs).
- Observed red: the focused test initially failed to compile because the shared limiter and bounded accept API were absent; a subsequent compile exposed and corrected the test's tuple destructuring.
- Commands actually run in the configured Nix shell: `cargo fmt --all`; focused `cargo test -p prdb-engine --test gateway --locked` (5 passed); and `cargo fmt --all -- --check && cargo build --workspace --locked && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked && git diff --check`. All passed.
- Remaining at that increment: add an absolute deadline across the complete pre-authentication phase. Per-frame deadlines, per-IP connection rate limiting, and the fixed bounded listener worker pool are implemented in later entries; the phase deadline is recorded below.

## GW-1 / GW-2 dependency: bounded TCP gateway worker pool

- Behavior: one listener process accepts TCP peers into a bounded queue served by a fixed number of workers. Each worker opens one private Unix service stream, applies idle read/write timeouts to both ends, and forwards the client session. Shutdown stops accepting, drains queued clients, and joins all workers.
- Changed implementation and integration regression: [gateway runtime](../../engine/src/gateway.rs) and [TCP/Unix worker-pool test](../../engine/tests/gateway.rs).
- Observed red: initial runtime test compilation failed because `GatewayRuntimeConfig` and `run_gateway` were absent.
- Commands actually run in the configured Nix shell: `cargo fmt --all`; focused `cargo test -p prdb-engine --test gateway --locked gateway_worker_pool_opens_one_service_stream_per_client` passed; and `cargo fmt --all -- --check && cargo build --workspace --locked && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked && git diff --check`. All passed.
- Remaining at that increment: verify the Unix service peer and bind service connections to gateway generation; add pre-auth absolute deadlines, rate limits, ADMIN forwarding, result relay, and process startup configuration. Peer UID verification is recorded in the following entry.

## GW-2 dependency: Linux Unix-service peer UID verification

- Behavior: `UnixGatewayServiceConnector` connects only when Linux `SO_PEERCRED` reports the configured expected UID; mismatched peers fail with `PermissionDenied` before the connection is handed to the frame bridge.
- Changed implementation and regression: [peer-checked Unix connector](../../engine/src/gateway.rs) and [local Unix socket identity test](../../engine/tests/gateway.rs).
- Observed red: the focused test did not compile before the connector API existed. A later compile caught a test assertion using equality on `std::os::unix::net::SocketAddr`; the assertion was removed because successful connector verification is the behavior under test.
- Commands actually run in the configured Nix shell: `cargo fmt --all`; focused `cargo test -p prdb-engine --test gateway --locked unix_gateway_connector_checks_peer_uid` passed; and `cargo fmt --all -- --check && cargo build --workspace --locked && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked && git diff --check`. All passed.
- Remaining: bind service connections to gateway generation and per-client connection ID; verify socket filesystem ownership/mode; add authenticated handoff on non-Linux platforms if they enter the supported profile.

## GW-3 / SU-1 / SU-2 validation: real-redb TCP admission and upload path

- Behavior: a loopback TCP client now drives the real-redb session and staging state through the gateway adapter and its Unix IPC service channel. The regression covers HELLO, AUTH, PREPARE, artifact upload, SEAL, STATUS, START, active CANCEL, EOF cleanup, and durable ABORTED status.
- Changed regression: [gateway lifecycle integration](../../engine/tests/commit.rs).
- Focused command: `cargo test -p prdb-engine --test commit --locked client_handshake_requires_hello_first_and_sequences_responses` passed.
- Full configured Nix-shell checks: `cargo fmt --all -- --check && cargo build --workspace --locked && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked && git diff --check` all passed.
- Remaining: worker process launch/reap, result data and terminal relay, sandbox enforcement, and status reconciliation after gateway/process failure.

## GW-1 dependency: runnable gateway process wrapper

- Behavior: `prdb-gateway` parses a loopback-default bind address, requires a private service socket and expected peer UID, configures bounded worker/queue/idle values, installs graceful SIGINT/SIGTERM shutdown, and refuses plaintext wildcard binding without an explicit flag.
- Changed binary and CLI regressions: [gateway process](../../engine/src/bin/prdb-gateway.rs) and [gateway CLI tests](../../engine/tests/gateway.rs).
- Observed red: the CLI test initially failed to compile because Cargo had no `prdb-gateway` target.
- Commands actually run in the configured Nix shell: `cargo fmt --all`; focused `cargo test -p prdb-engine --test gateway --locked` (9 passed); and `cargo fmt --all -- --check && cargo build --workspace --locked && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked && git diff --check`. All passed.
- Remaining at that increment: provide the supervisor-side Unix listener and bind gateway generation/session context; connect result relay and control ADMIN operations. The listener is recorded in the following entry.

## GW-2 dependency: bounded supervisor-side gateway service listener

- Behavior: the supervisor binds a private mode-0600 Unix socket, accepts only the configured Linux peer UID, caps active sessions, applies idle read/write timeouts, creates a fresh `ClientConnectionSession` per stream, and returns to request handling after the START callback hands admission to trusted scheduling code.
- Changed implementation and integration test: [supervisor gateway listener](../../engine/src/supervisor_ipc.rs) and [real-redb HELLO over the private listener](../../engine/tests/commit.rs).
- Observed red: the focused listener test failed to compile because the listener configuration, binder, and accept loop did not exist.
- Commands actually run in the configured Nix shell: `cargo fmt --all`; focused `cargo test -p prdb-engine --test commit --locked gateway_service_listener_binds_peer_and_serves_hello` passed; and `cargo fmt --all -- --check && cargo build --workspace --locked && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked && git diff --check`. All passed.
- Remaining: bind both sides to a gateway generation and connection ID, exercise listener through `prdb-gateway` in a separate process, and implement worker scheduling/result relay/control ADMIN.

## GW-1 dependency: absolute client-frame deadline

- Behavior: when serving a network connection with configured timeouts, the TCP adapter bounds each complete request frame by an absolute deadline, resetting the deadline only after a complete request has been processed. Partial-frame clients cannot extend the deadline by sending occasional bytes.
- Changed implementation and regression: [gateway frame reader](../../engine/src/gateway.rs) and [slow partial-frame test](../../engine/tests/gateway.rs).
- Observed red: focused slow-client coverage was added before the deadline path; it previously had only idle socket timeouts and no whole-frame deadline.
- Commands actually run in the configured Nix shell: focused `cargo test -p prdb-engine --test gateway --locked incomplete_tcp_client_frame_hits_absolute_read_deadline` passed; and `cargo fmt --all -- --check && cargo build --workspace --locked && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked && git diff --check`. All passed.
- Remaining at that increment: enforce one absolute unauthenticated phase deadline across HELLO/AUTH, and rate-limit by source address. The per-IP connection cap is implemented in the following entry.

## GW-1 dependency: bounded per-IP connection rate

- Behavior: the gateway applies a rolling-window connection-attempt limit per source IP before queueing a socket. Its remembered source map is capped at 4096 entries and prunes expired histories; when full, new sources fail closed. The process wrapper exposes the per-IP count.
- Changed implementation and regression: [gateway peer limiter and runtime integration](../../engine/src/gateway.rs), [CLI option](../../engine/src/bin/prdb-gateway.rs), and [deterministic window/source-cap test](../../engine/tests/gateway.rs).
- Observed red: focused test compilation failed because `GatewayPeerRateLimiter` did not exist. The first run then exposed a test expectation inconsistent with the one-source cap; the corrected test passes.
- Commands actually run in the configured Nix shell: `cargo fmt --all`; focused `cargo test -p prdb-engine --test gateway --locked per_ip_gateway_rate_limit_is_bounded_and_expires` passed; and `cargo fmt --all -- --check && cargo build --workspace --locked && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked && git diff --check`. All passed.
- Remaining at that increment: add an absolute deadline across the complete unauthenticated HELLO/AUTH phase and track auth failures without retaining credentials. The phase deadline is recorded in the following entry.

## GW-1 dependency: absolute unauthenticated phase deadline

- Behavior: the TCP bridge starts one deadline when client serving begins and applies it across HELLO, any pre-auth PING requests, and AUTH. Successful server-side AUTH ends this deadline; per-frame and idle limits continue afterward.
- Changed implementation and regression: [gateway bridge pre-auth state](../../engine/src/gateway.rs) and [HELLO plus phase-expiry socket test](../../engine/tests/gateway.rs).
- Observed red: the new regression failed to compile because the bridge had no combined idle/frame and pre-auth timeout API.
- Commands actually run in the configured Nix shell: `cargo fmt --all`; focused `cargo test -p prdb-engine --test gateway --locked unauthenticated_gateway_phase_has_one_absolute_deadline` passed; and `cargo fmt --all -- --check && cargo build --workspace --locked && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked && git diff --check`. All passed.
- Remaining: rate-limit failed AUTH attempts separately and test gateway/service restarts with live sessions.

## GW-4 / SU-4 dependency: START result and terminal event relay

- Behavior: after START acknowledgement, the TCP bridge accepts only request-correlated result-data or terminal IPC events, maps them to RESULT_DATA/TERMINAL, and assigns increasing network sequence numbers. Supervisor helpers bound provisional data and emit TERMINAL only after storage's correlated commit/abort response succeeds; ambiguous failures emit no terminal claim.
- Contract and implementation: [gateway event IPC](../../specs/ipc-protocol.md), [TCP event relay](../../engine/src/gateway.rs), and [storage-confirmed terminal sender](../../engine/src/supervisor_ipc.rs).
- Changed tests: [network event ordering/correlation](../../engine/tests/gateway.rs) and [real-redb terminal evidence](../../engine/tests/commit.rs).
- Focused verification: `cargo test -p prdb-engine --test gateway --locked start_gateway_bridge_relays_provisional_data_then_terminal`, `cargo test -p prdb-engine --test commit --locked gateway_terminal_event_requires_a_storage_confirmed_decision`, and the real TCP/IPC lifecycle test `cargo test -p prdb-engine --test commit --locked client_handshake_requires_hello_first_and_sequences_responses` passed.
- Full validation in the configured Nix shell passed: `cargo fmt --all -- --check`, `cargo build --workspace --locked`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check`.
- Remaining: connect real confined worker output/completion to these events, bound result queues/backpressure across processes, and test cancellation/restart around streaming terminal transitions.

## SU-5 dependency: worker exit observation and reaping

- Behavior: after planner and executor streams have drained, `SupervisorInvocation::observe_worker_exit` polls the associated process while checking cancellation and age. The sandbox path uses `observe_worker_exit_in_cgroup`, which kills descendants and waits for an empty cgroup before authorizing terminal storage. `serve_terminal` enforces that the worker exited successfully before commit, and no new worker channel can open after exit observation starts or succeeds. A nonzero exit or process-inspection error durably aborts; deadline failure kills and reaps the worker.
- Contract and implementation: [supervisor lifecycle contract](../../specs/storage-manager.md) and [worker process observation](../../engine/src/storage_manager.rs).
- Changed real-redb regression: [failed worker exit records ABORTED and is reaped](../../engine/tests/commit.rs).
- Observed red: the terminal-authority regression first failed to compile because no `WorkerExitNotObserved` state existed. The existing worker-observation regression also established nonzero exit as a durable ABORTED result.
- Verification: focused worker-failure and worker-channel/terminal tests passed. Full configured Nix-shell validation passed after cgroup cleanup integration: `cargo fmt --all -- --check`, `cargo build --workspace --locked`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check`.
- Remaining: launch a worker with a verified sandbox profile, bind worker channels to that process, relay its result bytes, and test cancellation/expiry during the process wait.

## SU-5 validation: cancellation and expiry during worker wait

- Behavior: accepted owner cancellation and the admitted execution deadline are both observed while a worker process is idle; the supervisor kills/reaps the worker, and storage records ABORTED before terminal handling can commit.
- Changed real-redb regression: [cancellation during process observation](../../engine/tests/commit.rs).
- Verification: focused `supervisor_cancellation_and_deadline_during_worker_wait_abort` passed for both paths. Full configured Nix-shell validation passed: format check, workspace build, warning-denied Clippy, workspace tests, and `git diff --check`.
- Remaining: exercise these paths with cgroup descendant cleanup and live cgroup availability.

## SU-6 validation: reconcile after terminal delivery loss

- Behavior: storage commits before the supervisor writes TERMINAL; if the gateway side disconnects and terminal delivery fails, the durable COMMITTED outcome remains available to the authenticated owner through `get_invocation_status`.
- Changed regression: [gateway disconnect after storage commit](../../engine/tests/commit.rs).
- Verification: focused real-redb `gateway_disconnect_after_storage_commit_is_reconcilable` passed. Full configured Nix-shell validation passed: `cargo fmt --all -- --check`, `cargo build --workspace --locked`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check`.
- Remaining: exercise the same reconciliation across separate gateway/supervisor processes and restart, and prove no automatic replay occurs.

## SU-1 / IPC-6 dependency: gateway submission IPC dispatcher

- Behavior: a role-checked gateway dispatcher maps AUTH, PREPARE, chunk, SEAL, START, STATUS, and CANCEL operation tags to the strict per-client session handlers. Chunk IPC prepends an explicit artifact/parameter kind byte. START returns the network acknowledgement plus a typed storage invocation and immutable submission handle to trusted supervisor code; those authority-bearing objects are never serialized.
- Contract: [gateway submission IPC payloads](../../specs/ipc-protocol.md).
- Changed code and integration regression: [gateway request dispatcher](../../engine/src/supervisor_ipc.rs) and [real-redb full submission path](../../engine/tests/commit.rs).
- Verification: focused real-redb HELLO/AUTH and IPC submission lifecycle test passes in the repository Nix shell.
- Remaining: run this dispatcher from a provisioned, bounded Unix service loop and keep launch/results in a separate supervisor process.

## SU-1 dependency: gateway failed-authentication throttling

- Behavior: the gateway allows a configurable number of failed AUTH attempts per source IP in a rolling 60-second window (default 10). It reserves in-flight attempts before forwarding AUTH, releases reservations only on successful authentication, caps retained source histories at 4,096, and fails closed when the cap is full. The source address is only a throttle key; credential bytes are not retained.
- Contract and implementation: [gateway authentication and connection limits](../../specs/client-gateway.md), [storage authentication boundary](../../specs/storage-manager.md), [gateway limiter and runtime](../../engine/src/gateway.rs), and [gateway configuration](../../engine/src/bin/prdb-gateway.rs).
- Regression evidence: deterministic limiter tests verify threshold, source-map cap, and expiry; a real TCP/Unix-socket gateway regression confirms the second failed AUTH is closed before reaching the supervisor service. The CLI exposes the configured limit.
- Verification: focused limiter, live gateway, and CLI tests passed. Full configured Nix-shell validation passed: format check, workspace build, warning-denied Clippy, all workspace tests, and `git diff --check`.
- Remaining: gateway restart loses in-memory throttle history; invalid-authentication throttling is per source address, while network-level denial-of-service protection remains a deployment concern. The gateway/supervisor process startup and sandboxed worker launch are still incomplete.

## SM-5 / NP-5 dependency: administrator point status

- Behavior: a storage administrator can look up any invocation's live or retained terminal status after current database-administrator authority is checked from a fresh catalog snapshot. Ordinary owner lookup remains owner-scoped. Control-mode client STATUS uses this path; query-mode STATUS continues to consult only the caller's staging and storage state.
- Contract and implementation: [storage status authority](../../specs/storage-manager.md), [control protocol status](../../specs/client-protocol.md), [StorageManager lookup](../../engine/src/storage_manager.rs), and [control-mode session handler](../../engine/src/client_protocol.rs).
- Regression evidence: the real-redb administrative runtime test verifies a current administrator can see another owner's active invocation and retained ABORTED outcome, a non-administrator is denied, and a missing ID remains unavailable. The same test exercises both point statuses through an authenticated control-mode session.
- Verification: focused real-redb test passed in the configured Nix shell.
- Remaining at that increment: control-mode CANCEL was still unwired; the following increment records its audited implementation. ADMIN operations, explicit sanitized ERROR responses, and process-separated service integration remain open.

## SM-5 / NP-5 dependency: administrator control-mode cancellation

- Behavior: control-mode CANCEL revalidates the authenticated session, creates a fresh audit event, and delegates to `StorageManager::cancel_invocation`, which checks current administrator authority, applies the per-actor request limit, serializes against COMMITTING, and persists the cancellation decision. Query-mode prepared/owner cancellation keeps its existing owner-scoped path.
- Contract and implementation: [client cancellation semantics](../../specs/client-protocol.md), [audited storage cancellation](../../specs/storage-manager.md), and [control session handler](../../engine/src/client_protocol.rs).
- Regression evidence: the real-redb control-mode test cancels a live invocation and confirms its administrative status exposes `cancellation_requested = true`.
- Verification: focused test and full configured Nix-shell validation passed: format check, workspace build, warning-denied Clippy, all workspace tests, and `git diff --check`.
- Remaining: `ADMIN` operation envelopes/dispatch, sanitized protocol ERROR responses, and the separate supervisor process remain open.

## SM-5 / NP-5 dependency: read-only token inventory ADMIN operation

- Behavior: control-mode ADMIN accepts the strict version-1 `tokens.list` read envelope, revalidates the connection before execution, checks owner/MANAGE_USERS/database-administrator authority in storage, and returns at most 64 token metadata records with an ID cursor. Verifier and bearer-secret material are omitted.
- Contract and implementation: [client protocol](../../specs/client-protocol.md), [bounded token page](../../engine/src/storage_manager.rs), [ADMIN decoder and session handler](../../engine/src/client_protocol.rs), and gateway IPC kind mapping.
- Regression evidence: decoder coverage rejects prepare-phase mutation envelopes and unknown arguments; a real-redb authenticated control-mode session exercises the gateway IPC ADMIN dispatch for another user's inventory and verifies secret material is absent.
- Remaining at that increment: runtime invocation listing and other ADMIN operations, and the separate supervisor process remained open. Runtime invocation listing is recorded below.

## SM-5 / NP-5 dependency: prepared token revocation ADMIN operation

- Behavior: ADMIN PREPARE reserves one random operation ID and token ID on the control connection; START must present that ID on the same connection. START consumes the reservation before calling the audited `StorageManager::revoke_token` path. The storage layer performs the current `MANAGE_USERS` check and commits the revocation with its audit and durable operation outcome. Unknown, already revoked, and unauthorized tokens retain the storage API's shared failure.
- Contract and implementation: [tokens.revoke ADMIN contract](../../specs/client-protocol.md), [strict ADMIN command decoder and per-session operation reservation](../../engine/src/client_protocol.rs), and gateway operation dispatch.
- Regression evidence: decoder tests cover both phase schemas; a real-redb gateway-dispatch regression uses a separate `MANAGE_USERS` principal to prepare/start revocation and confirms the target metadata becomes revoked.
- Remaining: reservation behavior after failures before a durable outcome, other ADMIN operations, and the separate supervisor/worker process remain open.

## SU-1 / GW-2 dependency: sanitized client ERROR responses

- Behavior: failed gateway request dispatches produce fixed-category, fixed-message ERROR payloads over a dedicated supervisor-to-gateway IPC response event. The gateway maps them to client frame kind 101 and closes the connection. Locally rejected client kinds and authentication throttles return similarly fixed responses. Raw internal errors are not serialized.
- Contract and implementation: [client ERROR frame](../../specs/client-protocol.md), [gateway service event](../../specs/ipc-protocol.md), [gateway bridge](../../engine/src/gateway.rs), and [supervisor error classifier](../../engine/src/supervisor_ipc.rs).
- Regression evidence: gateway bridge regression verifies the error kind, request ID, fixed payload, and close behavior.
- Remaining: deliver errors for malformed frame headers where a trustworthy request ID is unavailable, map every transport failure category, and exercise real service errors across separate gateway/supervisor processes.

## SM-5 / NP-5 dependency: one-time token issuance ADMIN operation

- Behavior: `tokens.issue` uses the connection-owned PREPARE/START reservation. START revalidates the session and invokes audited `StorageManager::issue_token`. The committed response returns token ID and padded base64 secret once; the encoding, JSON construction buffer, TCP frame buffer, and gateway-service response buffer are zeroized after handoff.
- Contract and implementation: [one-time issuance contract](../../specs/client-protocol.md), [strict command schema and issuance handler](../../engine/src/client_protocol.rs), [service response cleanup](../../engine/src/supervisor_ipc.rs), and [TCP frame cleanup](../../engine/src/gateway.rs).
- Regression evidence: the real-redb ADMIN dispatch issues a credential, decodes it through the regular AUTH schema, authenticates a new session with it, and confirms stored inventory contains metadata only.
- Remaining at that increment: lost-response reconciliation for token mutations is recorded below; other ADMIN operations and separate supervisor/worker integration remain open.

## SM-5 / NP-5 dependency: administrator live invocation pages

- Behavior: control-mode ADMIN accepts `runtime.invocations.list` with a bounded page limit and an opaque cursor bound to the current administrator and process-local registry generation. Every page revalidates the control session and database-administrator authority. Results expose bounded invocation metadata and best-effort consistency; cursors become invalid after registry changes or restart.
- Contract and implementation: [runtime listing protocol](../../specs/client-protocol.md), [cursor encoding and bounded storage page](../../engine/src/storage_manager.rs), and [ADMIN decoder and response handler](../../engine/src/client_protocol.rs).
- Regression evidence: decoder coverage round-trips a canonical cursor. The real-redb test exercises first and second ADMIN pages through gateway dispatch, checks the next invocation and cancellation state, and separately covers unauthorized callers, stale cursors, page limits, and terminal status.
- Remaining: remaining ADMIN operations and separate supervisor/worker process integration.

## SM-5 / NP-5 dependency: reconcile a lost ADMIN mutation reply

- Behavior: after a prepared token revocation commits, an authorized database administrator can query STATUS with the operation ID and retrieve its durable committed outcome. The client does not repeat START when the original response is unavailable.
- Regression evidence: the real-redb ADMIN lifecycle test discards the revocation response payload, then queries the operation ID from both a separate token-manager session and a database-administrator control session; each receives `committed`.
- Remaining: other ADMIN operations and separate supervisor/worker process integration.

## SM-5 / DDL-identity dependency: prepared CREATE USER ADMIN operation

- Behavior: `users.create` uses the connection-owned PREPARE/START flow. START creates a random stable user ID, atomically publishes its LOGIN principal and matching named object, records DDL audit/outcome, and returns the user ID. The user receives no data privileges or credential; `tokens.issue` provisions login separately.
- Contract and implementation: [identity DDL protocol](../../specs/client-protocol.md), [storage-manager identity lifecycle](../../engine/src/storage_manager.rs), and [ADMIN session handling](../../engine/src/client_protocol.rs).
- Regression evidence: decoder tests accept canonical names and reject uppercase names. A real-redb `MANAGE_USERS` session creates the user, issues a token, authenticates a new query session with it, and confirms token inventory is attached to the created user.
- Verification: format check, workspace build, warning-denied Clippy, all workspace tests, and `git diff --check` passed in the configured Nix shell.
- Remaining at that increment: role creation is recorded below; other user lifecycle DDL, remaining ADMIN operations, and separate supervisor/worker process integration remain open.

## SM-5 / DDL-identity dependency: prepared CREATE ROLE ADMIN operation

- Behavior: `roles.create` uses connection-owned PREPARE/START. Storage checks `MANAGE_ROLES`/database-administrator authority and atomically writes a generated non-login role principal and matching named object with DDL audit/outcome. Roles start without memberships or privileges.
- Contract and implementation: [identity DDL protocol](../../specs/client-protocol.md), [shared storage principal creation](../../engine/src/storage_manager.rs), and [ADMIN session handling](../../engine/src/client_protocol.rs).
- Regression evidence: decoder accepts normalized role names. A real-redb database-administrator session creates the role, and the test reads its persisted principal record to verify role kind and disabled LOGIN state.
- Verification: format check, workspace build, warning-denied Clippy, all workspace tests, and `git diff --check` passed in the configured Nix shell.
- Remaining at that increment: role membership DDL is recorded below; other user lifecycle DDL, remaining ADMIN operations, and separate supervisor/worker process integration remain open.

## SM-5 / DDL-identity dependency: role membership ADMIN operations

- Behavior: `roles.grant` and `roles.revoke` use the connection-owned PREPARE/START flow. Storage atomically changes the membership and dependency records, increments the member authority epoch, and delegates authorization, endpoint validation, cycle checks, and last-administrator protection to the established membership DDL commit path.
- Contract and implementation: [membership protocol](../../specs/client-protocol.md), [storage membership lifecycle](../../engine/src/storage_manager.rs), and [ADMIN session handling](../../engine/src/client_protocol.rs).
- Regression evidence: decoder tests cover grant and revoke schemas. A real-redb ADMIN lifecycle grants and revokes a newly created role for a new user, then authenticates again to confirm role closure changes are visible.
- Verification: format check, workspace build, warning-denied Clippy, all workspace tests, and `git diff --check` passed in the configured Nix shell.
- Remaining: other user lifecycle DDL, privilege grant/revoke ADMIN operations, remaining ADMIN operations, and separate supervisor/worker process integration.

## SM-5 / DDL-identity dependency: system privilege ADMIN operations

- Behavior: `privileges.grant` and `privileges.revoke` use connection-owned PREPARE/START. Storage atomically changes the database-scoped grant and dependency edge, increments the grantee's authority epoch, and delegates authorization to the current database-administrator check.
- Contract and implementation: [system privilege ADMIN protocol](../../specs/client-protocol.md), [storage-manager contract](../../specs/storage-manager.md), [storage lifecycle](../../engine/src/storage_manager.rs), and [ADMIN dispatch](../../engine/src/client_protocol.rs).
- Regression evidence: decoder coverage checks grant/revoke schemas. The real-redb ADMIN test grants `MANAGE_ROLES` to a newly created user and verifies that the user's token-backed session can then create a role.
- Focused verification: formatting and the client-protocol decoder and real-redb ADMIN lifecycle tests passed in the configured Nix shell. Workspace checks remain to run.
- Remaining: user rename/login-state/drop ADMIN operations, remaining ADMIN operations, and separate supervisor/worker process integration.

## SM-5 / DDL-identity dependency: user and role rename ADMIN operations

- Behavior: `users.rename` and `roles.rename` use connection-owned PREPARE/START. Storage updates the principal name and authority epoch with the matching named object and object version in one authorized DDL commit.
- Contract and implementation: [identity ADMIN protocol](../../specs/client-protocol.md), [storage-manager contract](../../specs/storage-manager.md), [identity lifecycle](../../engine/src/storage_manager.rs), and [ADMIN dispatch](../../engine/src/client_protocol.rs).
- Regression evidence: strict decoder tests cover both operation schemas. The real-redb identity lifecycle test exercises `StorageManager::rename_identity` under `MANAGE_USERS`, alongside the authorization path's existing rename validation.
- Remaining: login-state and DROP ADMIN operations, remaining ADMIN operations, and separate supervisor/worker process integration.

## SM-5 / DDL-identity dependency: user LOGIN state ADMIN operation

- Behavior: `users.login` uses connection-owned PREPARE/START and changes the LOGIN flag and authority epoch. Disabling LOGIN revokes every outstanding token in the same commit; enabling LOGIN does not restore tokens.
- Contract and implementation: [login-state protocol](../../specs/client-protocol.md), [storage-manager contract](../../specs/storage-manager.md), [identity lifecycle](../../engine/src/storage_manager.rs), and [ADMIN dispatch](../../engine/src/client_protocol.rs).
- Regression evidence: decoder coverage checks the strict boolean-string schema. A real-redb identity lifecycle regression verifies that disabling a user revokes its active token atomically, and that a change cannot remove the last login-enabled administrator.
- Remaining: DROP ADMIN operations, remaining ADMIN operations, and separate supervisor/worker process integration.

## SM-5 / DDL-identity dependency: user and role DROP ADMIN operations

- Behavior: `users.drop` and `roles.drop` use connection-owned PREPARE/START and delegate RESTRICT validation to the authorized identity commit path. User drops revoke outstanding credentials, delete the principal/object pair, and add a permanent tombstone atomically.
- Contract and implementation: [DROP ADMIN protocol](../../specs/client-protocol.md), [storage-manager contract](../../specs/storage-manager.md), [identity lifecycle](../../engine/src/storage_manager.rs), and [ADMIN dispatch](../../engine/src/client_protocol.rs).
- Regression evidence: decoder coverage checks both strict operation schemas. Real-redb tests exercise successful user/role drops, user credential revocation and tombstones, plus rejection of a drop while a privilege grant references the user.
- Remaining: remaining ADMIN operations and separate supervisor/worker process integration.

## DS-5 / SM-5: empty schema DROP RESTRICT — verified

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [client protocol](../../specs/client-protocol.md), and [storage manager](../../specs/storage-manager.md). The owner, a member of an owning role, or a database administrator can remove an empty non-system schema. Child objects and remaining dependency edges reject the request. A second emptiness check runs under the serialized redb writer to linearize against concurrent table creation.
- Red evidence: the new schema lifecycle test first called a compiling `drop_schema` stub and failed with `SchemaLifecycleError::InvalidRequest`.
- Implementation: [authorized schema drop commit path](../../engine/src/commit.rs), [storage entry point](../../engine/src/storage_manager.rs), and connection-owned [`schemas.drop` PREPARE/START](../../engine/src/client_protocol.rs). Schema and object records, DDL audit, and durable outcome publish atomically.
- Regression evidence: the real-redb lifecycle test covers owner success, unauthorized rejection, a populated-schema RESTRICT rejection with durable ABORTED outcome, and record deletion. A barrier-coordinated create/drop race accepts only a complete table-creation win or complete schema-drop win, then reopens through startup validation. The gateway ADMIN lifecycle test creates and drops an empty schema through PREPARE/START; the decoder test checks the strict schema-drop request.
- Verification: in the configured Nix shell, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` all passed. `git diff --check` passed.
- Remaining: table/column rename, default and nullability changes, schema ownership transfer, full constraint/admin wiring, and separate supervisor/gateway process integration remain incomplete.
- Next smallest behavior: implement one supported table or column alteration with snapshot/fence validation.

## DS-5 / SM-5: table DROP RESTRICT — implemented

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [client protocol](../../specs/client-protocol.md), and [storage manager](../../specs/storage-manager.md). The table owner or a database administrator can drop a table if no external dependency remains. The DDL commit removes owned table/column/constraint/index/policy definitions, expressions, grants and dependency edges, table rows, and physical index entries atomically. Cleanup is bounded to 65,536 rows, 65,536 index entries, and 64 MiB combined row tuple payload plus physical index key and locator bytes per commit.
- Implementation: [authorized table-drop commit path](../../engine/src/commit.rs), [storage entry point](../../engine/src/storage_manager.rs), and connection-owned [`tables.drop` PREPARE/START](../../engine/src/client_protocol.rs).
- Regression evidence: real-redb tests verify unauthorized rejection, atomic rejection of 65,537 rows followed by successful table removal at exactly 65,536 rows, and conflict for a writer pinned before table drop. The successful drop also removes an authorized table grant and its authority dependency while advancing the grantee's authority epoch. Table-owned row policy and CHECK constraint fixtures verify removal of each typed definition, expression, object records, policy target, and dependency edges. A barrier-coordinated race between a staged writer commit and table drop accepts either commit order, checks durable outcomes, and confirms no orphan row or table record remains. Byte-boundary tests reject 64 MiB plus one byte and accept exactly 64 MiB for both tuple payload bytes alone and the combined tuple payload, physical index-key, and locator bytes; they check row/catalog deletion and startup validation. The stale writer publishes no row and records a durable ABORTED outcome. The two-index cleanup boundary rejects 65,538 memberships atomically, then removes both indexes at exactly 65,536 entries; the test checks table/column/object/catalog deletion, both physical index directions, and startup validation. The FK authorization lifecycle also confirms that dropping a referenced table is rejected with `TableHasDependencies`, leaves its FK and table records intact, and records a durable ABORTED outcome. The strict protocol decoder accepts the exact `table_id` request, and the real-redb gateway ADMIN lifecycle test exercises PREPARE/START and the committed response.
- Verification: focused table-drop row, byte, index-entry, pinned-writer, and FK RESTRICT regressions passed. In the configured Nix shell, workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed for the payload accounting fix and byte-boundary test. Tool versions were Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06.
- TDD note: the exact byte-boundary test first failed after the planner fix because serialized snapshot revalidation still counted row-record framing; both planning and commit-time revalidation now count decoded tuple payload bytes. The table-grant regression first exposed that DROP TABLE neither advanced a grantee's authority epoch nor allowed the resulting principal update through its restricted catalog-write guard; the cleanup now advances each affected grantee once and the authorization guard accepts only the corresponding one-step epoch changes. The policy cleanup regression first failed with `InvalidCatalog` because the planner decoded policy-target records using the generic envelope parser; DROP TABLE now uses the dedicated policy-target decoder. The row/index-entry tests were added after the original implementation and have no observed failing behavior run to record.
- Remaining: DROP TABLE coverage for remaining constraint combinations and separate supervisor/gateway process integration remain open.

## DS-5 / SM-5: referenced-table epoch on FK owner drop — verified

- Behavior and normative contracts: [storage manager](../../specs/storage-manager.md), [schema DDL](../../specs/ddl-schema.md), and the table DROP RESTRICT lifecycle above. Dropping the table that owns a foreign key removes the FK and its target dependency atomically and advances the referenced table's write epoch once, so a writer pinned to its earlier constraint graph cannot commit afterward.
- Red evidence: the real-redb owner-table drop regression first failed with `InvalidCatalog`; final catalog validation showed the referenced table epoch had not advanced when the owned FK disappeared.
- Implementation: [table-drop write set](../../engine/src/storage_manager.rs) updates each distinct referenced table epoch with the cleanup. [Commit validation and authorization](../../engine/src/commit.rs) identify the actual deleted table record, permit only one-step epoch-only updates for tables referenced by FKs owned by that table, and treat a table record as deleted only when its payload is removed.
- Regression evidence: [storage_manager_drop_referencing_table_removes_owned_foreign_key](../../engine/tests/commit.rs) removes two source-table foreign keys to the same target, verifies both dependency edges are deleted, preserves the target constraint and index, advances the referenced-table epoch exactly once, records a durable COMMITTED outcome, and passes startup validation. A writer staged against the referenced table before the drop conflicts afterward, records ABORTED, and publishes no row.
- Verification: the focused test passed. In the configured Nix shell, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed; all 103 commit integration tests passed. `git diff --check` passed. Tool versions were Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06.
- Remaining: other constraint variants and separate supervisor/gateway process integration remain open.

## DS-5 / SM-5: owned NOT NULL, PRIMARY KEY, and UNIQUE cleanup on table drop — verified

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md) and [storage manager](../../specs/storage-manager.md). Dropping a table removes its internally generated NOT NULL constraint and the column's nullable metadata, plus owned PRIMARY KEY and UNIQUE definitions, backing index objects, kind-4 dependencies, and forward/reverse physical index memberships atomically with its rows.
- Red evidence: no behavioral failure was observed for this added coverage; the existing table-drop path already implemented index cleanup. The new acceptance test makes owned constraint/index cleanup observable against persisted physical memberships.
- Regression evidence: [storage_manager_drop_table_removes_owned_primary_and_unique_indexes](../../engine/tests/commit.rs) first makes a nullable column NOT NULL through the storage-manager lifecycle, then adds both key constraints. It commits a row through `InvocationTransaction` so both physical index directions contain entries, drops the table, checks all constraint/index/column records and dependencies are gone, verifies the row and memberships are removed, and reopens through startup validation.
- Verification: the focused real-redb test passed. In the configured Nix shell, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed; all 104 commit integration tests passed. `git diff --check` passed.
- Remaining: DROP TABLE coverage for remaining constraint combinations and separate supervisor/gateway process integration remain open.

## DS-5 / SM-5: self-referential FK cleanup on table drop — verified

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md) and [storage manager](../../specs/storage-manager.md). A self-referential foreign key is owned by the table it references; DROP TABLE removes that FK, its target dependency, and its owned table/index definitions atomically rather than treating the internal reference as an external RESTRICT dependency.
- Red evidence: no behavioral failure was observed. The first fixture attempt was rejected during catalog setup because its column and object names differed; after correcting the fixture and publishing the primary key before the FK, the lifecycle test passed.
- Regression evidence: [storage_manager_drop_table_removes_self_referential_foreign_key](../../engine/tests/commit.rs) creates a table with a primary key, adds a foreign key from that table back to its own key, commits a row whose FK value resolves to itself, and then drops the table through `StorageManager`. It checks removal of table/column/constraint/index objects, both dependency edges, the row, and both physical index directions, then confirms a durable COMMITTED outcome and validates startup.
- Verification: the focused real-redb test passed. In the configured Nix shell, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed; all 105 commit integration tests passed. `git diff --check` passed.
- Remaining: DROP TABLE coverage for remaining constraint combinations and separate supervisor/gateway process integration remain open.
- Next smallest behavior: review remaining storage-manager validation gaps and address the next unfinished acceptance case before process-level integration.

## DS-5 / SM-5: column DROP RESTRICT — implemented

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [client protocol](../../specs/client-protocol.md), and [storage manager](../../specs/storage-manager.md). The table owner or a database administrator can drop an unreferenced column from a non-system table. Expressions, constraints, indexes, and policies that reference the column block the operation. Storage removes the column and its default expression/grants/dependency edges, compacts ordinals, advances schema version and write epoch, rewrites tuples, and rebuilds surviving index keys in one bounded fenced commit.
- Implementation: [authorized column-drop commit path](../../engine/src/commit.rs), [storage entry point](../../engine/src/storage_manager.rs), and connection-owned [`tables.drop_column` PREPARE/START](../../engine/src/client_protocol.rs). The rewrite budget counts old and rewritten tuple payload bytes, excluding row-record framing.
- Regression evidence: a real-redb test inserts a two-column row, creates an index on the retained column, verifies RESTRICT rejects dropping that indexed column with a durable aborted outcome, then drops the other column while a writer is pinned to the old schema. The writer conflicts, publishes no row, and records an aborted outcome. The FK authorization lifecycle also verifies RESTRICT blocks dropping either FK endpoint. The test checks ordinal compaction, schema version bump, retained tuple value, committed outcome, and that the surviving index locator advances to the rewritten row version while its logical key and reverse membership remain valid. A separate row-boundary regression rejects 65,537 rows atomically and accepts exactly 65,536, then verifies retained values, schema version, durable outcome, and startup validation. The byte-boundary regression rejects 64 MiB plus one byte atomically and accepts exactly 64 MiB of combined tuple payloads, then verifies the retained value and startup validation. A strict ADMIN decoder test checks the two-ID request.
- TDD note: the behavior test first failed with `InvalidRequest` against the compiling storage-manager stub, then passed after the schema rewrite path was implemented. The exact byte-boundary regression first failed because the commit path included row-record framing in its tuple-payload budget; the implementation now measures the decoded old tuple payload consistently with the contract.
- Verification: focused real-redb rewrite, RESTRICT, index-locator, row-boundary, and byte-boundary regressions passed. In the configured Nix shell, workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed for the row and byte boundary coverage. Tool versions were Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06.
- Remaining: external-FK/constraint/policy dependency regressions, index/constraint DDL race coverage, and separate supervisor/gateway process integration remain open.

## DS-5 / SM-5: table rename — implemented

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [client protocol](../../specs/client-protocol.md), and [storage manager](../../specs/storage-manager.md). The table owner or a database administrator can rename a non-system table. The stable table ID and tuple schema version remain fixed; the table write epoch and object version advance. Schema-local name uniqueness is checked inside the serialized final catalog commit.
- Implementation: [authorized table-rename commit path](../../engine/src/commit.rs), [storage entry point](../../engine/src/storage_manager.rs), and connection-owned [`tables.rename` PREPARE/START](../../engine/src/client_protocol.rs).
- Regression evidence: the real-redb lifecycle test verifies name/object update, stable table identity, unchanged schema version, incremented write epoch, duplicate-name rejection with a durable aborted outcome, and subsequent row/index operations through a column drop. The strict decoder test accepts the exact table ID and normalized name shape.
- TDD note: the rename assertion first failed with `InvalidRequest` against the compiling storage stub; the storage path and authorized commit guard now pass it.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check` all passed in the configured Nix shell; the focused storage and decoder tests passed as well.
- Remaining: column/table rename gateway dispatch coverage, role-owner authorization regression, DDL stale-writer race, and other supported alterations remain open.

## DS-5 / SM-5: column nullability changes — implemented

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [client protocol](../../specs/client-protocol.md), and [storage-manager contract](../../specs/storage-manager.md). The table owner or a database administrator can set or drop NOT NULL through `tables.alter_column_nullability`. Storage keeps the tuple schema version, increments the table write epoch, validates persisted rows before setting NOT NULL, and adds/removes the internal constraint atomically under the table DDL fence.
- Implementation: [authorized constraint DDL path](../../engine/src/commit.rs), [storage entry point](../../engine/src/storage_manager.rs), and connection-owned PREPARE/START in [client protocol](../../engine/src/client_protocol.rs).
- Regression evidence: the real-redb test verifies that an existing NULL row rejects SET NOT NULL with a durable aborted outcome and leaves catalog nullability unchanged; after removing that row, a writer staged against the nullable schema conflicts after SET NOT NULL, publishes no row, and records an aborted outcome. SET then DROP NOT NULL updates the column, constraint/object records, and write epoch while preserving schema version. Startup validation succeeds afterward. The decoder accepts the strict operation shape and rejects an invalid nullable value.
- TDD note: the storage behavior assertion initially failed against the stub returning `InvalidRequest`, then passed through the authorized storage manager path.
- Verification: in the configured Nix shell, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check` all passed. The focused storage and protocol decoder regressions passed.
- Remaining: stale-writer race coverage for other DDL paths, role-owner authorization regression, and separate supervisor/gateway process integration remain open.

## DS-5 / SM-5: column rename — implemented

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [client protocol](../../specs/client-protocol.md), and [storage-manager contract](../../specs/storage-manager.md). The table owner or a database administrator can rename a column on a non-system table. Storage preserves column ID, ordinal, tuple schema version, and dependencies; updates the family 6 and family 2 names and object version; and advances the table write epoch. Table-local uniqueness is checked against the serialized final catalog.
- Implementation: [authorized column-rename commit path](../../engine/src/commit.rs), [storage entry point](../../engine/src/storage_manager.rs), and connection-owned PREPARE/START in [client protocol](../../engine/src/client_protocol.rs).
- Regression evidence: the real-redb test verifies duplicate-name rejection with a durable aborted outcome and successful name agreement in both catalog records; a writer staged against the previous column schema conflicts after rename, publishes no row, and records an aborted outcome. The gateway ADMIN lifecycle test exercises `tables.rename_column` PREPARE/START, and the strict decoder test checks the request shape.
- TDD note: the behavior test first failed with `InvalidRequest` against the compiling storage stub. Initial implementation exposed the family 6 name tag and schema-epoch behavior, which were corrected to match the catalog contract.
- Verification: in the configured Nix shell, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check` all passed. Focused real-redb, protocol decoder, and gateway ADMIN PREPARE/START tests passed.
- Remaining: stale-writer coverage for default changes, DROP COLUMN, and index/constraint DDL, plus separate supervisor/gateway process integration.

## DS-5 / SM-5: current column default changes — implemented

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [client protocol](../../specs/client-protocol.md), and [storage-manager contract](../../specs/storage-manager.md). Owners or database administrators can set, replace, or drop a non-generated column's current insert default on non-system tables. Typed literal, NULL, and UUID current-principal defaults use the create-table encoding. Existing rows and schema version remain unchanged; the table write epoch advances, and replaced/dropped expression catalog records and dependency edges are removed atomically. The immutable add-column default is preserved.
- Implementation: [authorized constraint DDL guard](../../engine/src/commit.rs), [storage entry point](../../engine/src/storage_manager.rs), and connection-owned `tables.set_column_default` / `tables.drop_column_default` PREPARE/START in [client protocol](../../engine/src/client_protocol.rs).
- Regression evidence: the real-redb storage regression exercises set, replacement, drop, and rejection of an absent-default drop. A writer staged before a default change conflicts, publishes no row, and records an aborted outcome. The strict decoder checks both command shapes; the gateway ADMIN lifecycle test runs set and drop through PREPARE/START.
- TDD note: the first storage behavior assertion failed against the `InvalidRequest` stub; the catalog expression/object/dependency changes then passed through the serialized authorized DDL path.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check` all passed in the configured Nix shell. Focused storage, decoder, and gateway ADMIN PREPARE/START regressions passed.
- Remaining: stale-writer coverage for DROP COLUMN and index/constraint DDL, plus separate supervisor/gateway process integration.

## DS-5 / DS-7: table rename versus a pinned writer — verified

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [MVCC concurrency](../../specs/concurrency-mvcc.md), and [storage manager](../../specs/storage-manager.md). A transaction that has loaded the table schema and indexes and staged a row against that snapshot must not commit after table rename advances the write epoch.
- Regression evidence: the real-redb table lifecycle test stages a valid tuple under the pinned schema, renames the table, then verifies commit returns `CommitError::Conflict`, the row is absent, and the invocation has a durable ABORTED outcome.
- Implementation: no code change was needed. The existing schema/object exact-byte guards and shared DDL fence reject the stale commit.
- Verification: focused lifecycle regression passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check` all passed in the configured Nix shell.
- Remaining: stale-writer coverage for column rename, nullability/default changes, add/drop column, and index/constraint DDL; role-owner authorization regression; separate supervisor/gateway process integration.

## DS-5 / SM-5: table ADD COLUMN — implemented

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [client protocol](../../specs/client-protocol.md), and [storage manager](../../specs/storage-manager.md). The table owner or a database administrator can append a non-generated column to a non-system table. NOT NULL requires a typed non-null literal; nullable columns without a default backfill NULL. Context-dependent defaults are rejected. Storage advances schema version and write epoch, rewrites existing tuples, and updates index definitions and locators atomically under the DDL fence. Row rewrites are bounded to 65,536 rows and 64 MiB of combined old and rewritten tuple payloads.
- Implementation: [serialized add-column validation and row rewrite](../../engine/src/commit.rs), [storage manager entry point](../../engine/src/storage_manager.rs), and connection-owned `tables.add_column` PREPARE/START in [client protocol](../../engine/src/client_protocol.rs).
- Regression evidence: the real-redb test seeds a row, builds an index, adds a NOT NULL text column with a literal default, then adds a nullable text column without a default. It checks both backfilled values, updated index schema versions, and locators for each rewritten row. A writer staged against the old schema conflicts after ADD COLUMN, publishes no row, and records an aborted outcome. Large real-redb cases reject 65,537 rows atomically then accept exactly 65,536 rows, and reject a rewrite totaling 64 MiB plus one byte then accept exactly 64 MiB; startup validation succeeds after accepted rewrites. Decoder coverage accepts the exact operation and rejects a missing required default and `CURRENT_PRINCIPAL_ID`. The gateway ADMIN lifecycle test exercises PREPARE/START and the committed response.
- TDD note: the storage behavior regression first failed with `InvalidRequest` against the stub and passed after the serialized DDL/rewrite path was added.
- Verification: the focused real-redb backfill/index regression, strict decoder test, and gateway ADMIN lifecycle test passed. In the configured Nix shell, workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed. Tool versions were Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06.
- Remaining: separate supervisor/gateway process integration.

## DS-5 / SM-5 / DS-7: table ownership transfer — implemented

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [client protocol](../../specs/client-protocol.md), and [storage manager](../../specs/storage-manager.md). The current owner or a database administrator can transfer a non-system table to an existing user or role. The table object owner/version and table write epoch update atomically; tuple schema, rows, indexes, and grants stay unchanged. The new owner receives table DDL authority without data privileges.
- Implementation: the storage entry point and serialized authority guard update only the table object and table record, validate the target principal, reject system schemas, and guard actor/owner/membership inputs. `tables.set_owner` uses connection-owned ADMIN PREPARE/START.
- Regression evidence: the real-redb lifecycle transfers to a role, checks there are no table data-grant records for that role, confirms an inherited member can rename the table, rejects an unrelated user's transfer with a durable ABORTED outcome, and verifies a writer pinned before the transfer aborts with `Conflict` and publishes no row. The gateway ADMIN lifecycle and strict request decoder cover PREPARE/START and the exact argument shape.
- TDD note: the ownership behavior assertion first failed with `InvalidRequest` against a compiling storage-manager stub; it passes through the authorized DDL path.
- Verification: focused real-redb, decoder, and gateway lifecycle tests passed. Workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed in the configured Nix shell. Tool versions were Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06.
- Remaining: DDL limit boundaries, other stale-writer races, and separate supervisor/gateway process integration.

## DS-5: schema ownership transfer — implemented

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [client protocol](../../specs/client-protocol.md), and [storage manager](../../specs/storage-manager.md). A schema owner or database administrator can transfer a non-system schema to a live user or role. The owner object and version update atomically; schema contents stay unchanged. Schema owners can create tables and manage the schema without gaining table data privileges.
- Implementation: the storage entry point updates the schema object; its serialized authorization guard validates actor and target principals, role membership, system-schema mappings, schema record, and audit. The `schemas.set_owner` operation uses connection-owned ADMIN PREPARE/START. Table creation now recognizes current schema ownership as DDL authority.
- Regression evidence: real-redb coverage transfers the schema to a role, checks owner/version, rejects the previous non-admin owner's drop with a durable ABORTED outcome, and verifies an inherited member can create and drop a table, rename the schema, and receives no table data grants. Decoder and gateway ADMIN lifecycle tests cover the exact request and committed result.
- TDD note: the schema transfer assertion first failed with `InvalidRequest` against the compiling storage-manager stub; it passes through the serialized owner guard.
- Verification: focused real-redb, decoder, and gateway lifecycle tests passed. Workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed in the configured Nix shell. Tool versions were Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06.
- Remaining at this milestone: additional stale-writer and limit boundary evidence, and separate supervisor/gateway process integration.

## DS-5: schema rename — implemented

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [client protocol](../../specs/client-protocol.md), and [storage-manager contract](../../specs/storage-manager.md). The schema owner or a database administrator can rename a non-system schema. Storage preserves the schema ID, schema record, and child objects while updating the owned object name and version atomically. Final name uniqueness is checked by the serialized catalog writer.
- Implementation: `StorageManager::rename_schema` and its authorized commit guard update only the schema object. The `schemas.rename` operation uses connection-owned ADMIN PREPARE/START.
- Regression evidence: the real-redb lifecycle test verifies duplicate-name rejection, successful rename with a stable schema ID and incremented object version, and denial to the previous non-admin owner. Decoder and gateway lifecycle tests cover the exact request and committed result.
- TDD note: the schema rename assertion first failed with `InvalidRequest` against the compiling storage stub; the rename and uniqueness checks now pass through the serialized authorized DDL path.
- Verification: focused real-redb, decoder, and gateway lifecycle tests passed. Workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed in the configured Nix shell. Tool versions were Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06.
- Remaining: role-owner transfer regression, additional stale-writer and limit boundary evidence, and separate supervisor/gateway process integration.

## DS-5 / DS-7: index publication versus a pinned writer — verified

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [MVCC concurrency](../../specs/concurrency-mvcc.md), and [storage manager](../../specs/storage-manager.md). Standalone index create, drop, and rebuild advance each affected table's write epoch without changing tuple schema version, so writers pinned to an older index set cannot publish after DDL.
- Implementation: [`commit.rs`](../../engine/src/commit.rs) appends expected-byte table-record epoch updates to the same serialized DDL commit for affected tables that do not already have a table-record update. Combined schema rewrites retain their existing single epoch increment.
- Regression evidence: real-redb lifecycle tests stage row writes before index creation and rebuild, then confirm both conflict with durable ABORTED outcomes and publish no rows. The create/drop lifecycle also checks exact table write-epoch increments. Rebuild assertions verify the old snapshot retains its index definition and memberships while the current snapshot contains only rebuilt forward and reverse memberships.
- TDD note: the new stale-writer assertion first failed because the pinned write committed successfully at sequence 3 after index publication; it passes after index DDL began incrementing the table write epoch.
- Verification: focused real-redb create/drop and rebuild stale-writer regressions passed. In the configured Nix shell, workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed after the create/drop fix. Tool versions were Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06.
- Remaining: separate supervisor/gateway process integration and broader DDL acceptance evidence.

## DS-5 / DS-7: CHECK constraint publication versus a pinned writer — verified

- Behavior and normative contracts: [schema DDL](../../specs/ddl-schema.md), [MVCC concurrency](../../specs/concurrency-mvcc.md), and [storage manager](../../specs/storage-manager.md). A writer pinned before a CHECK constraint is published must not commit using the older table constraint epoch.
- Regression evidence: the real-redb authorized constraint lifecycle stages a tuple against the existing table schema, publishes a CHECK constraint and table write-epoch increment, then verifies the staged writer conflicts, publishes no row, and records an ABORTED outcome.
- Implementation: no code change was needed; the existing serialized constraint DDL path and explicit table epoch update reject the stale commit.
- TDD note: the focused race regression passed against the current implementation.
- Verification: focused regression passed. In the configured Nix shell, workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed for this increment. Tool versions were Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06.
- Remaining: other constraint variants and separate supervisor/gateway process integration.

## DS-5 / DS-7: PRIMARY KEY and UNIQUE publication versus a pinned writer — verified

- Behavior: a writer that loaded a table's schema and indexes before PRIMARY KEY and UNIQUE constraints were published cannot commit a row using the stale constraint graph.
- Regression: the real-redb [constraint and table-drop lifecycle test](../../engine/tests/commit.rs) stages a row before the combined PK/UNIQUE catalog commit, then verifies the stale writer receives `Conflict`, publishes no row, and records an ABORTED outcome.
- Implementation: no production change was needed; the existing serialized constraint DDL path advances the table write epoch and the commit guard rejects the stale writer.
- Verification: the focused regression passed in the configured Nix shell.
- Remaining: separate supervisor/gateway process integration and broader DDL acceptance evidence remain open.

## DS-5 / DS-7: FOREIGN KEY removal versus pinned source and target writers — verified

- Behavior: removing a FOREIGN KEY advances both the source and referenced table write epochs, so writers pinned to the prior relationship graph cannot publish rows afterward.
- Regression: the real-redb [FK authority lifecycle test](../../engine/tests/commit.rs) stages one row on each table before authorized FK removal; both writers receive `Conflict`, publish no row, and record ABORTED outcomes.
- Implementation: no production change was needed; the existing serialized foreign-key DDL path protects both table epochs.
- Verification: the focused real-redb FK authority regression passed in the configured Nix shell.
- Remaining: separate supervisor/gateway process integration and broader DDL acceptance evidence remain open.

## DS-5: role-owned table authority follows current membership — verified

- Behavior: a role member can use owner-derived table DDL while membership is active, then loses that authority after an administrator revokes the membership.
- Regression: the real-redb [table rename and ownership lifecycle test](../../engine/tests/commit.rs) transfers ownership to a role, verifies the member can rename the table, revokes membership through a credentialed database administrator, and confirms the former member's next rename is denied with an ABORTED outcome and leaves the table name unchanged.
- Test setup: the initial attempt to revoke membership from the fixture's non-credentialed owner was correctly denied because changing a role's membership requires an authorized administrative caller. The fixture now provisions a credentialed database administrator and performs the revocation through the supported API.
- Verification: the focused real-redb lifecycle test passed in the configured Nix shell.
- Remaining: separate supervisor/gateway process integration and broader DDL acceptance evidence remain open.

## SM-6: periodic bounded submission expiry — implemented

- Behavior and normative contract: [supervisor staging lifecycle](../../specs/supervisor.md). Expired pre-start reservations must be reaped even while the service receives no client traffic, and each maintenance pass must have a bounded scan cost.
- Implementation: `StagingStore::reap_expired(max_scanned)` scans an ordered reservation map with a rotating cursor, visits at most the caller's budget, and eventually covers all live reservations. Opportunistic staging operations use a 256-entry pass. The gateway service runs the same 256-entry reaper on each accept-loop iteration and stops if the staging state is poisoned.
- Red evidence: disabling the service-loop reaper made `idle_gateway_service_periodically_reaps_expired_staging` fail: closing the expired connection returned its still-live reservation ID. Restoring periodic maintenance made the test pass. The bounded-pass regression creates three expired reservations, confirms one is returned per one-entry pass, and checks all are unavailable afterward.
- Verification: focused reaper and idle-service tests passed. In the prescribed Nix shell, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check` passed. The workspace had 22 passing library unit tests; the commit integration target had 109 passing and 2 pre-existing cgroup-dependent tests ignored.
- Remaining: production manifest validation and a supervisor executable still depend on the manifest format and process admission contracts left open in the specs; gateway/supervisor process-level execution remains incomplete.

## SM-6: requested-scope decoding and enforcement — implemented

- Behavior and normative contracts: [client SDK](../../specs/client-sdk.md), Requested Capability Scope v1; [client protocol](../../specs/client-protocol.md), PREPARE/SEAL/START; [authorization](../../specs/authorization.md); and [storage manager](../../specs/storage-manager.md). The client's requested scope is a ceiling intersected with current catalog-derived grants at START, never a grant source.
- Implementation: added the bounded canonical `PSC1` decoder in [submission_scope.rs](../../engine/src/submission_scope.rs), validates it during staging SEAL, decodes again at START, and applies the intersection to every transaction-built `AuthorizationContext` in [transaction.rs](../../engine/src/transaction.rs). The supervisor admission path accepts the decoded scope in [storage_manager.rs](../../engine/src/storage_manager.rs), and both legacy and executor launch START paths pass it from [client_protocol.rs](../../engine/src/client_protocol.rs).
- Red evidence: removing the grant-intersection filter made `invocation_scope_intersects_requested_capabilities_with_catalog_grants` fail because DELETE authority outside the request remained present. Removing the SEAL decoder check made `sealing_rejects_invalid_v1_scope_even_when_custom_validator_accepts_it` fail because malformed `scope` sealed successfully. Both passed with their checks restored.
- Verification: canonical decoder, malformed-input, seal, START, and scope-intersection regressions passed. In the prescribed Nix shell, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, `cargo test --workspace --locked`, and `git diff --check` passed. The suite included 27 passing library unit tests and 109 commit integration tests; 2 cgroup-dependent sandbox tests remain ignored.
- Decisions: `PSC1` uses a big-endian u16 count, strict bytewise record ordering, nonzero IDs, five operation codes, three target kinds, and a 200-capability ceiling (6,806-byte maximum encoding).
- Remaining: manifest v1 encoding/compatibility validation, service executable integration, and sandboxed scope-to-storage acceptance remain open.

## SM-6: manifest v1 compatibility and ELF dependency validation — implemented

- Behavior and normative contracts: [client SDK](../../specs/client-sdk.md), Artifact Manifest v1; [client protocol](../../specs/client-protocol.md), PREPARE/SEAL; [supervisor staging](../../specs/supervisor.md); and [toolchain](../../specs/toolchain.md). Admission accepts only canonical `PMF1` Linux x86_64 manifests for configured CPU/runtime/SDK profiles and dependencies.
- Implementation: added bounded `PMF1` decoding and a configured `VersionedSubmissionMetadataValidator`. SEAL now supplies its immutable artifact bytes to metadata validation; the production validator parses ELF64 program/dynamic tables without loading or executing the library, rejects loader search/audit/filter tags, and requires the actual `DT_NEEDED` set to equal the declared allowlisted dependencies.
- Red/green evidence: the manifest test suite covers unsupported target/ABI/protocol/symbols, malformed/trailing/unsorted/duplicate data, profile and dependency rejection, malformed ELF, prohibited dynamic tags, and mismatch between manifest and artifact dependencies. Focused tests pass with validation enabled.
- Red/green check: temporarily removing the manifest-versus-ELF dependency comparison made `elf_dependency_reader_checks_loaded_names_without_loading_the_artifact` fail because a manifest listing libc and libm accepted an ELF listing only libc. Restoring the comparison made it pass.
- Verification: Rust/Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, TinyCC 0.9.27-unstable-2025-01-06 were verified in the prescribed Nix shell. Workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed. The engine library had 33 passing unit tests; the commit target had 109 passing and 2 pre-existing cgroup/TinyCC fixture tests ignored.
- Decisions: manifest encoding uses the 28-byte `PMF1` header and bounded canonical body specified in the SDK contract. Deployment-specific CPU/runtime profile and dependency allowlist remain configuration inputs, since the repository has not selected its final TinyCC/runtime profile.
- Remaining: supervisor executable integration, production profile configuration, and sandboxed scope-to-storage acceptance remain open.

## SM-6: manifest validation through staging SEAL — verified

- Behavior and normative contracts: [client protocol](../../specs/client-protocol.md), PREPARE/SEAL; [client SDK](../../specs/client-sdk.md), Artifact Manifest v1; and [supervisor staging](../../specs/supervisor.md). SEAL must validate the artifact bytes that were uploaded and pinned in that reservation.
- Regression: [submission_manifest.rs](../../engine/src/submission_manifest.rs) now stages canonical metadata and ELF fixtures through `StagingStore::prepare`, `upload_chunk`, and `seal`. The matching dependency set seals successfully; an ELF missing a declared dependency is rejected as `InvalidMetadata`.
- Red/green evidence: temporarily making SEAL call metadata-only `validate` caused the mismatch reservation to seal successfully and the new test failed at the expected assertion. Restoring `validate_artifact` makes it pass.
- Verification: focused regression passed in the prescribed Nix shell. Workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed. The engine library had 34 passing unit tests; the commit target had 109 passing and 2 pre-existing cgroup/TinyCC fixture tests ignored.
- Remaining: supervisor executable integration, production compatibility-profile configuration, and execution inside the sandbox remain open.

## SM-6: verify ABI exports before SEAL — implemented

- Behavior and normative contracts: [client SDK](../../specs/client-sdk.md), Artifact Manifest v1 and its acceptance plan; [supervisor staging](../../specs/supervisor.md). A manifest naming ABI entry points is accepted only when the pinned ELF exports both symbols with the required ELF symbol kinds.
- Implementation: the bounded ELF64 dynamic-table reader now requires and validates `DT_HASH`, `DT_SYMTAB`, and `DT_SYMENT` when checking an artifact. It walks bounded dynamic symbols and accepts the descriptor only as a defined externally visible OBJECT and the entry point only as a defined externally visible FUNC. No loader or constructor is invoked.
- Red/green evidence: before the check, the validator accepted an artifact missing `prdb_query_descriptor_v3`; `elf_dependency_reader_checks_loaded_names_without_loading_the_artifact` failed at the expected assertion. The test also covers an object symbol exported with FUNC type. Both pass with the check enabled.
- Toolchain evidence: the configured TinyCC 0.9.27-unstable-2025-01-06 output for a shared object using `puts` contained `libc.so.6`, `ld-linux-x86-64.so.2`, both SysV/GNU hash tables, and the required ABI-named exports. A temporary Rust validation harness accepted it when the manifest and profile declared those dependencies.
- Verification: manifest-focused tests and full workspace build, formatting, warning-denied Clippy, all workspace tests, and `git diff --check` passed. The engine library had 34 passing unit tests; the commit target had 109 passing and 2 pre-existing cgroup/TinyCC fixture tests ignored.
- Remaining: supervisor executable integration and confined query execution acceptance remain open.

## SU-6: release staging reservation after START callback — implemented

- Behavior and normative contract: [supervisor lifecycle](../../specs/supervisor.md). Once the trusted START callback returns, the completed invocation's immutable artifact and parameters must stop consuming per-connection and per-principal staging quota before the same connection is serviced again.
- Implementation: the gateway service captures the authenticated staging owner and invocation ID before handing control to the START callback, then calls `StagingStore::finish_started` on callback return. A staging cleanup error closes that service stream.
- Red/green evidence: `started_callback_releases_reservation_before_same_session_resumes` first failed because a second PREPARE on the same connection returned `InvalidState`; after the callback cleanup path was added, the next PREPARE succeeds with a new invocation ID.
- Verification: focused lifecycle regression passed. Workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed. The engine library had 35 passing unit tests; the commit target had 109 passing and 2 pre-existing cgroup/TinyCC fixture tests ignored.
- Remaining: the production callback that runs the sandboxed executor and resolves its durable terminal outcome is still not provided by a supervisor executable.

## SU-5 / GW-2: provide the live storage manager to START callbacks — implemented

- Behavior and normative contracts: [supervisor lifecycle](../../specs/supervisor.md), steps 4–6; [gateway service IPC](../../specs/ipc-protocol.md), START and terminal events. Trusted START handling must confirm outcomes through the same validated storage instance owned by the recovery loop.
- Implementation: `serve_gateway_service_listener` and `run_gateway_service_with_recovery` pass the current `StorageManager` reference into their START callback. When storage reports an ambiguous commit and the recovery loop reopens the database, later callbacks receive the new validated manager rather than reopening a second database handle.
- Regression/build evidence: all in-repository listener and recovery callback call sites compile against the new signature; focused supervisor IPC unit tests passed. Workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed. The engine library had 35 passing unit tests; the commit target had 109 passing and 2 existing cgroup/TinyCC sandbox tests ignored.
- Remaining: provide a supervisor executable that wires the callback to executor result relay, durable finalization, and its configured compatibility profile; exercise the gateway/supervisor boundary across processes.

## SM-6: authenticated pre-invocation schema discovery — implemented

- Behavior and normative contract: [metadata discovery](../../specs/metadata-discovery.md). A client can build a requested scope before invocation admission from schemas visible to its authenticated principal.
- Implementation: `StorageManager::discover_token_authorized_schemas` opens a request-scoped transaction snapshot, validates the token against that snapshot, derives catalog schema USAGE visibility, and consumes transaction-local pages internally. It caps full results at 4,096 records and 1 MiB, returns no partial result on overflow, and creates no invocation admission or durable outcome.
- Regression: the credential lifecycle integration test now verifies that a valid token sees the public `information_schema` namespace and that a wrong secret is rejected.
- Verification: the focused `token_admission_checks_expiry_secret_revocation_and_commit_guards` real-redb test passed in the prescribed Nix shell. Initial sandbox Nix access was denied; the authorized `nix-shell` invocation succeeded.
- Later verified by the selected-scope TCP and metadata paging increments below.

## SM-6: expose schema discovery through the client control channel — implemented

- Behavior and normative contract: [client protocol](../../specs/client-protocol.md), ADMIN read envelope and `metadata.schemas.list`; [metadata discovery](../../specs/metadata-discovery.md).
- Implementation: the versioned read decoder accepts only `phase=read`, operation version 1, operation `metadata.schemas.list`, and empty arguments. The connection-owned handler revalidates its token and emits bounded schema IDs/names from the storage snapshot API. Existing gateway administrative-operation dispatch carries this request over the control channel; no invocation is admitted.
- Regression evidence: strict decoder tests reject arguments and mutation phase; the real-redb gateway dispatch test authenticates a non-admin control session and receives the public schema in the response.
- Verification: focused decoder and real-redb gateway-dispatch regressions passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed. Commit integration had 109 passing and 2 cgroup-dependent tests ignored.
- Later extended with session-bound network pages; see the metadata paging increment below.

## SM-6: authenticated relation discovery over the control channel — implemented

- Behavior and normative contract: [metadata discovery](../../specs/metadata-discovery.md), relation-grant filtering; [client protocol](../../specs/client-protocol.md), `metadata.relations.list`.
- Implementation: `StorageManager::discover_token_authorized_relations` authenticates and filters under one request-scoped catalog snapshot, consumes the transaction-bound cursors internally, and bounds the complete result by record count and encoded ID/name bytes. The client read requires exactly one schema ID. Hidden and absent schemas serialize identically to an authorized empty schema.
- Regression evidence: strict request decoding checks the schema ID and rejects extra arguments. Real-redb gateway dispatch authenticates an ordinary principal, returns only its granted `visible_app` schema, exposes the granted `metrics` relation, and returns its `reading` column shape. A second table in the visible schema without a data grant is omitted; an ungranted schema containing a table and an absent schema both return the indistinguishable empty relation array. The reads do not start invocations.
- Verification: focused decoder and real-redb gateway-dispatch regressions passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed. Commit integration had 109 passing and 2 cgroup-dependent tests ignored.
- Later extended with session-bound network pages; see the metadata paging increment below.

## SM-6: authenticated column-shape discovery over the control channel — implemented

- Behavior and normative contract: [metadata discovery](../../specs/metadata-discovery.md), declared columns; [client protocol](../../specs/client-protocol.md), `metadata.columns.list`.
- Implementation: `InvocationTransaction::describe_catalog_authorized_columns_page` derives the scope and grant set from its pinned catalog; the storage-manager adapter authenticates the token in a request-scoped snapshot and consumes its pages. It exposes only stable column IDs, names, zero-based ordinals, canonical scalar type names, and nullability. Built-in `information_schema` descriptors share the same stable relation and column IDs as ordinary query metadata. `MetadataClient` negotiates control mode, authenticates, parses all three metadata response shapes into typed records, and provides those records directly to `RequestedScopeBuilder` callers.
- Regression evidence: strict decoder checks require both schema and relation IDs. Real-redb gateway dispatch obtains the public `information_schema.columns` shape and the granted user table's `reading` integer column through an ordinary principal's control connection.
- Verification: the new `client_metadata` TCP test proves typed schema/relation/column decoding, secret zeroization, ordinal-gap preservation, and requested-scope construction. A real-redb transaction regression verifies column discovery through a column-level SELECT grant, and gateway dispatch verifies built-in columns. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed; commit integration had 109 passing and 2 cgroup-dependent tests ignored.
- Later extended with session-bound network pages; complete snapshots remain bounded by the storage adapter's 1 MiB cap.

## SM-6: align relation discovery with table catalog membership — fixed

- Behavior and normative contract: [storage manager](../../specs/storage-manager.md), grant-filtered relation discovery; [metadata discovery](../../specs/metadata-discovery.md), pre-invocation discovery.
- Red evidence: the gateway regression initially authenticated with an admin token and could not prove ordinary-principal filtering. After switching to an ordinary principal with explicit schema USAGE and table SELECT grants, the test showed schema discovery worked but relation discovery returned an empty list. The table catalog held the relation and grant, while schema records had no relation ID list.
- Implementation: both relation enumeration and relation-grant loading now scan the pinned table catalog and select records whose stored schema ID matches the requested schema. The regression verifies the granted schema, relation, and column over the real gateway path; an ungranted table within that visible schema and a second schema with no USAGE grant remain hidden.
- Verification: the focused real-redb gateway regression passed. Rust 1.95.0, Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06 were verified in the prescribed Nix shell. Workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed; the commit integration target had 109 passing tests and 2 cgroup-dependent tests ignored.
- Later extended with session-bound network pages; complete snapshots remain bounded by the storage adapter's 1 MiB cap.

## SM-6: build requested scope from authenticated TCP discovery — implemented

- Behavior and normative contract: [client SDK](../../specs/client-sdk.md), Requested Capability Scope v1 and `MetadataClient`; [metadata discovery](../../specs/metadata-discovery.md); [storage manager](../../specs/storage-manager.md), token-authenticated pre-invocation reads.
- Regression: the real-redb authorization integration test now runs `MetadataClient` over loopback TCP against a `ClientConnectionSession` backed by the real `StorageManager`. An ordinary user's bearer token returns only the granted schema and relation, then the declared granted column. `RequestedScopeBuilder` produces and decodes PSC1 with schema USAGE and the selected column SELECT capability. The same fixture omits an ungranted relation in the visible schema and a separate schema without USAGE.
- Evidence: the focused `storage_manager_runtime_and_metadata_reads_enforce_authorization` integration test passed. Existing gateway-dispatch checks in that fixture independently verify the versioned gateway ADMIN read route and no-invocation behavior. Workspace build, format check, warning-denied Clippy, and all workspace tests passed; commit integration had 109 passing tests and 2 cgroup-dependent tests ignored.
- Later implemented by the resumable metadata paging increment below. The storage methods still return bounded full lists; pagination is owned by the connection session.

## SM-6: resumable metadata pages across control requests — implemented

- Behavior and normative contract: [client protocol](../../specs/client-protocol.md), metadata `limit`/`cursor`; [metadata discovery](../../specs/metadata-discovery.md); [client SDK](../../specs/client-sdk.md), `MetadataClient` page methods.
- Implementation: paged schema, relation, and column reads capture each bounded complete result from one token-authenticated storage snapshot in the connection-owned `ClientConnectionSession`. The session retains at most one result (bounded by the 1 MiB storage cap), emits at most 128 records per page, returns one-use random 16-byte cursors, and expires continuations after 60 seconds idle. Cursors bind the operation, filter, limit, principal, and role closure. New metadata queries replace an outstanding cursor; role changes, replay, filter/limit substitution, and cross-connection use reject.
- Client behavior: `MetadataClient::list_*_page` exposes cursor pages, while existing `list_*` methods follow pages with a default size of 64 and validate ordering across page boundaries.
- Regression evidence: the real-redb TCP integration test pages all three record kinds, rejects query substitution, cursor replay, and cross-connection cursor use, preserves the old column name across a concurrent rename, and observes the renamed column on a fresh query. Decoder tests cover canonical limits and cursor encodings.
- Verification: Rust 1.95.0, Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06 were verified in the prescribed Nix shell. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed. Commit integration had 109 passing tests and 2 cgroup/TinyCC fixture tests ignored; `git diff --check` passed.
- Remaining: server-side snapshots are connection-local and process-local; complete result snapshots remain bounded at 1 MiB, and control-session memory is retained until completion, replacement, connection close, or an attempted continuation after expiry.

## SM-6 / SU-5: authenticated metadata discovery across gateway and supervisor processes — verified

- Behavior and contracts: [metadata discovery](../../specs/metadata-discovery.md), [client protocol](../../specs/client-protocol.md), and [supervisor service](../../specs/supervisor.md). A real authenticated control client can read schema, relation, and column descriptions through the external TCP gateway and private supervisor service, backed by the READY storage instance.
- Regression: [`metadata_process.rs`](../../engine/tests/metadata_process.rs) launches the actual `prdb-supervisor` and `prdb-gateway` executables, authenticates the bootstrap token through `MetadataClient`, checks the public schema, enumerates its relations, and describes `information_schema.schemata` columns. No query artifact is loaded and no worker is launched.
- Verification: the focused `cargo test --locked --test metadata_process` process-boundary test passed in the prescribed Nix shell.
- Limit: this closes the metadata control-read process path only. Successful worker launch and query execution still require a writable delegated cgroup host and the remaining executor callback ABI integration.

## SM-6 / SQL-6: expose filtered `information_schema.schemata` through executor scans — implemented

- Behavior and contract: [storage manager](../../specs/storage-manager.md), [metadata discovery](../../specs/metadata-discovery.md), and [SQL library](../../specs/sql-library.md). The executor's normal `open_scan`/`next_batch` route can read the fixed virtual schema relation from the invocation's pinned, permission-filtered catalog view.
- Red evidence: the real-redb transaction regression resolved the public `information_schema.schemata` relation successfully but `ExecutorMetadataConnection::open_relation_scan` returned `InvalidMetadataRequest` before the virtual path existed.
- Implementation: executor scans now route that relation through `InvocationTransaction::scan_information_schema_schemata`, preserving stable schema-ID ordering, the invocation's USAGE filtering, one-use IPC continuations, and connection-scoped opaque row handles. Virtual row handles are not entered in the writable row map; attempts to update them and inserts into the built-in relation fail closed. Closing the cursor clears virtual scan state.
- Regression evidence: the test pages one schema at a time through the bounded executor batch API, checks the visible public and USAGE schemas in order, rejects cursor replay, and rejects UPDATE/INSERT on the virtual relation.
- Verification: the focused transaction regression passed. Rust 1.95.0, Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06 were verified in the prescribed Nix shell. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check` passed. The transaction target had 67 passing tests; commit integration had 109 passing and 2 cgroup-dependent tests ignored. The maintained TinyCC suite `make -f engine/sdk/Makefile test` passed before this increment.
- Follow-up: the next SM-6 / SQL-6 increment connects the `tables` and `columns` virtual relations below. SQL execution and API parity remain open; sandboxed end-to-end tests still require a delegated-cgroup host.

## SM-6 / SQL-6: expose `information_schema.tables` and `columns` through executor scans — implemented

- Contract: [storage manager](../../specs/storage-manager.md), [metadata discovery](../../specs/metadata-discovery.md), and [SQL library](../../specs/sql-library.md). The executor's bounded scan API now reads all three fixed information_schema relations through the invocation's authorization-filtered scanners.
- Implementation: the executor converts the existing `tables` and `columns` metadata rows to their declared cell shapes, including one-based integer ordinals and typed NULL cells for inapplicable length/precision/scale fields. Virtual row handles remain read-only; closing cursors, relation handles, and connections clears the matching invocation scan state.
- Regression: the real-redb transaction test pages `schemata`, `tables`, and `columns` one row at a time through `next_relation_scan_batch`, verifies ordered row content and nullable cells, rejects cursor replay, and rejects virtual-row UPDATE and virtual-relation INSERT.
- Verification: Rust 1.95.0, Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06 were used in the prescribed Nix shell. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed. Commit integration had 109 passing tests and 2 delegated-cgroup tests ignored in the default run; both ignored sandbox acceptance tests were then run explicitly and passed. The maintained TinyCC SDK suite and sandbox fixture build passed, as did `git diff --check`.
- Remaining: SQL API execution with a real query callback fixture and full API/SQL parity remain open. Sandboxed end-to-end execution still requires a delegated-cgroup host.

## SM-6 / SU-5: recover a disconnected invocation outcome after supervisor restart — verified

- Contract: [redb storage](../../specs/redb-storage.md), outcome recovery; [client protocol](../../specs/client-protocol.md), gateway disconnect and STATUS. An accepted invocation whose gateway disappears before terminal delivery is cancelled, receives a durable ABORTED outcome, and remains owner-queryable after the supervisor closes and reopens the same READY instance.
- Regression: [`metadata_process.rs`](../../engine/tests/metadata_process.rs) compiles a TinyCC ABI-v3 package, submits it through `SubmissionClient` to separate gateway and supervisor processes, kills the gateway after START acceptance, queries ABORTED through a replacement gateway, restarts the supervisor on the same database, then confirms STATUS still returns ABORTED through a new gateway.
- Observed behavior: the supervisor reports a broken pipe while relaying the lost terminal frame; storage nevertheless persists ABORTED. An initial expectation of COMMITTED was rejected by the observed result and corrected to match the specified disconnect cancellation contract.
- Verification: `cargo fmt --all -- --check`, `cargo clippy -p prdb-engine --test metadata_process --locked -- -D warnings`, and `cargo test -p prdb-engine --test metadata_process --locked` passed. The ignored sandbox test passed when run directly under `sudo -n` with the configured delegated cgroup and runtime paths. This is Linux x86_64 process-restart evidence, not power-loss evidence.
- Follow-up: a real sandboxed SQL regression now reaches the irreversible commit boundary and checks durable COMMITTED after gateway terminal delivery fails; rerunning it requires a delegated cgroup with enabled `cpu`, `memory`, and `pids`. Storage-file fault points and other supported durability profiles remain open. The broader SQL, DDL, pressure and analytical stages remain open.

## SM-6 / SU-5: recover COMMITTED status after terminal loss through gateway and supervisor processes — verified

- Contract: [redb storage](../../specs/redb-storage.md), outcome recovery; [client protocol](../../specs/client-protocol.md), terminal delivery and STATUS.
- Regression: `authenticated_metadata_reads_cross_gateway_and_supervisor_processes` creates a token-owned invocation in a real READY redb instance, observes a successful worker exit, commits through the production terminal helper with the receiving socket closed, closes the storage instance, then launches the actual supervisor and gateway binaries. An authenticated `SubmissionClient` STATUS returns COMMITTED after the database is reopened by the service process.
- Verification: `cargo fmt --all -- --check` and `cargo test -p prdb-engine --test metadata_process --locked` passed (1 passed, 1 delegated-cgroup test ignored).
- Scope: proves durable COMMITTED outcome recovery through process startup and the authenticated network status path after terminal delivery fails. The sandboxed query boundary regression is now implemented in `sandboxed_v3_sql_dml_sequence_is_visible_and_commits`; its execution is pending a delegated cgroup with the required controllers.

## SM-6 / MD-4: exercise virtual scans through executor IPC and the SQL consumer — verified

- Contract: [storage manager](../../specs/storage-manager.md), [metadata discovery](../../specs/metadata-discovery.md), and [SQL library](../../specs/sql-library.md). SQL supplies the admitted schema separately from its unqualified `FROM` relation; storage remains the authority for filtering metadata rows.
- Regression: `information_schema_virtual_scans_round_trip_through_executor_ipc` scans all three virtual relations through the production Unix-socket executor loop with a real redb invocation and checks ordered rows, typed cells, and NULLs. The maintained TinyCC fixture `sdk_sql_information_schema_v2.c` executes `SELECT schema_name FROM schemata WHERE schema_name = $1 LIMIT 1` with the schema set to `information_schema`, verifies filtering against two supplied sanitized rows, emits the matching row, and closes the live cursor at LIMIT.
- Verification: `make -f engine/sdk/Makefile test` passed. Workspace build, format, warning-denied Clippy, workspace tests, and `git diff --check` passed; the transaction target had 68 passing tests, and commit integration had 109 passing with 2 delegated-cgroup tests ignored.
- End-to-end regression: `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` executes the TinyCC artifact through the confined worker with a writable delegated cgroup. The artifact queries all three virtual relations through SQL with parameterized schema filters, then completes the existing INSERT/SELECT/UPDATE/DELETE sequence; the test observes the provisional rows and committed terminal outcome. `sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output` also passed when run explicitly.
- Authorization setup: the test grants schema/table/column capabilities and installs row and cell allow policies for its fixture relation, matching the executor's table, column, and policy checks.
- SDK fix: close-cursor response decoding now matches the existing executor IPC wire shape (u16 version plus u8 status); the live LIMIT queries exercise cleanup.
- API/SQL parity: the supervisor exposes narrow read-only methods that query the owned invocation transaction for bounded metadata pages, including continuations. The confined acceptance test compares returned SQL projection values with the same invocation's metadata API rows for the public relations and a granted user schema/table/column. It also queries an ungranted table and a nonexistent name through `tables` and `columns`; SQL returns zero rows for each and the API pages omit the ungranted relation. An authorized empty schema appears in both API and SQL `schemata` results while its `tables` and `columns` queries return no rows through either path. A separate schema containing a table but lacking USAGE is absent from API `schemata`, `tables`, and `columns`; SQL predicates for that hidden schema and a nonexistent schema both yield zero rows across all three views.
- Verification: the rebuilt TinyCC artifact, maintained SDK suite, workspace build, format check, warning-denied Clippy, and workspace tests passed (commit target: 109 passed, 2 delegated-cgroup tests ignored). The confined metadata/DML acceptance and both delegated-cgroup SQL acceptance tests passed with the writable delegated-cgroup parent and declared runtime allowlists; `git diff --check` passed.
- Distinct principals: the real-redb discovery fixture now includes a second principal with USAGE on only the public `public` schema. `information_schema_scans_respect_each_principals_grants` verifies that the broader principal sees `app` and its granted/DELETE-visible relations, while the restricted principal sees only the public virtual schema; the corresponding table and column scan pages narrow with those grants.
- Distinct-principal SQL coverage is recorded below; the authenticated network path follow-up is recorded in the subsequent process integration.

## SM-1 / SM-7: repeated durable writes under a retained reader — verified

- Contract: [redb storage](../../specs/redb-storage.md) and [storage manager validation](05-validation-and-integration.md). A retained read transaction must continue to observe its pinned version while later durable writes make progress.
- Regression: the real-redb [foundation test](../../engine/tests/redb_foundation.rs) pins an initial value, completes 64 Immediate/two-phase commits of 32 KiB replacements while that reader remains open, verifies the pinned reader still sees the original bytes, and confirms a new reader sees the latest committed value.
- Verification: Rust 1.95.0 and redb 4.4.0; `cargo fmt --all -- --check` and `cargo test -p prdb-engine --test redb_foundation --locked` passed.
- Scope: this validates redb snapshot stability and writer progress for this bounded workload. It does not establish the storage manager's admission limits under sustained pressure, physical reclamation/compaction behavior, or power-loss durability.
- Remaining: power-loss durability and production-duration contention remain unverified; bounded retained-reader, concurrent-writer, and reclamation workloads are recorded below.

## SM-4 / SM-7: durable writer progress under a retained invocation snapshot — verified

- Contract: [MVCC](../../specs/concurrency-mvcc.md) and [storage manager validation](05-validation-and-integration.md). A long-lived admitted invocation keeps its original read view while independent writers durably publish later versions.
- Regression: the real-redb [transaction test](../../engine/tests/transaction.rs) holds one invocation snapshot while 32 other invocations each commit a 16 KiB replacement to the same row. The retained reader returns the original row after every commit; a new invocation reads the 32nd committed value.
- Verification: `cargo fmt --all -- --check` and `cargo test -p prdb-engine --test transaction retained_invocation_snapshot_survives_a_durable_write_burst --locked` passed.
- Scope: validates engine transaction behavior for a bounded single-row burst. It does not measure production reader scheduling, sustained write/analytical contention, file reclamation, or power-loss durability.

### Expanded retained invocation pressure — verified

- Regression: expanded `retained_invocation_snapshot_survives_a_durable_write_burst` to hold one invocation snapshot open while 64 other invocations durably replace the same row with 32 KiB values (2 MiB of committed replacement payload). After every commit, the held snapshot still reads its original value; a fresh invocation reads the last replacement.
- Verification: the focused transaction integration test passed with the pinned Nix toolchain.
- Scope: bounded invocation-write pressure only; sustained contention and power-loss behavior remain unmeasured. A separate single-reader compaction case follows.

### Concurrent retained invocation pressure — verified

- Regression: `retained_invocation_snapshot_survives_concurrent_writer_pressure` pins one invocation snapshot while four concurrent writers each publish 64 durable 32 KiB replacements to separate rows (8 MiB total across 256 commits). In each writer round, the reader also rereads all four rows and checks that its original values remain visible; a new invocation reads each writer's final value.
- Verification: formatting and the focused transaction integration test passed with the pinned Nix toolchain; the focused test completed in 1.11 seconds.
- Scope: expanded bounded concurrent pressure; extended production-duration workloads and power-loss behavior remain unmeasured.

### Physical reclamation with a retained reader — verified

- Regression: `retained_reader_blocks_compaction_until_release_then_reclaims_deleted_pages` writes 6.25 MiB of durable values, pins a read snapshot, deletes most rows, and verifies redb rejects compaction while the reader is open. The pinned snapshot still reads all original rows; after release, compaction shrinks the database file and the remaining rows are intact.
- Verification: the focused redb foundation integration test passed on redb 4.4.0.
- Scope: bounded single-reader reclamation evidence; sustained mixed read/write pressure and power-loss behavior remain unmeasured.

### Restricted-principal metadata filtering through gateway and supervisor — verified

- Regression: `authenticated_metadata_reads_cross_gateway_and_supervisor_processes` creates an owner-owned `admin_private` schema and `private_records` table, plus a separate login user/token without grants. Through the actual gateway and supervisor processes, the restricted token still discovers `information_schema` but does not receive the private schema, relations, or columns, including when it supplies the known private schema/table IDs.
- Verification: the focused process test passed in the prescribed Nix toolchain. Workspace build, format, warning-denied Clippy, all workspace tests, and `git diff --check` passed; the commit integration target had 109 passed and 3 delegated-cgroup tests ignored.
### Administrative metadata visibility through the control path — implemented

- Implementation: authenticated control metadata now adds schemas owned by the principal/active owner roles and all schemas for database administrators; relation and column control reads use the same authority check. These checks are isolated from invocation-bound schema grants. Metadata page continuations compare current visible object IDs before serving retained pages, while stable-ID renames preserve the original page snapshot.
- Regression: the process integration authenticates database-admin, schema-owner, and ungranted-reader tokens through the actual gateway and supervisor. The administrator sees the owner-created schema and relation; the owner can inspect declared columns without an explicit USAGE/data grant; the unrelated reader sees none of the private schema, relation, or columns even when supplying known IDs.
- Boundary: the same administrator and schema-owner credentials, when admitted as ordinary invocation transactions, still do not see the schema through grant-filtered `list_catalog_authorized_schemas`.
- Verification: `authenticated_metadata_reads_cross_gateway_and_supervisor_processes` and `storage_manager_runtime_and_metadata_reads_enforce_authorization` passed. Workspace build, format, warning-denied Clippy, all tests, and `git diff --check` passed; the commit target had 109 passed and 3 delegated-cgroup tests ignored.
- Cursor regression: `storage_manager_runtime_and_metadata_reads_enforce_authorization` captures a schema page for an ordinary principal, revokes that principal's schema USAGE grant in a concurrent authorized commit, and verifies the saved continuation is rejected. The same test confirms a stable-ID column rename still serves the original captured page, rejects replay, and rejects cross-connection cursors.
- Verification: the focused metadata authorization integration passed with the pinned Nix toolchain. Workspace build, format, warning-denied Clippy, all tests, and `git diff --check` passed; the commit target had 109 passed and 3 delegated-cgroup tests ignored.
- Distinct-principal SQL acceptance: `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` runs the maintained broad-scope and restricted-scope TinyCC artifacts through the confined worker. The broad principal's SQL/API results include its granted schema, table, and column; the restricted principal receives `empty_roundtrip` while its artifact verifies hidden schema, table, and column predicates return no rows. The artifact uses the same SQL information_schema consumer used by ordinary queries.
- Verification: the acceptance passed again with `/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service`, whose `cpu`, `memory`, and `pids` controllers are enabled. This NixOS profile used the pinned loader/libc paths and an empty `PRDB_TEST_RUNTIME_READ_ONLY` list because no additional read-only runtime files were required.
- Terminal-loss regression: `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` now completes the real worker and relays every provisional SQL result, closes the gateway before terminal delivery, requires the terminal frame write to fail, and then verifies the durable outcome is COMMITTED. The restricted-principal query still follows the normal terminal path.
- Verification: the terminal-loss acceptance passed under the delegated `user@1000.service` cgroup. The confined metadata submission, caught-query-error abort, disconnected gateway restart, and SQL DML/terminal-loss acceptances all passed with the same NixOS runtime profile. Default workspace build, format, warning-denied Clippy, and tests passed; three delegated-cgroup tests remain ignored by the default suite.
- Remaining: the five redb backend callbacks now have injected failure coverage, and bounded mixed reader/writer pressure is covered above. Power-loss durability, production-duration contention, and other supported platform/filesystem profiles remain unverified.

## SM-1 / SM-7: recover an injected redb storage write error — verified

- Regression: `redb_storage_write_error_quarantines_and_recovers_atomically` injects an error from the redb backend's `write` callback during a managed row commit. It verifies the live manager rejects new admissions after the error, then closes and reopens the database and checks that the row, outcome, and commit sequence are either all present or all absent.
- Verification: the focused test passed. Full workspace build, format, warning-denied Clippy, tests, and `git diff --check` passed; the commit integration target had 109 passing tests and 3 delegated-cgroup tests ignored.
- Scope: adds a backend-write failure point alongside the existing injected sync failure. Power loss, other storage-file operations, and other platform/durability profiles remain untested.

### Backend read failure at database open — verified

- Regression: `redb_storage_read_or_len_error_rejects_database_open` injects one-shot backend read and length failures while redb opens an existing database image and verifies each failure rejects open before a storage manager can be constructed.
- Verification: the focused library test passed.
- Scope: read and length fail-closed evidence at startup; additional file operations and platform/durability profiles remain untested.

### Compaction resize failure — verified

- Regression: `redb_compaction_resize_error_preserves_reopenable_data` injects a backend `set_len` failure during redb compaction. Compaction returns the storage error; after reopen, all live rows remain and deleted rows stay deleted.
- Verification: the focused library test passed on redb 4.4.0.
- Scope: bounded compaction-resize recovery only; other file operations, power loss, and platform/durability profiles remain untested.

### Truncated redb image fails closed at service startup — verified

- Regression: `storage_open_rejects_a_truncated_redb_image_and_releases_the_instance_lock` creates a READY instance, truncates its committed redb file to one byte, and verifies `StorageInstance::open` refuses admission. It restores the original image and verifies a subsequent open and full persisted-record validation succeed, proving the failed attempt releases the instance lock.
- Verification: the focused bootstrap integration test passed with the pinned Nix toolchain.
- Scope: fail-closed behavior for a severely truncated local image; it does not simulate torn writes, arbitrary page corruption, or power loss.

## SM-3 / SM-7: cumulative invocation scan-work budget — implemented

- Contract: [memory storage](../../specs/memory-storage.md) and [storage manager](../../specs/storage-manager.md). Per-page caps alone must not let one long-lived invocation scan unbounded row candidates or catalog metadata across repeated scans.
- Implementation: `StorageManagerConfig::max_invocation_scan_work` defaults to 65,536. Managed transactions receive this cap; trusted low-level transactions use the same default. Physical row identities are charged after merging snapshot and overlay IDs and before authorization filtering. Catalog-family records are charged before type, grant, and ownership filtering. Repeated scans consume the budget again. The catalog-family helper owns the durable abort on budget exhaustion, so callers cannot accidentally leave an invocation active after receiving `ScanBudgetExceeded`. Exceeding the cap discards staged writes and persists ABORTED.
- Red evidence: disabling the cumulative cap made the new regression continue to a third page, so its expected `ScanBudgetExceeded` assertion failed. Restoring the cap makes the test pass.
- Regression: `managed_scan_work_budget_is_cumulative_and_aborts_private_writes` consumes a two-unit budget across two one-row pages, then verifies the third page aborts and persists ABORTED without its staged write. It also verifies that metadata discovery with a one-unit cap aborts and discards a staged write. The storage-manager configuration test rejects a zero budget.
- Verification: the focused scan-work regression and full workspace build, format check, warning-denied Clippy, and test suite passed under Rust 1.95.0 with TinyCC 0.9.27-unstable-2025-01-06 after making budget-exhaustion abort local to the catalog scan helper.
- Scope: bounds physical row candidates and catalog-family records used by managed invocation discovery. It does not account for every CPU or memory operation.

## SM-7: first release-mode managed pressure benchmark — measured

- Contract: [redb storage validation](../../specs/redb-storage.md) requires operational point reads and durable writes to be measured under bounded analytical scans, with compilation and execution reported separately.
- Harness: [`storage_pressure.rs`](../../engine/examples/storage_pressure.rs) creates a temporary READY redb instance, seeds four 32 KiB rows, starts a managed invocation with a retained authorized snapshot, then runs four managed writer threads for 64 durable commits each. During the writes, the reader repeats full authorized scans and point reads and compares every returned value with its original snapshot. The benchmark removes its temporary instance afterward.
- Command: `nix-shell -p rustc cargo clippy rustfmt tinycc --run 'cargo build --release -p prdb-engine --example storage_pressure --locked'`, followed separately by `nix-shell -p rustc cargo clippy rustfmt tinycc --run './target/release/examples/storage_pressure'`.
- Measurement: redb 4.4.0, Rust 1.95.0, x86_64 Linux, ext4 on `/tmp` (`/dev/vda2`). One local run completed 256 durable commits in 0.610 s (419.5 commits/s) while the retained snapshot performed 8,930 full authorized scans and 35,720 point reads (14,632.4 scans/s). Exact values matched the original snapshot through all writer commits.
- Scope: one synthetic four-row workload and one local run; timings are machine-specific and do not establish production-duration contention, database growth/reclamation, power-loss durability, or other filesystem profiles.

## SM-1 / SM-7: process crashes at durable commit I/O boundaries — verified

- Contract: [redb storage recovery](../../specs/redb-storage.md) requires atomic rows, outcomes, and commit sequence after process exit; process termination does not prove power-loss durability.
- Regression: `process_crash_during_backend_write_recovers_atomic_commit` exits with code 87 immediately after an underlying backend write; `process_crash_after_durable_sync_recovers_atomic_commit` exits with code 86 immediately after a successful sync callback. Each parent reopens the database and accepts only the fully committed row/outcome/sequence or the original empty state.
- Verification: both focused crash tests passed on redb 4.4.0. The full workspace build, format check, warning-denied Clippy, and tests also passed; the commit integration target had 109 passing tests and 3 delegated-cgroup tests ignored.
- Scope: abrupt process exits after a backend write and successful sync callback; kernel cache and filesystem remain alive. Torn-write, power-loss, production filesystem, and other durability-profile recovery remain open.

## SM-1 / SM-7: single-byte persisted-page corruption rejection — verified

- Contract: [redb storage recovery](../../specs/redb-storage.md) requires invalid persisted state to fail closed before service admission, with the instance lock released after a failed open.
- Regression: `storage_open_rejects_single_byte_page_corruption_and_releases_the_instance_lock` flips one byte at the same offset in successive 4 KiB pages of a READY redb image until open rejects a corrupted page. It restores the valid image and confirms the instance can reopen and persisted records validate.
- Verification: the focused bootstrap regression passed. The workspace build, formatting check, warning-denied Clippy, and full workspace tests passed with Rust 1.95.0; the commit integration target had 109 passing tests and 3 delegated-cgroup tests ignored.
- Scope: detects the tested redb page corruption through the pinned local open/validation path. It does not prove every byte/page corruption is detected, simulate torn writes or power loss, or establish recovery across other platforms and durability profiles.

## SM-5 / DS-5: external foreign-key RESTRICT on table drop — verified

- Contract: [schema DDL](../../specs/ddl-schema.md) requires an external dependency to block `DROP TABLE ... RESTRICT`; failed DDL must leave catalog state unchanged and record a durable ABORTED outcome.
- Regression: `storage_manager_drop_table_restricts_external_foreign_key_dependencies` creates a source FK referencing a target table, attempts to drop the target through `StorageManager`, and verifies `TableHasDependencies`, unchanged commit sequence and both tables/FK edge, an ABORTED outcome, and successful persisted-record validation.
- Verification: the focused commit integration test and full workspace build, formatting check, warning-denied Clippy, and test suite passed under Rust 1.95.0. The commit integration target had 110 passed and 3 delegated-cgroup tests ignored.
- Scope: verifies the referenced-table RESTRICT path and durable failure behavior. The source-table path, which owns and removes its FK, has separate coverage; broader external constraint/policy and DDL race coverage remain open.

## SM-5 / DS-5: FK creation racing with referenced-table drop — verified

- Contract: [schema DDL](../../specs/ddl-schema.md) requires FK creation and referenced-table removal to serialize so neither operation can publish a dangling reference or silently remove a committed constraint.
- Regression: `foreign_key_creation_racing_referenced_table_drop_is_atomic` releases an authorized FK add and target-table `DROP ... RESTRICT` together from a barrier. It accepts only one committed DDL operation, checks both durable outcomes and the single commit-sequence advance, validates the resulting catalog shape, and reopens through `validate_persisted_records`.
- Verification: the focused race test passed. Workspace build, formatting check, warning-denied Clippy, and full workspace tests passed with Rust 1.95.0; the commit integration target had 111 passed and 3 delegated-cgroup tests ignored.
- Scope: one barrier-coordinated schedule per run; repeated stress and races with other external constraint/policy changes remain open.

## SM-5 / DS-5: UNIQUE backfill racing with duplicate insertion — verified

- Contract: [schema DDL](../../specs/ddl-schema.md) requires unique constraint validation against the serialized final table state; a concurrent insert and constraint backfill cannot both publish duplicate values.
- Regression: `unique_constraint_backfill_racing_duplicate_insert_is_atomic` stages a duplicate-value insert while adding an owned UNIQUE constraint and its backing index, then releases both commits from a barrier. It accepts only either a committed insert with aborted backfill or committed backfill with the stale insert aborted. It verifies both outcomes, exactly one commit-sequence advance, matching row/index presence, and startup integrity.
- Verification: the focused race test passed. Workspace build, formatting check, warning-denied Clippy, and full workspace tests passed with Rust 1.95.0; the commit integration target had 112 passed and 3 delegated-cgroup tests ignored.
- Scope: one barrier-coordinated schedule per run; repeated race stress and races involving other constraint and policy changes remain open.

## SM-5: authorized foreign-key removal through StorageManager and ADMIN — implemented

- Contract: [storage manager](../../specs/storage-manager.md) requires FK removal to delete the constraint, its object and target dependency atomically, and advance both participating table write epochs so pinned invocations cannot commit against the old relationship graph.
- Implementation: `StorageManager::drop_foreign_key` reads and validates the FK and target constraint, constructs expected-value deletes, advances the distinct source/target table epochs, and delegates to the serialized authorized FK DDL commit path. Self-referencing FKs advance their shared table once.
- Exposure: authenticated ADMIN `PREPARE`/`START` accepts `foreign_keys.drop` with a foreign-key ID and returns the durable committed outcome. Ownership/DBA authorization, auditing, cancellation and operation-state behavior follow the existing DDL gateway.
- Regression: `storage_manager_drops_foreign_key_and_advances_both_table_epochs` verifies FK/object/dependency removal, both epoch updates, stale source/target writers conflicting, and startup integrity. `storage_manager_drops_self_referential_foreign_key_once_per_table` verifies a self-reference advances the shared table epoch once. The authenticated control integration test exercises the wire operation; decoder tests reject extra arguments.
- Verification: focused manager, decoder, and authenticated gateway tests passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed under Rust 1.95.0. The commit integration target had 114 passed and 3 sandbox/cgroup-dependent tests ignored.
- Remaining: repeated race stress and broader external constraint/policy interactions remain open.

## SM-5: structured StorageManager foreign-key creation — implemented

- Contract: [schema DDL](../../specs/ddl-schema.md) requires FK creation to validate ordered source/target columns, source ALTER and target REFERENCES authority, existing rows, and both table epochs under one serialized fenced commit.
- Implementation: `StorageManager::create_foreign_key` takes a typed request with source table/columns and a target PRIMARY KEY/UNIQUE constraint. It generates the FK ID, constructs the constraint/object/dependency records, increments each distinct table write epoch, and delegates final authority, compatibility, orphan-row, fence and publication checks to the authorized FK DDL path.
- Regression: `storage_manager_creates_foreign_key_and_advances_both_table_epochs` creates an FK using a target REFERENCES grant and source owner authority. It verifies returned server-generated ID, durable COMMITTED outcome, all catalog records and dependency, both table epoch increments, and startup integrity. The existing authority-separation and FK-create/table-drop race regressions also pass.
- Exposure: authenticated ADMIN `foreign_keys.create` uses connection-owned PREPARE/START, accepts ordered source columns plus a target key constraint, and returns the generated FK ID. The strict decoder rejects extra arguments; the real ADMIN control-dispatch integration now creates then drops an FK.
- Verification: focused FK creation, authorization-separation, FK/table-drop race, decoder, and authenticated control-path tests passed. Workspace build, format check, warning-denied Clippy, and full tests passed under Rust 1.95.0; the commit integration target had 115 passed and 3 delegated-cgroup tests ignored.

## SM-5 / DS-7: FK removal racing with referenced-table drop — verified

- Contract: removing an FK and dropping its referenced table must serialize so no committed table remains with a dangling dependency and no committed table drop leaves the FK behind.
- Regression: `foreign_key_removal_racing_referenced_table_drop_is_atomic` releases authorized FK removal and target-table drop together. It accepts FK removal with either a successful subsequent table drop, a RESTRICT abort, or a stale-catalog conflict; checks durable outcomes and sequence advances; verifies source/FK/target shape; and runs startup integrity validation.
- Evidence: the first run observed a stale-catalog `Conflict` for the table drop after FK removal had serialized first. The test records that as a safe durable abort alongside the explicit dependency RESTRICT result. The focused barrier test passes.
- Verification: workspace build, format check, warning-denied Clippy, and all workspace tests passed under Rust 1.95.0. The commit integration target had 116 passed and 3 delegated-cgroup tests ignored.

## SM-5: structured UNIQUE creation through StorageManager and ADMIN — implemented

- Contract: [schema DDL](../../specs/ddl-schema.md) requires UNIQUE creation to validate current rows, publish its owned unique index atomically, preserve NULLS DISTINCT behavior, and invalidate writers pinned before DDL.
- Implementation: `StorageManager::create_unique_constraint` generates constraint/index IDs and creates the family-8 definition, family-9 unique index, objects, dependency and table epoch update. The authorized constraint DDL commit validates and backfills the physical index under the table fence. ADMIN `constraints.create_unique` uses connection-owned PREPARE/START and returns both IDs.
- Regression: `storage_manager_creates_unique_constraint_and_backfills_existing_rows` inserts two unique values before DDL, creates the constraint, checks stale writer conflict and durable ABORTED outcome, and validates startup. Decoder coverage checks ordered ID arrays and exact arguments; the authenticated control integration creates a UNIQUE constraint before creating/dropping an FK.
- Verification: focused manager, decoder, and authenticated ADMIN control-path tests passed. Workspace build, format check, warning-denied Clippy, and full tests passed under Rust 1.95.0; the commit integration target had 117 passed and 3 delegated-cgroup tests ignored.
- Remaining: PRIMARY KEY and CHECK creation/removal need equivalent structured manager and ADMIN operations; SQL `ALTER TABLE` parsing remains outside this control protocol.

## SM-5: structured PRIMARY KEY creation through StorageManager and ADMIN — implemented

- Contract: [schema DDL](../../specs/ddl-schema.md) requires PRIMARY KEY columns to be non-nullable and the owned unique index to be validated/backfilled before publication.
- Implementation: `StorageManager::create_primary_key` shares the fenced key-constraint catalog path with UNIQUE while writing the PRIMARY KEY kind. Authenticated ADMIN `constraints.create_primary_key` exposes the structured request through connection-owned PREPARE/START and returns the generated constraint and index IDs.
- Regression: real-redb coverage creates a PRIMARY KEY on a non-null column, verifies its owned index and startup integrity, and proves a writer pinned before creation conflicts, publishes no row, and records ABORTED. Decoder coverage checks the exact ordered-column request, and the authenticated control integration creates a PRIMARY KEY before creating a UNIQUE and exercising FK lifecycle.
- Verification: focused manager, decoder, authenticated gateway, and stale-writer regressions passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed in the prescribed Nix shell; the commit integration target reported 121 passed and 3 cgroup-dependent ignored.
- Remaining: CHECK creation/removal and SQL `ALTER TABLE` parsing remain open.

## SM-5: structured CHECK lifecycle through StorageManager and ADMIN — implemented

- Implementation: typed `create_check_constraint` and `drop_check_constraint` operations generate/remove CHECK, expression, object, and dependency records and advance the table write epoch through the authorized, fenced constraint commit. Authenticated ADMIN exposes create/drop as connection-owned PREPARE/START operations; creation accepts bounded EIR1 bytes and declared column dependencies.
- Regression: real-redb coverage creates a Boolean CHECK, verifies startup integrity, then removes the CHECK/expression. A row-local CHECK that rejects an existing row leaves no publication and records ABORTED; non-row-local identity expressions are rejected. Writers staged before CHECK add and drop each conflict afterward, publish no row, and record ABORTED. Decoder tests cover create/drop requests, and the authenticated ADMIN control integration creates then drops a CHECK.
- Verification: focused lifecycle, decoder, authenticated gateway, and stale-writer regressions passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed in the prescribed Nix shell; the commit integration target reported 121 passed and 3 cgroup-dependent ignored.
- Remaining: SQL `ALTER TABLE` parsing remains outside this control protocol.

## SM-5: structured PRIMARY KEY and UNIQUE removal through StorageManager and ADMIN — implemented

- Implementation: typed PK/UNIQUE drop methods remove the constraint, owned index, object rows, and kind-4 dependency, and advance the table write epoch through the authorized constraint commit. That commit removes physical index memberships and checks remaining FK references before publication. ADMIN exposes `constraints.drop_primary_key` and `constraints.drop_unique` with connection-owned PREPARE/START.
- Regression: manager tests create and backfill a UNIQUE over existing rows and create a PRIMARY KEY, then remove each and verify catalog cleanup and startup integrity. Writers staged before UNIQUE and CHECK drops receive Conflict, publish no row, and record ABORTED. Dropping a PRIMARY KEY still referenced by an FK aborts without changing the commit sequence or catalog. The authenticated ADMIN integration creates and removes both key kinds after exercising CHECK and FK lifecycle; decoder tests verify the exact drop request shapes.
- Verification: focused manager, decoder, authenticated gateway, dependency-rejection, and stale-writer regressions passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed under Rust 1.95.0; the commit integration target had 120 passed and 3 cgroup-dependent tests ignored.
- Remaining: SQL `ALTER TABLE` parsing remains outside this control protocol.

## SM-5 / DS-7: FK creation racing with referenced key removal — verified

- Contract: foreign-key creation and PRIMARY KEY removal must serialize so the catalog never commits an FK with a missing referenced key or drops a key while a committed FK still depends on it.
- Regression: `foreign_key_creation_racing_referenced_primary_key_drop_is_atomic` releases the authorized FK catalog commit and typed PK drop from a barrier. It accepts only one committed DDL operation, verifies both durable outcomes, checks the corresponding FK/key/dependency records, and runs startup integrity validation.
- Evidence: the focused barrier-coordinated regression passed against real redb.
- Verification: the focused barrier regression passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed under Rust 1.95.0; the commit integration target had 121 passed and 3 cgroup-dependent tests ignored.

## SM-5 / DS-7: CHECK creation racing with a violating insert — verified

- Contract: CHECK publication must validate the serialized final row set, and an invocation pinned before constraint publication cannot commit using the older table write epoch.
- Regression: `storage_manager_creates_and_drops_check_constraint` races a non-NULL insert against publishing `CHECK (column IS NULL)` from a three-party barrier. It accepts only either a committed insert with CHECK backfill durably aborted as `CheckViolation`, or committed CHECK DDL with the stale insert rejected as `Conflict`. Both paths verify durable outcomes, exactly one sequence advance, and row presence consistent with the winner; startup validation succeeds after cleanup.
- Verification: the focused real-redb lifecycle/race regression passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed in the prescribed Nix shell; the commit integration target reported 121 passed and 3 cgroup-dependent ignored.

## SM-5 / DS-7: FK creation racing with an orphan child insert — verified

- Contract: FK creation validates existing child rows against the referenced key set, while a writer pinned before FK publication must conflict after the source table epoch advances.
- Regression: `foreign_key_creation_racing_orphan_child_insert_is_atomic` releases an orphan child insert and typed FK creation from a barrier. It accepts only a committed insert with durable FK `ForeignKeyViolation`, or committed FK creation with the stale insert rejected as `Conflict`. Both outcomes verify durable statuses, one commit-sequence advance, and row/catalog state consistent with the winner; any published FK is removed before startup validation.
- Verification: the focused real-redb barrier regression passed ten consecutive fresh-database runs, exercising FK cleanup when DDL wins. Full workspace build, format, warning-denied Clippy, and tests passed in the prescribed Nix shell; the commit integration target reported 123 passed and 3 cgroup-dependent ignored.

## SM-5 / DS-7: index DDL racing with table ownership transfer — verified

- Contract: index DDL must recheck current owner/grant authority under serialized commit, so a former table owner cannot publish an index after ownership changes.
- Regression: `index_ddl_racing_table_owner_transfer_rechecks_authority_at_commit` releases an old owner's index creation and administrator's table-owner transfer from a barrier. It permits the index to commit before transfer, or the transfer to commit while the index aborts with `DdlAuthorizationDenied`; a transfer planned before a competing table-epoch change may instead abort with `Conflict`. Durable outcomes, sequence increments, index presence, final owner, and startup integrity are checked.
- Verification: the focused real-redb barrier regression passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed in the prescribed Nix shell; the commit integration target reported 123 passed and 3 cgroup-dependent ignored.

## SM-5: typed ordinary index creation through StorageManager — implemented

- Contract: [storage manager](../../specs/storage-manager.md) requires ordinary index publication to authorize the actor, validate/backfill current rows, atomically publish the index and object, and advance the affected table write epoch.
- Implementation: `StorageManager::create_index` accepts a normalized name, ordered column IDs, and uniqueness flag; it generates the ID and delegates publication/backfill to `commit_authorized_index_ddl`. The narrow authorization guard now permits only kind-1 ownership-edge writes whose dependent is an index changed in the same batch. Invalid request/audit, storage, ID-generation, and commit errors have a typed `IndexLifecycleError`.
- Regression: `storage_manager_creates_named_unique_index_and_backfills_atomically` builds a named unique index over populated data, verifies its schema parent, ownership edge, physical forward/reverse memberships, and stored uniqueness, and proves a writer pinned before index publication conflicts, publishes no row, and records ABORTED.
- TDD evidence: the first real-redb run rejected the generated index object as `InvalidCatalog`; the accepted catalog shape requires a schema parent plus its matching kind-1 edge and the canonical database-admin owner.
- Verification: the focused real-redb lifecycle regression passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` all passed in the prescribed Nix shell.
- Remaining: typed StorageManager removal/rebuild requests and authenticated ADMIN operation dispatch remain open; the raw authorized index DDL commit path remains available for trusted internal callers.

## SM-5: typed ordinary index removal through StorageManager — implemented

- Contract: [storage manager](../../specs/storage-manager.md) requires index removal to authorize the current actor, remove the definition and physical memberships atomically, and advance the table write epoch.
- Implementation: `StorageManager::drop_index` accepts an index ID and captures exact current bytes for the family-9 definition, family-2 object, and schema-parent kind-1 ownership edge. It removes all three through `commit_authorized_index_ddl`; that path removes forward and reverse memberships and rejects a drop that leaves an invalid dependent constraint.
- Regression: `storage_manager_creates_named_unique_index_and_backfills_atomically` now continues through typed drop, checking durable COMMITTED status and removal of the definition, object, ownership edge, and all forward/reverse memberships.
- Verification: the focused real-redb create/drop lifecycle regression passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` all passed in the prescribed Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: authenticated ADMIN operation dispatch remains open.

## SM-5: typed ordinary index rebuild through StorageManager — implemented

- Contract: [storage manager](../../specs/storage-manager.md) requires rebuild to validate old memberships, derive replacement keys from current rows, publish the updated definition and memberships atomically, advance the table write epoch, and preserve older snapshots.
- Implementation: `StorageManager::rebuild_index` accepts an index ID, ordered column IDs, and uniqueness profile. It captures the exact current index definition and table schema version, then rewrites the typed catalog fields and delegates the replacement build to `commit_authorized_index_ddl`.
- Regression: `storage_manager_rebuilds_populated_indexes_atomically` now exercises the typed API with authenticated database-admin membership. It verifies old snapshot visibility, replacement forward/reverse memberships, and durable conflict of a writer pinned before the rebuild.
- Verification: the focused real-redb rebuild regression passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` all passed in the prescribed Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: none for the typed StorageManager layer; authenticated ADMIN operation dispatch is tracked below.

## SM-5 / CP-1: authenticated ADMIN dispatch for ordinary indexes — implemented

- Contract: [client protocol](../../specs/client-protocol.md) exposes connection-owned PREPARE/START for index create, rebuild, and drop. START must revalidate the authenticated principal and delegate to action-specific storage authorization; a native query receives no administrative credentials.
- Implementation: the strict ADMIN decoder accepts `indexes.create`, `indexes.rebuild`, and `indexes.drop`. PREPARE binds each request to the connection's pending operation ID; START revalidates session state and routes through the typed `StorageManager` request with an audit event correlated to that ID.
- Regression: `admin_revoke_decoder_requires_prepare_then_start_schemas` checks all three canonical decoder shapes. `storage_manager_runtime_and_metadata_reads_enforce_authorization` exercises authenticated gateway PREPARE/START for create, rebuild, and drop and verifies committed responses; startup catalog validation succeeds after the broader DDL lifecycle.
- Verification: focused decoder and authenticated gateway ADMIN lifecycle tests passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` all passed in the prescribed Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: SDK-facing SQL DDL compilation and a separately packaged TinyCC sandbox integration harness are still outside this increment.

## SM-5 / SDK-5: cross-process authenticated ADMIN identity and token lifecycle — verified

- Contract: client control operations must traverse the gateway and supervisor boundary, then reach storage's authorized DDL/token lifecycle instead of a test-only storage call.
- Regression: the real-process metadata integration now issues role create/rename/drop, user creation, one-time token issue, credential-file save/load, fresh token-authenticated metadata discovery, and token revoke through `AdminClient`. A post-revocation authentication attempt is rejected. The test confirms one-time client secret buffers are zeroized.
- Verification: the focused integration test passed and `cargo test --workspace --locked` passed with 124 commit tests and 3 cgroup-dependent ignored. Full build, format, and warning-denied Clippy passed.
- Remaining: multi-process coverage for the other ADMIN DDL families and the confined artifact submission path remains open.

## SDK-5: schema/table DDL CLI and multi-process coverage — implemented

- Contract: [client SDK](../../specs/client-sdk.md) requires schema/table administrative work to use the authenticated control channel and typed `AdminClient` operations; query artifacts receive no administrative credentials.
- Implementation: `prdb-client` now exposes schema create/rename/drop and table create/rename/drop. Table creation parses semicolon-separated typed column declarations, bounds column count, validates scalar parameters, duplicate names, nullability, and the non-null UUID identity modifier, then delegates to the existing typed client request. Literal/default expressions remain available through `AdminClient` but are not yet part of this CLI syntax.
- Regression: parser tests cover parameterized DECIMAL and VARCHAR types, UUID identity columns, and reject invalid precision/scale, zero VARCHAR length, duplicates, missing defaults for non-null non-identity columns, and invalid identity combinations. The real-process metadata integration now creates a schema and table, renames/drops the table, and drops the schema through the gateway and supervisor.
- Verification: focused CLI parser tests passed (6); the focused authenticated metadata/admin process test passed. Final `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the Nix shell. The commit integration target reported 124 passed and 3 cgroup-dependent ignored. Updated `prdb-client --help` was inspected.
- Remaining: column add/rename/drop, defaults/nullability, indexes, constraints, privileges, policies, and broader multi-process ADMIN DDL coverage remain for later slices; confined artifact submission still requires the sandbox integration environment.

## SDK-5: column add/rename/drop CLI and process coverage — implemented

- Contract: column DDL uses authenticated typed ADMIN operations, and storage applies schema evolution and its write-epoch fences atomically.
- Implementation: `prdb-client` now exposes `column-add`, `column-rename`, and `column-drop`. Column addition accepts one nullable typed column and returns the generated column ID; rename/drop require the table and column IDs. Storage's existing AdminClient API still supports broader add-column defaults.
- Regression: the parser rejects non-null or identity columns for the CLI add form and rejects multiple declarations. The authenticated gateway/supervisor process test exercises add, rename, and drop on the created table.
- Verification: focused CLI parser and real-process tests passed. Workspace build, format, warning-denied Clippy, and full tests passed under the prescribed Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: nullability/default changes, indexes, constraints, privileges, policies, additional multi-process DDL coverage, and confined artifact submission remain open.

## SDK-5: column nullability/default CLI and process coverage — implemented

- Contract: column metadata changes use authenticated ADMIN operations and are validated against the current stored schema and row set before atomic publication.
- Implementation: `prdb-client` now exposes nullable-state changes and column default set/drop. Default input is typed by an explicit scalar type and supports SQL NULL, current principal UUID, and typed scalar literals including bounded DECIMAL, VARCHAR, BYTEA hex, UUID, DATE epoch days, and TIMESTAMPTZ epoch microseconds.
- Regression: parser checks cover decimal scaling, bytea decoding, UUID principal defaults, and type mismatches. The multi-process ADMIN test exercises nullability changes and a literal default set/drop through the gateway and supervisor.
- Verification: focused parser and real-process tests passed. Build, formatting, and warning-denied Clippy passed. The first full-suite run hit the known intermittent CHECK lifecycle `InvalidAudit`; that test passed in isolation, and the complete rerun passed with 124 commit tests and 3 cgroup-dependent ignored.
- Remaining: CLI commands for indexes, constraints, privileges, and policies; broader ADMIN process coverage; and confined artifact submission remain open.

## SDK-5: ordinary index CLI and process coverage — partial

- Contract: authenticated index create/rebuild/drop requests use the existing typed AdminClient and server-side authorization/fencing path.
- Implementation: the CLI exposes create, rebuild, and drop with canonical ordered column IDs; uniqueness defaults to false and is an optional boolean flag.
- Regression: parser coverage checks canonical nonzero ID lists and duplicate rejection. The real-process ADMIN lifecycle now covers index create, rebuild with a changed uniqueness profile, and drop; existing typed-client and ADMIN dispatcher tests cover rebuild request construction.
- Verification: focused CLI parsing and real multi-process index lifecycle passed. Full workspace build, format check, warning-denied Clippy, and tests passed; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: constraint, privilege, and policy CLI commands remain open; confined artifact execution still needs a delegated-cgroup integration environment.

## SM-5 / DS-7: same-profile index rebuild — implemented

- Contract: `indexes.rebuild` rebuilds and validates physical memberships even when the requested columns and uniqueness profile match the existing definition; it records audit/outcome and advances the table write epoch.
- Red evidence: the focused real-redb populated-index regression initially failed with `Commit(InvalidCatalog)` when repeating the just-committed definition. The real gateway/supervisor test also showed the same behavior on an empty table.
- Implementation: identical family-9 definition writes are now treated as explicit rebuild intent during table-fence discovery, index authorization, and backfill planning. Rebuilds with no rows still enter the authorized commit path and commit the durable audit/outcome plus table epoch.
- Regression: the populated redb test repeats the same definition and checks the next commit sequence and COMMITTED outcome. The real-process ADMIN test now rebuilds first to a changed uniqueness profile, then repeats that profile through gateway/supervisor, and finally drops the index.
- Verification: both focused regressions passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed under Rust/Cargo 1.95.0; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.

## SM-5: key constraints after schema evolution — fixed

- Contract: typed unique and primary-key DDL must create a backing index compatible with the current table schema, including after column changes.
- Red evidence: the multi-process ADMIN regression created a column, then unique constraint, and failed with `Commit(InvalidCatalog)`.
- Cause and fix: the generated family-9 index used the table schema version in its format-version field and hardcoded its schema-version field to 1. The builder now writes fixed format version 1 and the current schema version separately.
- Regression: the real gateway/supervisor lifecycle creates and drops a unique constraint after column addition; the scenario passes after the fix.
- Verification: the focused process regression and complete workspace build, format, warning-denied Clippy, and tests passed; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.

## SM-5: typed object grant/revoke lifecycle — implemented

- Contract: object grants use the stable family-12 key and envelope format, validate privilege/object/scope combinations against the live catalog, and publish the grant, first/last authority dependency edge, grantee epoch, audit, and durable outcome atomically.
- Implementation: `StorageManager::set_object_grant` accepts typed schema/table/column targets and the closed object privilege set. It rejects duplicate grants and absent revokes, verifies that a column belongs to its table and that the grantee is a live user or role, and delegates publication to the authorized grant DDL validator.
- Regression: the gateway/supervisor process test uses the typed AdminClient grant path to grant both column-level SELECT and REFERENCES, create/drop a foreign key, revoke REFERENCES while SELECT remains, then revoke SELECT as the final grant. This exercises first/last authority dependency handling. Decoder tests cover table/column request shapes and reject an unknown privilege; CLI parsing accepts only the closed privilege set.
- Verification: focused decoder, process, parser, and CLI help checks passed. The full workspace build, format check, warning-denied Clippy, and rerun of the full test suite passed under Rust 1.95.0; the commit integration target reported 124 passed and 3 cgroup-dependent ignored. The first full test run had one intermittent `InvalidAudit` in the existing CHECK lifecycle test, and that test passed alone.
- Remaining: policy CLI and broader authenticated ADMIN coverage remain open.

## SM-5: typed policy create/drop through StorageManager — implemented

- Contract: row and cell policies use family 13 definitions, family 14 recipient/column targets, purpose-tagged family 7 expressions, exact dependency edges, table write-epoch fencing, audit, and durable outcome. `SELECT`/`DELETE` require USING; `INSERT` requires WITH CHECK; `UPDATE` requires both. Cell policies require column targets and reject DELETE.
- Implementation: `StorageManager::create_policy` validates normalized per-table names, operation/clause shape, unique live user/role recipients, same-table cell columns, expression Boolean type and binding against the current ordered schema, and live role dependencies. It generates policy/expression IDs and publishes all records and edges atomically. `drop_policy` removes the policy object, recipient/column targets, expression objects/records and all dependencies while advancing the table epoch.
- Regression: the existing policy DDL authorization integration now creates a typed row SELECT ALLOW policy for a role, checks its generated expression ID and committed outcome, then drops it and checks the next durable commit. Existing policy DDL tests cover inherited `MANAGE_POLICIES`, denied actors, target validity, and required recipients.
- Verification: focused policy lifecycle passed. Rust 1.95.0 workspace format check, warning-denied Clippy, build, and full tests passed; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: broader row/cell operation coverage and repeated race stress remain open.

## SM-5 / SDK-5: authenticated policy ADMIN and CLI — implemented

- Contract: strict PREPARE/START accepts canonical row/cell policy shape, recipient IDs, cell-column targets, and nullable EIR1 USING/WITH CHECK expressions. START delegates to typed StorageManager policy create/drop and revalidates authority at serialized commit.
- Implementation: `policies.create` and `policies.drop` are connection-owned ADMIN operations. `AdminClient` encodes typed policy enums, recipients, target columns and expressions; `prdb-client policy-create/policy-drop` exposes the same path.
- Regression: protocol decoder tests accept canonical row-policy create/drop and reject malformed shapes; the real gateway/supervisor process lifecycle creates and drops row SELECT/INSERT/UPDATE/DELETE and cell SELECT/INSERT/UPDATE ALLOW policies, using required USING/WITH CHECK clauses and a live cell target. CLI recipient parser accepts only user/role plus canonical nonzero lowercase IDs.
- Verification: focused protocol, multi-process, and CLI parser tests passed; updated `prdb-client --help` was inspected. Rust 1.95.0 workspace build, formatting, warning-denied Clippy, and full tests passed; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: broader authenticated ADMIN families, repeated race stress, and confined artifact execution remain open.

## SM-5 / DS-7: policy creation conflicts with stale writers — verified

- Contract: a policy DDL commit advances its table write epoch; a writer pinned before that authority change must not commit using stale policy state.
- Regression: the named-index lifecycle test now stages a row write, creates a row policy through typed `StorageManager::create_policy`, and confirms the stale writer records an ABORTED outcome and publishes no row while policy creation records COMMITTED.
- Verification: the focused real-redb regression passed. Rust 1.95.0 workspace build, formatting, warning-denied Clippy, and full tests passed; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: race coverage against other DDL families and confined artifact execution remain open.

## SM-5 / DS-7: policy DDL racing with a staged writer — verified

- Contract: policy publication and invocation writes must linearize under the table fence. A writer that commits first may succeed; a writer that reaches finalization after policy publication must conflict and persist ABORTED.
- Regression: a barrier-coordinated real-redb race stages a row write, then concurrently commits it against typed row-policy creation. It accepts either order and checks row visibility, both durable outcomes, and the resulting commit sequence. The focused race test passed in ten fresh runs.
- Verification: focused repeated race passed. Rust 1.95.0 workspace build, format check, warning-denied Clippy, and full test suite passed; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: races against other DDL families, broader stress schedules, and confined artifact execution remain open.

## SM-5 / DS-7: policy DDL racing with table-owner transfer — verified

- Contract: policy creation must revalidate table ownership at its serialized commit boundary when a concurrent owner transfer changes authority.
- Regression: a barrier-coordinated real-redb race concurrently creates a row policy as the current table owner and transfers ownership as a database administrator. It accepts the valid serial orders, checks durable outcomes and commit-sequence advancement, verifies the resulting policy and table owner, and validates the recovered catalog image.
- Verification: focused test passed once and in ten fresh processes. Rust 1.95.0 workspace build, format check, warning-denied Clippy, and full test suite passed; the commit integration target reported 125 passed and 3 cgroup-dependent ignored.
- Remaining: races against other DDL families, broader stress schedules, and confined artifact execution remain open.

## SM-2: reject zero IDs in derived index keys — verified

- Contract: [index-key encoding v1](../../specs/index-key-encoding.md) requires catalog table/index/row/column identifiers to be nonzero before a persisted SQL index key is derived.
- Red evidence: `catalog_index_key_rejects_zero_index_and_row_ids` failed because `derive_index_key` produced a complete key with a zero index ID.
- Implementation: `derive_index_key` now rejects a zero index ID or row UUID before decoding and projecting the typed tuple; the codec spec states the nonzero-ID rule.
- Verification: all 12 `index_key` integration tests passed. With Rust/Cargo 1.95.0, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed; the commit integration target reported 125 passed and 3 delegated-cgroup tests ignored.
- Remaining: no change to storage admission or migration behavior; database-format upgrade evidence and production power-loss/filesystem validation remain open.

## SM-5 / AU-6: object-grant revocation conflicts with a staged writer — verified

- Contract: [authorization](../../specs/authorization.md), [concurrency/MVCC](../../specs/concurrency-mvcc.md), and [storage manager](../../specs/storage-manager.md). Finalization must revalidate the exact grant record used by an authorized write; revocation cannot leave a previously staged write committing under stale authority.
- Regression: `storage_manager_authorizes_object_grants_and_linearizes_revocation_races` stages an authorized row update with the persisted table UPDATE grant and races its commit against `commit_authorized_object_grant_ddl` revocation. It accepts only commit-before-revoke or revoke-before-abort, checks row bytes, durable outcomes, commit sequence, and final grant/dependency state. Ten fresh-process runs all observed commit-before-revoke; token and role-membership regressions separately verify revoke-before-stale-write aborts.
- Verification: the full Rust commit integration target passed (127 passed, 3 delegated-cgroup tests ignored); workspace formatting and warning-denied Clippy passed under Rust/Cargo 1.95.0.
- Remaining: repeat the grant race under schedules that exercise revoke-before-write, and cover other authority-changing DDL races.

## SM-5 / AU-6: role-membership revocation conflicts with a staged writer — verified

- Contract: a committed role-membership change advances the member's authority epoch and invalidates writes admitted through the prior role closure.
- Regression: `role_membership_revocation_aborts_a_staged_role_authorized_writer` admits a user with an inherited role grant, stages an authorized update, revokes the role membership through `StorageManager::set_role_membership`, and verifies the stale writer is durably ABORTED, the row remains unchanged, the revocation is COMMITTED, and the commit sequence advances only for the two successful commits.
- Implementation: no production change was needed; the existing pinned membership and authority-epoch guards reject the stale write.
- Verification: the focused regression passed. The Rust commit integration target passed with 126 tests and 3 delegated-cgroup tests ignored; `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the prescribed Rust/Cargo 1.95.0 Nix shell.
- Remaining: equivalent stale-writer coverage for identity, credential, and other authority-changing DDL routes, plus repeated concurrent schedules, remains open.

## SM-5 / AU-6: token revocation discards a pending row write — verified

- Contract: a token-authenticated invocation must revalidate its exact credential record at finalization; revoking that token makes a pending invocation conflict before any staged row can publish.
- Regression: `token_admission_checks_expiry_secret_revocation_and_commit_guards` admits an ordinary user, stages a policy-authorized row update and its old/new index memberships, then revokes the token through `StorageManager::revoke_token` as a separate database administrator. It verifies the stale invocation is durably ABORTED, the original row and index entry remain, and the replacement index entry is absent. A second live admin credential keeps final-admin validation satisfied.
- Implementation: no production change was needed; token admission's pinned verifier guard rejects the stale finalization.
- Verification: the focused regression passed. `cargo fmt --all -- --check` and warning-denied workspace Clippy passed. The full Rust commit integration target passed serially with 126 tests and 3 delegated-cgroup tests ignored under the prescribed Rust/Cargo 1.95.0 Nix shell. Two earlier parallel full-target runs hit the existing intermittent `InvalidAudit` in `storage_manager_creates_and_drops_check_constraint`; that test passed alone and in the serial full-target run.
- Remaining: add stale-writer coverage for identity rename/drop and broader authority-changing DDL routes.

## SM-5 / AU-6: LOGIN disable conflicts with an authorized staged write — verified

- Contract: disabling a LOGIN user advances the principal authority epoch and revokes all active credentials atomically; an invocation admitted through a prior token and authority snapshot must not publish its pending mutation.
- Regression: `token_admission_checks_expiry_secret_revocation_and_commit_guards` admits a second token-authenticated writer, stages an authorized row patch and corresponding index replacement, disables the user through `StorageManager::set_user_login_enabled`, and verifies the writer is durably ABORTED, the row and old index membership remain, the replacement index membership is absent, new token admission fails, and the issued token is listed as revoked.
- Implementation: no production change was needed; the identity epoch and verifier guards reject the stale write after the serialized LOGIN transition.
- Verification: the focused regression passed. Rust formatting and warning-denied workspace Clippy passed; the full commit integration target passed serially with 126 tests and 3 delegated-cgroup tests ignored in the prescribed Rust/Cargo 1.95.0 Nix shell.
- Remaining: stale-writer coverage for identity rename/drop and other authority-changing DDL routes remains open.

## SM-5 / AU-6: identity rename conflicts with an authorized staged write — verified

- Contract: a token-authenticated invocation admitted under a pinned principal authority snapshot cannot publish after the corresponding identity is renamed through the authorized StorageManager path.
- Regression: `token_admission_checks_expiry_secret_revocation_and_commit_guards` stages an authorized row patch and index replacement, renames that user through `StorageManager::rename_identity` as a separate database administrator, and then verifies the stale writer conflicts and records an ABORTED outcome. The original row and index membership remain, and the replacement index membership is absent. LOGIN disable coverage in the same fixture now follows the rename and checks the resulting commit sequence.
- Implementation: no production change was needed; the existing identity epoch guard rejects the stale finalization.
- Verification: the focused regression passed. Under Rust/Cargo 1.95.0, the workspace build, formatting check, warning-denied Clippy, full workspace tests, and serial commit integration target passed. The commit target reported 126 passed and 3 delegated-cgroup tests ignored.
- Remaining: stale-writer coverage for identity drop and other authority-changing DDL routes remains open.

## SM-5 / AU-6: identity drop revokes token admission — verified

- Contract: dropping a user atomically removes its principal and object, records the permanent identity tombstone, and revokes its credentials; a still-unexpired token for the dropped user must fail new invocation admission without creating an invocation outcome.
- Regression: `identity_ddl_drops_unreferenced_user_and_revokes_tokens_atomically` now tries the target user's original token after `StorageManager::drop_identity` commits and verifies authentication fails and no outcome is recorded for the rejected invocation. The existing test also checks principal/object removal, tombstones, and persisted verifier presence.
- Implementation: no production change was needed; identity drop already commits token revocation atomically, and admission honors the verifier's revoked state.
- Verification: the focused real-redb test passed; `cargo fmt --all -- --check`, warning-denied workspace Clippy, and the serial commit integration target passed under Rust/Cargo 1.95.0. The commit target reported 126 passed and 3 delegated-cgroup tests ignored.
- Remaining: stale-writer races for other authority-changing DDL routes and broader repeated concurrency schedules remain open.

## SM-5 / AU-6: token revocation racing a staged write — verified

- Contract: token-authenticated row publication and token revocation serialize so a write committed first remains durable, while a writer finalized after revocation conflicts and records ABORTED; revocation always records its own committed outcome.
- Regression: `token_revocation_racing_a_staged_write_linearizes` starts a catalog-backed staged row write and `StorageManager::revoke_token` from a barrier, then validates the winning serialization, row state, durable outcomes, commit sequence, index/catalog integrity, and rejection of later admission. Twenty fresh-process repetitions all observed write-before-revoke; the existing `token_admission_checks_expiry_secret_revocation_and_commit_guards` regression covers revoke-before-stale-write and durable abort.
- Implementation: no production change was needed; token verifier guards and the serialized redb commit boundary preserve the allowed outcomes.
- Verification: the focused race passed in twenty fresh processes; `cargo fmt --all -- --check`, warning-denied workspace Clippy, and the serial commit integration target passed under Rust/Cargo 1.95.0. The commit target reported 127 passed and 3 delegated-cgroup tests ignored.
- Remaining: repeated race coverage for other authority-changing DDL routes and broader concurrent schedules remains open.

## SM-5 test-fixture correction: CHECK DDL race cleanup audit correlation — fixed

- Red evidence: parallel `cargo test --workspace --locked` and default-parallel `cargo test --test commit` runs intermittently failed in `storage_manager_creates_and_drops_check_constraint` with `InvalidAudit`; the test passed alone and serially.
- Cause: when the staged INSERT won the barrier race, cleanup used drop invocation ID `0xd5` with an audit event whose helper-default correlation ID was `0xd7`. The drop API correctly rejected the mismatched event.
- Fix: the race-winning cleanup branch now sets the audit correlation ID to the actual drop invocation ID before calling `drop_check_constraint`.
- Verification: default-parallel `cargo test --locked --test commit` passed (129 passed, 3 cgroup-dependent ignored), followed by `cargo test --workspace --locked`, which passed across all targets. Rust formatting passed; warning-denied workspace Clippy is recorded with this fix's handoff.
