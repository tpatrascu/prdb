# Client SDK Progress

## SDK-3/SDK-5: authenticated metadata discovery CLI — implemented

- Contract: [Client SDK](../../specs/client-sdk.md). `prdb-client discover` lists only authenticated visible schemas, relations, and declared columns, with optional exact-name schema/relation filters. Relation filtering requires a schema. Output is tab-separated, uses lowercase IDs, preserves ordinal order, and includes the declared scalar type and nullability.
- Red/green evidence: a unit fixture checks every initial scalar type spelling and parameterized DECIMAL/VARCHAR metadata. The real gateway/supervisor process test runs the compiled CLI over the authenticated control path for `information_schema.schemata` and checks the schema, relation, and first ordered column records.
- Verification: focused `prdb-client` unit tests and `authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes` passed. Locked workspace build, format check, warning-denied Clippy, serial workspace tests, and `make -f engine/sdk/Makefile test` also passed; the commit suite reported 129 passed and 3 cgroup-dependent tests ignored.
- Remaining: finish other separate-process ADMIN families and SDK packaging/release gates; confined artifact execution remains governed by the Linux sandbox host requirements.

## SDK-3 dependency: ABI v4 PMF1 and query packaging — implemented

- Behavior: PMF1 encode/decode, ELF export inspection, TinyCC packaging, and the C PMF1 encoder accept ABI versions 1–4. ABI v4 requires its exact descriptor OBJECT and entry FUNC exports; ABI 5 remains rejected. The packager still inspects ELF as data and never loads the query.
- Regression: Rust tests cover v4 manifest roundtrip, correct ELF symbols, mismatched entry rejection, and a real TinyCC package build using v4 exports. A second packaging test compiles the maintained optimizer SQL fixture and checks its v4 manifest without loading it. The package fixture's aborting constructor remains unexecuted. The maintained C manifest fixture checks v4 symbol derivation and unsupported-version rejection.
- Verification: the focused Rust manifest and packaging tests, full workspace build/format/Clippy/tests, and `make -f engine/sdk/Makefile test` passed. Three sandbox START tests remain ignored because this host's cgroup2 mount remains read-only even after the authorized remount attempt.
- Filesystem compatibility: publication uses `renameat2(RENAME_NOREPLACE)`. The repository's NFSv4.2 workspace mount returns `EINVAL`; the packager removes its staging directory and now reports `AtomicPublishUnsupported`. The same maintained fixture packages successfully to `/tmp` on ext4.
- Remaining: execute the ABI v4 optimizer callback in the confined acceptance harness on a conforming cgroup host.

## SDK-1 dependency: canonical Rust PMF1 encoder — implemented

- Behavior: `encode_manifest_v1` emits the canonical PMF1 representation only for supported ABI/symbol pairs and bounded fields with sorted, unique dependency names. It round-trips through the decoder to keep the public encoder aligned with admission constraints.
- Regression: the manifest fixture round-trips through the public encoder/decoder and rejects unsorted dependency lists. The real TinyCC TCP submission regression uses the encoder to prepare a valid SEAL request.
- Verification: the encoder unit test, full workspace build/format/Clippy/tests and maintained TinyCC SDK test suite passed.
- Remaining: the current Rust packager handles client uploads; a C-side transport helper could reuse the encoder but is not implemented.

## SDK-1 dependency: bounded C PMF1 encoder — implemented

- Behavior: `prdb_sdk_manifest_encode_v1` writes the canonical 28-byte PMF1 header and body into caller-owned storage, derives ABI symbols from the native ABI version, validates profile/compiler/dependency fields, and requires dependencies in strict bytewise order. It supports size queries and returns the required length without partial output when capacity is short.
- Red/green evidence: the maintained fixture first failed to link because the encoder symbol was missing. It now compares ABI-v3 output byte-for-byte with a hand-checked 124-byte PMF1 fixture and covers size query, insufficient capacity, invalid ABI, empty SDK release, unsorted dependencies, and invalid arguments.
- Verification: the focused fixture and full TinyCC SDK suite passed with the pinned Nix-shell TinyCC. The Rust manifest encoder test remains the independent decoder-roundtrip check.
- Remaining: the bounded Rust packager now inspects ELF and invokes the Rust encoder; the C encoder remains available to future C-side tooling.

## SDK-1 dependency: bounded C PSC1 scope encoder — implemented

- Behavior: `prdb_sdk_scope_encode_v1` emits sorted canonical PSC1 capability records for schema USAGE and table/column SELECT/INSERT/UPDATE/DELETE. It rejects zero IDs, invalid target/operation pairs, duplicate or unsorted records, and counts outside 1–200. Size queries report the required length and undersized buffers remain untouched.
- Red/green evidence: the maintained fixture first failed to link because the encoder was absent. It now matches a hand-checked PSC1 byte fixture, exercises invalid pairs/IDs/order/count, checks no-partial-output behavior, and accepts the maximum 200-column capability scope at 6,806 bytes.
- Verification: the focused TinyCC fixture and forced full SDK archive/test/fixture build passed. Rust workspace build, formatting, warning-denied Clippy, and all workspace tests also passed; the suite reports 109 commit tests passing and two sandbox START tests ignored for missing cgroup delegation.
- Remaining: client-side discovery/selection must supply current object IDs; scope encoding is only a requested ceiling and grants no authority.

## SDK-3 dependency: derive PMF1 inputs from a compiled ELF — implemented

- Behavior: `manifest_v1_from_elf` walks the artifact's ELF data without loading it, validates the selected ABI's descriptor OBJECT and entry FUNC exports, derives strict sorted unique `DT_NEEDED` names, combines them with caller-supplied profile fields, and validates the result through the canonical PMF1 encoder.
- Red/green evidence: the real TinyCC supervisor/gateway submission regression initially failed to compile because the manifest discovery API was absent; it now uses that API to build the PMF1 payload and successfully seals the same artifact. Unit coverage rejects unsupported ABI and malformed ELF.
- Verification: the focused TinyCC TCP regression and complete Rust workspace build, formatting, warning-denied Clippy, and tests passed. The full maintained TinyCC archive/test/fixture build also passed.
- Remaining: the separate bounded packaging dependency is tracked below; this ELF parser/encoder API remains reusable by the package helper and TCP admission path.

## SDK-3 dependency: bounded TinyCC client packager — implemented

- Behavior: `prdb-packager` compiles one C source against the prebuilt SDK archive in a cleared-environment subprocess with CPU, wall-time, address-space, output-file, core-dump, and descriptor limits. It validates PSC1, derives and encodes PMF1 from the compiled ELF, hashes the artifact, then atomically publishes `query.so`, `manifest.pmf1`, `requested_scope.psc1`, and `artifact.sha256` in a private new directory without loading the query.
- Limits: source 1 MiB, SDK archive 128 MiB, PSC1 8 KiB, compiled artifact 64 MiB, diagnostic capture 8 KiB. Package files are mode 0600 and the directory is mode 0700. Existing output paths fail closed.
- Red/green evidence: the end-to-end test builds the maintained TinyCC SDK archive, invokes the actual packager binary, decodes PMF1, verifies exact PSC1 and SHA-256 output, proves a constructor that aborts is not run, and checks that an existing package path is not overwritten.
- Remaining: network scope selection through authorized metadata discovery; package generation alone does not grant authority.

## SDK-3 dependency: authenticated upload and terminal client — implemented

- Behavior: `SubmissionClient` negotiates query-mode protocol v1.0, authenticates with the token ID/secret, validates the private package, PREPAREs it, uploads artifact and parameter chunks in order, verifies every acknowledged offset, and SEALs it. It can then START once, persist a no-overwrite invocation receipt before transmission, stream provisional data, parse the matching supervisor terminal event, and query STATUS on a new authenticated connection after an uncertain START reply.
- Security: caller token-secret memory and temporary AUTH frames are zeroized. Package inputs must be private regular files; PMF1, PSC1, sidecar digest, and size bounds are checked before the network request. START is never automatically replayed.
- Regression: a mock TCP peer verifies handshake/auth, PMF1/PSC1 and SHA-256 preparation fields, a compiled query upload, 300 KB parameters split across chunks, exact offsets, SEAL, receipt-before-START ordering, coalesced START/data/terminal frames, committed status, and zeroized token-secret input. The existing real-binary TCP test remains the gateway/supervisor admission evidence.
- Remaining: broaden live client coverage and finish SDK packaging/release gates; successful START acceptance requires the delegated-cgroup sandbox host described in the Linux sandbox plan.

## SDK-3 dependency: build PSC1 from discovery records — implemented

- Behavior: `RequestedScopeBuilder` accepts authorized schema/relation/column metadata records, adds schema USAGE for selected data objects, encodes table- or column-level operations in canonical PSC1 byte order, deduplicates repeated selections, and enforces the 200-record limit without partial updates.
- Red/green evidence: tests compare a mixed schema/table/column selection byte-for-byte, decode the result independently, reject invalid operation/zero-ID/empty selections, and prove a selection that would exceed the record limit leaves the previous builder state unchanged.
- Remaining: the builder accepts only records supplied by the authenticated `MetadataClient`; the new `discover` CLI exposes those records, and neither component grants authority.

## SDK-1 initial borrowed value view — implemented

- Contract: [`prdb_sdk_value_v1`](../../engine/include/prdb_sdk_value_v1.h) exposes a sized/versioned view with VALUE, NULL, and REDACTED states. Value type codes and canonical bytes match the executor batch protocol. The view borrows bytes from an immutable source batch; it never allocates or frees them.
- Validation: [`value_v1.c`](../../engine/sdk/value_v1.c) checks layout version/size, state/type, the 1 MiB bound, fixed-width types, Boolean values, null/redacted empty representations, and UTF-8 strings. Invalid pointers remain a native caller error and cannot be made safe by this validator.
- Regression: [`sdk_value_v1.c`](../../engine/tests/fixtures/sdk_value_v1.c) covers all states, pointer borrowing, integer widths, malformed null/redacted records, invalid Boolean, invalid UTF-8, unsupported version, and undersized records.
- Red/green evidence: before implementation, TinyCC compilation failed because `prdb_sdk_value_v1.h` did not exist. After implementation, `make -f engine/sdk/Makefile test` passed using TinyCC 0.9.27-unstable-2025-01-06, including archive creation with `tcc -ar` and execution of the maintained test.
- Contract: [`prdb_sdk_batch_v1`](../../engine/include/prdb_sdk_batch_v1.h) opens the executor next-batch wire payload in place, validates it in full, and provides zero-copy row/cell iterators. The caller retains the unchanged response bytes for the lifetime of all views; it owns and releases that buffer.
- Regression: [`sdk_batch_v1.c`](../../engine/tests/fixtures/sdk_batch_v1.c) covers a hand-checked row with VALUE/NULL/REDACTED cells, row/cell iteration, borrowed data, continuation cursor, truncation, unsupported wire version, invalid handle kind and oversized cell count. The test was first compiled before implementation and failed because `prdb_sdk_batch_v1.h` did not exist; it now passes against the TinyCC archive.
- Typed accessors: value state, Boolean, signed 16/32/64-bit scalars, Date, TimestampTz and borrowed raw payload accessors validate state/type and preserve NULL versus REDACTED. Signed conversion handles negative values and minimum representable boundaries without out-of-range signed casts.
- Regression: the expanded value fixture initially failed to compile because `prdb_sdk_value_state_v1` and the typed getters were absent; after implementation it passes against the TinyCC archive, including negative/minimum signed values, no-value statuses, type mismatch, and borrowed text bytes.
- Query host ABI v3: [`prdb_query_abi_v3.h`](../../engine/include/prdb_query_abi_v3.h) adds a sticky SQL statement-failure callback while preserving v1/v2 entry points. The trusted executor latches the callback and forces a nonzero worker exit after user entry returns, so a caught SQL API error still reaches supervisor abort handling. Rust/C layout and descriptor checks and the SDK SELECT fixture cover the new boundary.
- Remaining: add generated bindings before claiming SDK-1 complete and complete the C SQL/runtime result format. Client cancellation and authenticated metadata discovery are implemented in later increments.

## SDK-1: owned batch-result lifetime — implemented

- Contract: [`prdb_sdk_owned_batch_v1`](../../engine/include/prdb_sdk_owned_batch_v1.h) optionally copies a bounded executor batch into SDK-owned memory. The copy is validated before publication; opened batch/row/cell views remain borrowed from it and become invalid on release.
- Red/green evidence: the maintained batch fixture first failed to compile because the owned-batch header was absent. It now checks copy isolation, full validation, borrowed views into the owned copy, active-wrapper rejection, malformed/unsupported payload rejection, release invalidation, idempotent release, and wrapper reuse.
- Implementation: `owned_batch_v1.c` owns the copy and exposes copy/open/release functions; release clears the wrapper and permits reuse. The API reuses the existing batch decoder so owned and directly borrowed payloads receive identical validation.
- Verification: the maintained TinyCC SDK suite passed with `make -f engine/sdk/Makefile BUILD_DIR=/tmp/prdb-sdk-owned-batch test` under TinyCC 0.9.27-unstable-2025-01-06. Rust 1.95.0 workspace build, format, warning-denied Clippy, and full tests also passed; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: generated bindings and the C SQL/runtime result format remain open. Client cancellation and authenticated metadata discovery are implemented in later increments.

## SDK-5 dependency: typed ordinary-index AdminClient — implemented

- Contract: [client SDK](../../specs/client-sdk.md) keeps administrative credentials on an authenticated control connection, provides typed allowlisted operations, and never retries a START whose result is uncertain.
- Implementation: `AdminClient` uses control-mode authentication and exposes index create, rebuild, drop, and status methods. It validates normalized names, nonzero IDs, and ordered distinct column lists locally; START errors that may conceal a committed result return the server-issued operation ID for reconciliation.
- Regression: `client_admin.rs` verifies control HELLO/AUTH, exact create/rebuild/drop PREPARE and START routing, typed response decoding, operation status lookup, token-secret zeroization, and lost START reply behavior. The validation unit test rejects zero IDs, empty/duplicate columns, and malformed names.
- Verification: focused mock-TCP AdminClient tests passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the configured Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored. An initial full-suite run had one non-reproducible `InvalidAudit` failure in the CHECK lifecycle test; that test passed alone and in the complete rerun.
- Remaining: typed client helpers for other administrative operations, CLI/credential-file lifecycle, and live ADMIN operations through the real multi-process gateway path remain open.

## SDK-5 dependency: typed user and role AdminClient operations — implemented

- Contract: [client protocol](../../specs/client-protocol.md) defines the strict PREPARE argument and START result shapes for users and roles; [client SDK](../../specs/client-sdk.md) keeps these credentials on the authenticated control connection and never retries uncertain STARTs.
- Implementation: `AdminClient` exposes user/role create, rename, and drop, user login enable/disable, and role membership grant/revoke. Creation returns the assigned identity ID and commit sequence; user creation does not issue a credential. IDs and canonical lowercase identity names are validated locally. Shared PREPARE/START decoding verifies the operation ID, committed outcome, exact result shape, and canonical commit sequence.
- Regression: mock-TCP coverage checks all eight operation request schemas, PREPARE/START routing, identity result decoding, and existing lost-START reconciliation. Local validation tests cover identity names and nonzero IDs.
- Verification: focused `client_admin` integration tests passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and the repeated `cargo test --workspace --locked` run passed in the Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored. The first full-suite run hit the known intermittent `InvalidAudit` failure in the CHECK lifecycle test; that test passed alone and the complete rerun passed.
- Remaining: typed helpers for schema rename, tables, constraints and foreign keys; CLI/credential-file lifecycle; and live ADMIN operations through the real multi-process gateway path remain open.

## SDK-5 dependency: typed schema create and drop AdminClient operations — implemented

- Contract: [client protocol](../../specs/client-protocol.md) defines owner-bearing schema creation and ID-based schema removal, with generated stable schema IDs returned by START.
- Implementation: `AdminClient::create_schema` validates the normalized name and owner ID and returns the generated schema ID; `drop_schema` validates the target and checks the echoed schema ID in the committed result.
- Regression: mock-TCP coverage verifies exact create/drop arguments, PREPARE/START flow, generated ID, and committed response decoding.
- Verification: focused `client_admin` tests passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: typed constraint and foreign-key helpers; CLI/credential-file lifecycle; and live ADMIN operations through the real multi-process gateway path remain open.

## SDK-5 dependency: typed table create AdminClient operation — implemented

- Contract: [client protocol](../../specs/client-protocol.md) requires ordered typed column definitions and returns the generated table ID and ordered column IDs in START.
- Implementation: `AdminClient::create_table` accepts typed column definitions with SQL scalar types and literal, NULL, or current-principal defaults. It validates names, duplicate columns, UUID identity rules, default type compatibility, decimal parameters, and varchar bounds, then encodes canonical protocol JSON. Its result decoder validates the exact operation response and preserves column ID order.
- Regression: mock-TCP coverage checks an exact table-create request with generated UUID, escaped text default, fixed-scale decimal default, and current-principal UUID default; it checks the ordered returned IDs. Unit coverage rejects invalid column/default combinations.
- Verification: focused `client_admin` and column-encoder tests passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: typed constraint and foreign-key helpers; CLI/credential-file lifecycle; and live ADMIN operations through the real multi-process gateway path remain open.

## SDK-5 dependency: typed table alteration AdminClient operations — implemented

- Contract: [client protocol](../../specs/client-protocol.md) defines separate owner, column add/rename/drop, nullability, and default operations, each with exact PREPARE arguments and durable START results.
- Implementation: `AdminClient` exposes table-owner transfer, column add/rename/drop, nullability changes, and column-default set/drop. Column addition reuses typed column/default encoding while enforcing backfill restrictions. The generalized result validator checks echoed IDs, names, booleans, positive schema versions, operation IDs, and commit sequences.
- Regression: mock-TCP coverage verifies exact wire arguments and START schemas for add/rename/drop column, owner transfer, nullability, and default set/drop. Existing table-create validation exercises the shared column/default encoder.
- Verification: focused `client_admin` tests passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: typed constraint and foreign-key helpers; CLI/credential-file lifecycle; and live ADMIN operations through the real multi-process gateway path remain open.

## SDK-5 dependency: typed table rename and drop AdminClient operations — implemented

- Contract: [client protocol](../../specs/client-protocol.md) defines stable table ID arguments and requires START to echo the table ID for both rename and drop.
- Implementation: `AdminClient::rename_table` validates the normalized new name; `drop_table` validates the nonzero table ID. Both use the shared PREPARE/START path and reject success responses with mismatched table IDs.
- Regression: mock-TCP coverage checks exact rename/drop argument shapes and typed committed-result validation.
- Verification: focused `client_admin` tests passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: typed constraint and foreign-key helpers; CLI/credential-file lifecycle; and live ADMIN operations through the real multi-process gateway path remain open.

## SDK-5 dependency: typed constraint and foreign-key AdminClient operations — implemented

- Contract: [client protocol](../../specs/client-protocol.md) defines ordered distinct key columns, EIR1 row-local CHECK expressions and dependencies, generated constraint/index/expression/FK IDs, and ID-echoing drop results.
- Implementation: `AdminClient` exposes unique and primary-key create/drop, CHECK create/drop, and foreign-key create/drop. Creation uses the canonical expression encoder and the shared exact PREPARE/START response validator; generated IDs and commit sequences are decoded into typed results. CHECK dependency lists may be empty but remain bounded and reject zero or repeated IDs.
- Regression: mock-TCP coverage verifies all eight wire schemas, canonical EIR1 bytes, generated IDs, committed sequence decoding, and empty CHECK dependencies.
- Verification: the focused `client_admin` regression passed. In the pinned Nix shell, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: CLI/credential-file lifecycle and live ADMIN operations through the real multi-process gateway path remain open.

## SDK-5 dependency: private client credential-file lifecycle — implemented

- Contract: [client SDK](../../specs/client-sdk.md) keeps bearer credentials in a private client-only file, rejects unsafe file ownership/permissions, and does not overwrite an existing credential path.
- Implementation: `client_credentials` reads canonical `PDB-CREDENTIALS-1` token ID/secret files, rejects symlinks and non-owner-only modes, atomically creates a private file with no-overwrite publication, and removes owned files. Secret-bearing temporary byte buffers are zeroized.
- Regression: module tests cover round trip, mode 0600, no-overwrite, removal, symlink rejection, and broad-permission rejection. The initial run caught an `O_NOFOLLOW` symlink error mapping issue; the corrected path now returns the documented unsafe-file result.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the pinned Nix shell. The first full-suite attempt hit the known intermittent CHECK lifecycle `InvalidAudit`; the isolated CHECK test and full rerun passed. The commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: user-facing CLI commands and live ADMIN operations through the real multi-process gateway path remain open.

### Credential removal CLI — implemented

- Behavior: `prdb-client credential-remove --credentials FILE` removes a valid owner-only credential file without requiring a network connection. It reuses the credential module's ownership, regular-file, and mode-0600 checks and reports local file removal without exposing token material.
- Contract: [client SDK](../../specs/client-sdk.md) now distinguishes local credential-file deletion from server-side `token-revoke`.
- Regression: the maintained CLI unit test creates a private credential file, runs the removal command handler, and confirms the file is gone; the authenticated gateway/supervisor process test invokes the compiled `prdb-client credential-remove` command after token revocation and confirms the file is removed. Credential module tests separately cover symlink and unsafe-mode rejection.
- Verification: the focused `authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes` test and rustfmt checks pass.
- Remaining: CLI credential lifecycle now supports issue, private save, read, and local removal. Admin CLI breadth and real multi-process ADMIN path coverage continue below.

## SDK-3 dependency: basic submit/status CLI — implemented

- Contract: [client SDK](../../specs/client-sdk.md) requires status reconciliation after uncertain START, a durable no-overwrite receipt before START, and provisional output until a committed terminal outcome.
- Implementation: `prdb-client submit` reads a private credential file, validates a package through `SubmissionClient`, uploads/seals, persists the requested receipt, starts once, streams provisional bytes, and exits successfully only after COMMITTED. `prdb-client status` opens a fresh authenticated connection and queries the supplied invocation ID. Parameters are bounded to the protocol's 1 MiB limit.
- Regression: unit coverage checks canonical nonzero invocation-ID parsing; `prdb-client --help` was exercised to inspect the user-facing commands.
- Verification: `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the pinned Nix shell. The commit integration target reported 124 passed and 3 cgroup-dependent ignored. The focused binary test and `prdb-client --help` check also passed.
- Remaining: credential creation/rotation UX, authenticated metadata-based scope selection, admin CLI commands, and live ADMIN multi-process path tests remain open.

## SDK-3 dependency: authenticated metadata scope CLI — implemented

- Contract: [client SDK](../../specs/client-sdk.md) requires scope selection from authorized metadata records; PSC1 is a requested ceiling and never grants authority.
- Implementation: `prdb-client scope` resolves exact schema/relation/column names using `MetadataClient`, selects schema USAGE and requested table/column operations through `RequestedScopeBuilder`, and atomically publishes a private no-overwrite PSC1 file.
- Regression: CLI tests cover valid operation and column lists, duplicate/unsupported inputs, nonzero invocation parsing, and private no-overwrite PSC1 output. Existing metadata client and scope-builder suites cover protocol response validation and independent PSC1 decoding.
- Verification: focused `prdb-client` tests passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the pinned Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored. The existing authenticated metadata process test passed, and updated CLI help was inspected.
- Remaining: credential creation/rotation UX, admin CLI commands, and live ADMIN multi-process path tests remain open.

## SDK-5 dependency: token issue/revoke CLI and credential rotation — implemented

- Contract: [client protocol](../../specs/client-protocol.md) returns token secrets exactly once; [identity DDL](../../specs/ddl-identity.md) requires the caller to preserve a usable administrator credential and never log bearer material.
- Implementation: `prdb-client token-issue` calls authenticated `AdminClient::issue_token`, computes a bounded absolute expiration from a positive TTL, and atomically writes the one-time result to a new private credential file without printing the secret. `token-revoke` exposes explicit revocation. If issuance commits but saving fails, the CLI reports the operation/token IDs and directs the operator to revoke before retrying. Rotation keeps the old credential until the new one is saved and verified.
- Regression: CLI tests cover zero/overflow TTL rejection; credential file tests cover the private no-overwrite publication path; existing AdminClient mock-TCP tests cover the one-time issue/revoke protocol and malformed secret handling.
- Verification: focused CLI/credential tests and the existing one-time `AdminClient` token issue/revoke mock-TCP regression passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the pinned Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored. CLI help was inspected.
- Remaining: general admin DDL CLI commands and live ADMIN multi-process path tests remain open.

## SDK-5 dependency: identity and membership admin CLI — implemented

- Contract: [client protocol](../../specs/client-protocol.md) defines user/role lifecycle, login state, and membership PREPARE/START operations; storage revalidates authority and protects the last usable administrator credential.
- Implementation: `prdb-client` exposes user/role create, rename, and drop, user login enable/disable, and role membership grant/revoke. It uses typed `AdminClient` methods, validates IDs and boolean flags before sending, and reports generated IDs and durable operation metadata.
- Regression: CLI parser tests cover canonical login booleans; existing mock-TCP integration covers each identity/membership operation shape and server response validation.
- Verification: focused CLI tests passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the pinned Nix shell. The commit integration target reported 124 passed and 3 cgroup-dependent ignored. The initial full-suite attempt hit the intermittent CHECK lifecycle `InvalidAudit`; the isolated regression and full rerun passed. Final CLI help was inspected.
- Remaining: table/schema/constraint/policy admin CLI commands and live ADMIN multi-process path tests remain open.

## SDK-5 / CP-1: real multi-process ADMIN identity and token lifecycle — verified

- Contract: the control-mode AdminClient must cross the dedicated gateway and supervisor process boundary; storage must independently enforce authority, token persistence, and revocation.
- Regression: `authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes` now creates/renames/drops a role through `AdminClient`, creates a user, issues a token, writes and reloads the private credential file, authenticates a fresh MetadataClient through the processes, revokes the token, then proves that token cannot authenticate again. The test also checks secret input buffers are zeroized.
- Verification: the focused real-process test passed. Full workspace build, format check, warning-denied Clippy, and tests passed; the commit integration target reported 124 passed and 3 cgroup-dependent ignored. One separate sandbox-dependent process test remains ignored.
- Remaining: extend multi-process ADMIN coverage to schema/table/constraint/privilege DDL and add the remaining admin DDL CLI commands.

## SDK-5 dependency: typed schema rename AdminClient operation — implemented

- Contract: [client protocol](../../specs/client-protocol.md) requires `schemas.rename` START to echo the stable schema ID and normalized new name.
- Implementation: `AdminClient::rename_schema` validates ID and name and accepts success only when both echoed values, the operation ID, committed outcome, and commit sequence match. The shared response validator now supports this exact echoed-name shape.
- Regression: schema mock-TCP coverage verifies create, rename, and drop requests and checks the rename response includes the expected schema ID and name.
- Verification: focused schema lifecycle mock-TCP test passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: typed table, constraint and foreign-key helpers; CLI/credential-file lifecycle; and live ADMIN operations through the real multi-process gateway path remain open.

## SDK-5 dependency: typed system privilege AdminClient operations — implemented

- Contract: [client protocol](../../specs/client-protocol.md) allowlists the closed database-scoped privilege set: `manage_users`, `manage_roles`, and `create_schema`.
- Implementation: `AdminClient` exposes grant/revoke methods taking a typed `AdminSystemPrivilege`; the shared administrative operation path enforces exact START response fields and uncertain-outcome handling.
- Regression: mock-TCP coverage verifies exact grant/revoke operation names and wire arguments for representative privilege values.
- Verification: focused `client_admin` tests passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `cargo test --workspace --locked` passed in the Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: typed helpers for schemas, tables, constraints and foreign keys; CLI/credential-file lifecycle; and live ADMIN operations through the real multi-process gateway path remain open.

## SDK-5 dependency: typed token lifecycle AdminClient operations — implemented

- Contract: [client protocol](../../specs/client-protocol.md) returns issued bearer material exactly once and never through STATUS. A lost issue result requires reconciliation followed by explicit revoke/reissue.
- Implementation: `AdminClient::issue_token` and `revoke_token` use the authenticated control PREPARE/START path. Issue validates the user ID, UTF-8 label byte bound, and positive expiration; JSON-escapes labels; validates the committed response; and decodes only canonical fixed-width base64. `AdminTokenResult` omits secret material from Debug and zeroizes the secret on drop. The response frame, receive buffer, and partially buffered decoder payload are zeroized.
- Regression: mock-TCP coverage verifies exact issue/revoke schemas, escaped label content, one-time secret decoding, token ID and sequence, plus malformed base64 rejection.
- Verification: focused `client_admin` and secret-decoder tests passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and the repeated `cargo test --workspace --locked` run passed in the Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored. The initial full-suite run hit the known intermittent `InvalidAudit` CHECK lifecycle failure; the test passed alone and the complete rerun passed.
- Remaining: typed helpers for administrative privileges, schemas, tables, constraints and foreign keys; CLI/credential-file lifecycle; and live ADMIN operations through the real multi-process gateway path remain open.

## SDK-5: schema/table DDL CLI and process coverage — implemented

- Contract: the client SDK routes schema/table administration through authenticated `AdminClient` PREPARE/START operations; query packages never receive administrative credentials.
- Implementation: `prdb-client` exposes schema create/rename/drop and table create/rename/drop. Table columns use a semicolon-separated typed declaration syntax, validated locally before the existing typed table-create request.
- Regression: CLI tests check the parameterized type parser and invalid schema shapes. The real gateway/supervisor process test now creates a schema and table, renames and drops the table, then drops the schema.
- Verification: focused CLI tests and the focused multi-process ADMIN lifecycle passed. The final locked workspace build, format check, warning-denied Clippy, and full tests passed; the commit integration target had 124 passed and 3 cgroup-dependent ignored. CLI help was inspected after correcting its continuation formatting.
- Remaining: general DDL CLI commands for columns, constraints, indexes, privileges, and policies; additional multi-process coverage; and confined artifact submission remain open.

## SDK-5: column DDL CLI and process coverage — implemented

- Contract: column changes use the authenticated ADMIN control channel and storage's typed, fenced schema-evolution operations.
- Implementation: `prdb-client` exposes add, rename, and drop column commands. Add accepts one nullable column declaration; rename/drop use explicit table and column IDs.
- Regression: parser tests reject unsupported non-null, identity, and multi-column add forms. The real gateway/supervisor integration now exercises nullable add, rename, and drop.
- Verification: focused CLI and process tests passed; final workspace build, formatting, warning-denied Clippy, and all workspace tests passed. The commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: CLI support for nullability/default operations, indexes, constraints, privileges, policies, more real-process DDL, and confined artifact execution remains open.

## SDK-5: column nullability/default CLI and process coverage — implemented

- Contract: the client exposes typed nullability/default changes through the authenticated ADMIN protocol; storage validates current rows and schema before commit.
- Implementation: CLI commands set column nullability and set/drop defaults. Default literals are parsed using the declared scalar type, including SQL NULL, `current-principal-id`, decimal coefficients, lowercase BYTEA hex, UUIDs, dates, and timestamps.
- Regression: parser tests cover decimal scaling and invalid scale, BYTEA decoding, principal UUID defaults, and type mismatch. The real-process ADMIN test exercises nullability changes and setting/dropping a literal default.
- Verification: focused CLI and process tests passed; final workspace build, formatting, warning-denied Clippy, and full test suite passed after isolating and rerunning the known intermittent CHECK lifecycle test. Commit integration had 124 passed and 3 cgroup-dependent ignored.
- Remaining: index, constraint, privilege, and policy CLI commands; broader live-process DDL coverage; and confined artifact execution remain open.

## SDK-5: ordinary index CLI and process coverage — implemented

- Contract: index administration stays on the authenticated AdminClient channel and uses ordered, distinct nonzero column IDs.
- Implementation: CLI commands now expose ordinary index create, rebuild, and drop; the uniqueness flag defaults to false.
- Regression: command parsing checks canonical IDs and duplicate detection. The real gateway/supervisor process test invokes `prdb-client index-create`, `index-rebuild` with a changed uniqueness profile, and `index-drop` using a private credential file; the typed-client and ADMIN dispatcher regressions also cover rebuild.
- Verification: `cargo test --locked --test metadata_process authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes -- --exact --nocapture` passed (1 test). `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` passed in the configured Nix shell.
- Remaining: broaden process-level CLI coverage for other DDL families and complete remaining SDK packaging/release gates.

## SDK-5 / SM-5: repeat same-profile index rebuild — implemented

- Contract: the typed `indexes.rebuild` operation is useful even when the replacement profile equals the existing one; it must validate and republish memberships under the authorized table fence.
- Red evidence: repeating the same profile first returned a sanitized `unavailable` from the process path; the real-redb regression exposed `Commit(InvalidCatalog)`.
- Implementation: explicit unchanged index-definition writes now flow through affected-table fencing, current-authority checks, membership rebuild, durable audit/outcome, and write-epoch advance.
- Regression: a populated index is rebuilt twice with the same definition and checks its durable outcome/sequence. The real gateway/supervisor lifecycle also repeats an unchanged profile after changing uniqueness.
- Verification: focused redb and process tests passed; full workspace build, format, warning-denied Clippy, and tests passed; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.

## SDK-5: unique and primary-key constraint CLI — implemented

- Contract: key constraints use ordered, distinct nonzero column IDs and typed AdminClient PREPARE/START operations; storage remains authoritative for schema, nullability, authority, dependencies, backfill, and uniqueness.
- Implementation: prdb-client exposes unique-create and primary-key-create with --table-id and --columns, and matching drop commands with --constraint-id. Creation reports generated constraint and backing-index IDs.
- Red evidence: the first real-process run after column addition failed with `Commit(InvalidCatalog)`; the generated backing index had conflated its fixed format version with the table's schema version.
- Regression: CLI parser coverage accepts ordered IDs and rejects empty, duplicate, and zero IDs. The real gateway/supervisor ADMIN lifecycle now invokes `unique-create`, `unique-drop`, `primary-key-create`, and `primary-key-drop` through `prdb-client`; the primary-key flow first makes the column non-null and restores nullability after dropping the key. Typed AdminClient coverage continues to exercise constraint dependencies and backfill validation.
- Verification: `cargo test --locked --test metadata_process authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes -- --exact --nocapture` passed (1 test). `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` passed in the configured Nix shell.
- Remaining: broaden process-level CLI coverage for other DDL families and complete remaining SDK packaging/release gates.

## SDK-5: system privilege grant/revoke CLI — implemented

- Contract: only the closed database-scoped privilege set (manage_users, manage_roles, create_schema) is accepted; the authenticated AdminClient and storage revalidate administrator authority.
- Implementation: prdb-client exposes system-privilege-grant and system-privilege-revoke with grantee ID and typed privilege value.
- Regression: the parser accepts all three values and rejects case changes and unsupported privilege names. The real gateway/supervisor ADMIN lifecycle exercises typed `AdminClient` grant/revoke and invokes `system-privilege-grant`/`system-privilege-revoke` through `prdb-client` for a role.
- Verification: `cargo test --locked --test metadata_process authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes -- --exact --nocapture` passed (1 test). `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` passed in the configured Nix shell.
- Remaining: broaden process-level CLI coverage for remaining DDL families and complete remaining SDK packaging/release gates.

## SDK-5: object grant/revoke CLI process path — verified

- Contract: object and column privileges use the authenticated ADMIN control channel with a closed privilege set and an explicit object or column target; storage rechecks the actor's current authority.
- Regression: the real gateway/supervisor ADMIN lifecycle invokes `object-grant` and `object-revoke` through `prdb-client` for a non-admin user and a column-scoped SELECT privilege. The command output confirms each durable operation completed.
- Verification: `cargo test --locked --test metadata_process authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes -- --exact --nocapture` passed (1 test). `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` passed in the configured Nix shell.
- Remaining: extend process-level CLI coverage to other DDL families and complete remaining SDK packaging/release gates.

## SDK-5: foreign-key create/drop CLI — implemented

- Contract: foreign-key administration uses ordered, distinct source column IDs and a target PRIMARY KEY/UNIQUE constraint ID; storage validates column correspondence, types, authority, existing rows, and dependencies.
- Implementation: prdb-client exposes foreign-key-create and foreign-key-drop through typed AdminClient calls and reports the generated foreign-key ID.
- Regression: foreign-key source IDs use the same strict nonempty, distinct ID-list parser as key constraints. The real gateway/supervisor process test grants column-level REFERENCES, creates and drops the foreign key, then revokes REFERENCES.
- Verification: focused decoder, parser, CLI help, and real-process checks passed. The full workspace build, format check, warning-denied Clippy, and rerun of the full test suite passed; the commit integration target reported 124 passed and 3 cgroup-dependent ignored. The first full test run had one intermittent existing CHECK lifecycle `InvalidAudit` failure, and that test passed alone.
- Remaining: policy CLI, broader live ADMIN DDL coverage, and confined artifact execution remain open.

## SDK-5: CHECK constraint CLI — implemented

- Contract: CHECK creation accepts a canonical EIR1 row-local Boolean expression and its declared dependency columns; storage validates binding, type, current rows, and authority.
- Implementation: prdb-client exposes check-create and check-drop. It decodes strict lowercase even-length hex locally, accepts an empty dependency list for constant expressions, then delegates typed validation and publication to AdminClient.
- Regression: parser coverage accepts canonical hex and empty dependencies while rejecting empty/odd/uppercase/nonhex input. The real gateway/supervisor ADMIN lifecycle creates and drops a constant-true CHECK after column addition.
- Verification: focused parser and real-process tests, CLI help inspection, format check, full workspace build, warning-denied Clippy, and tests passed under Rust/Cargo 1.95.0; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: policy CLI, broader live ADMIN DDL coverage, and confined artifact execution remain open.

## SDK-5: row/cell policy ADMIN and CLI — implemented

- Contract: policy creation uses a closed row/cell kind, operation, ALLOW/RESTRICT effect, typed user/role recipients, optional cell-column targets, and operation-required EIR1 clauses. Storage binds the expressions to current schema and checks current `MANAGE_POLICIES` authority.
- Implementation: `AdminClient` exposes typed create/drop policy methods; `prdb-client policy-create/policy-drop` uses the authenticated control channel. The CLI parses canonical recipient IDs and EIR1 hex without exposing policy bodies through metadata discovery.
- Regression: strict decoder coverage checks a canonical row SELECT policy and drop shape. The real gateway/supervisor lifecycle creates and drops row SELECT/INSERT/UPDATE/DELETE plus cell SELECT/INSERT/UPDATE policies, checks committed sequences, binds cell targets to a live column, and sends the operation-required USING/WITH CHECK clauses. It also invokes `policy-create`/`policy-drop` through `prdb-client` for a row SELECT policy with canonical EIR1 Boolean expression bytes; parser coverage rejects unknown kinds, uppercase IDs, and zero IDs.
- Verification: `cargo test --locked --test metadata_process authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes -- --exact --nocapture` passed (1 test). `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` passed in the configured Nix shell.
- Remaining: other live ADMIN DDL families and confined artifact execution remain open.

## SDK-5: table-owner transfer CLI and process coverage — implemented

- Contract: [`tables.set_owner`](../../specs/client-protocol.md) accepts a table ID and destination live user/role ID through the authenticated ADMIN channel. The current owner or a database administrator may transfer a non-system table; the new owner receives table-level DDL authority and no data privileges.
- Red evidence: the parser regression first failed to compile because `take_table_owner_args` and the `table-owner-set` command did not exist.
- Implementation: `prdb-client table-owner-set --table-id HEX --owner-id HEX` parses strict nonzero lowercase IDs, rejects extra fields, calls `AdminClient::set_table_owner`, and reports the operation ID and commit sequence.
- Regression: the CLI parser accepts canonical IDs and rejects zero, uppercase, short, and extra values. `authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes` transfers a table from the administrator to a newly credentialed non-admin user, then invokes the compiled `prdb-client table-owner-set` command with that user's private credential to transfer it back. The temporary token and credential file are revoked and removed before shutdown.
- Verification: focused CLI parser and real multi-process ADMIN tests passed. `cargo build --workspace --locked --offline`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`, and `cargo test --workspace --locked --offline -- --test-threads=1` passed in the pinned Nix shell. CLI help displays the new command. The full suite reported 3 cgroup-gated commit tests and 1 cgroup-gated metadata process test as ignored by default.
- Remaining: broaden separate-process DDL coverage and finish confined artifact execution and the remaining SDK packaging/release gates.


## SDK-5: schema-owner transfer client and CLI — implemented

- Contract: [`schemas.set_owner`](../../specs/client-protocol.md) lets the current schema owner or a database administrator transfer a non-system schema to a live user or role. The transfer does not grant data privileges.
- Red evidence: `client_admin` failed to compile because `AdminClient::set_schema_owner` did not exist.
- Implementation: `AdminClient::set_schema_owner` validates both IDs and validates the returned schema and owner ID echoes. `prdb-client schema-owner-set --schema-id HEX --owner-id HEX` applies strict ID parsing and reports the committed operation.
- Regression: the AdminClient mock verifies request and response fields. CLI parser checks reject zero, uppercase, short, and extra IDs. The real gateway/supervisor lifecycle transfers a schema to a newly credentialed non-admin user and invokes the compiled CLI with that user's credential to transfer it back.
- Verification: the focused AdminClient mock, CLI parser, and real multi-process CLI regressions passed. Rust 1.95.0 workspace build, formatting, warning-denied Clippy, and full serial workspace tests passed. The commit suite reported 127 passed and 3 cgroup-dependent ignored; the metadata-process suite reported 1 passed and 1 cgroup-dependent ignored.
- Remaining: broader separate-process DDL coverage and confined artifact execution remain open.


## SDK-4: owner-scoped cancellation client and CLI — implemented

- Contract: [`CANCEL`](../../specs/client-protocol.md) accepts a caller-owned prepared or active invocation and returns accepted, too-late, terminal, or unknown state. Acknowledgement is not a transaction terminal event.
- Red evidence: the new protocol regression failed to compile because `CancellationResult` and `SubmissionClient::cancel` did not exist.
- Implementation: `SubmissionClient::cancel` sends kind 9 once, validates the response shape and closed state set, and rejects a zero invocation ID. `prdb-client cancel` authenticates from the private credential file and prints the server acknowledgement.
- Regression: mock TCP coverage verifies kind, invocation ID, secret zeroization, and all five valid state results. The real gateway/supervisor lifecycle cancels an already committed owned invocation through both the Rust client and compiled CLI, confirming committed terminal status is returned.
- Verification: the focused cancellation mock and real multi-process lifecycle passed. Rust 1.95.0 workspace build, format check, warning-denied Clippy, and full serial tests passed; the commit suite reported 127 passed and 3 cgroup-dependent ignored, and the metadata-process suite reported 1 passed and 1 cgroup-dependent ignored. `make -f engine/sdk/Makefile test` also passed with TinyCC 0.9.27-unstable-2025-01-06.
- Remaining: generated bindings and other SDK/package release gates remain open.


## SDK-5: administrator-authorized invocation cancellation — implemented

- Contract: [`CANCEL`](../../specs/client-protocol.md) in control mode requires current database-administrator authority and reports the invocation cancellation decision.
- Red evidence: the control-mode regression failed to compile because `AdminClient::cancel_invocation` did not exist.
- Implementation: `AdminClient::cancel_invocation` sends the typed cancellation request on its existing authenticated control connection. `prdb-client admin-cancel` loads a private administrator credential and reports the returned decision.
- Regression: the mock verifies control HELLO/AUTH and kind-9 invocation cancellation. The live gateway/supervisor lifecycle uses both `AdminClient` and the compiled `admin-cancel` command on a committed invocation, verifying terminal reconciliation through the production control path.
- Verification: the focused control-channel mock and real multi-process ADMIN/CLI regressions passed. Rust 1.95.0 workspace build, format check, warning-denied Clippy, and full serial workspace tests passed; the commit suite reported 127 passed and 3 cgroup-dependent ignored, and the metadata-process suite reported 1 passed and 1 cgroup-dependent ignored.
- Remaining: generated bindings and other SDK/package release gates remain open.

## SDK-1 dependency: C executor cancellation checkpoint — implemented

- Contract: executor IPC `0x1113` returns a versioned cancellation/deadline state; storage aborts the invocation before reporting cancellation.
- Implementation: `prdb_sdk_executor_check_cancelled_v1` provides a bounded C SDK call for query runners to poll between work batches and validates exact response length, version, and boolean value.
- Regression: the maintained TinyCC executor fixture verifies active and cancelled values, malformed boolean rejection, and unchanged output on error. A Rust storage/IPC regression checks the state before and after administrator cancellation.
- Verification: focused fixture passed; full Rust and SDK checks recorded with the feature commit.
- Remaining: verify cancellation latency in the sandbox harness, and complete generated bindings and other SDK/package release gates.
