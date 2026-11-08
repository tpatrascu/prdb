# Client SDK Progress

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
- Remaining: expose a user-facing CLI and credential-file lifecycle, wire the scope builder to a network metadata-discovery API, and exercise successful START through a delegated-cgroup sandbox on a host that supports it.

## SDK-3 dependency: build PSC1 from discovery records — implemented

- Behavior: `RequestedScopeBuilder` accepts authorized schema/relation/column metadata records, adds schema USAGE for selected data objects, encodes table- or column-level operations in canonical PSC1 byte order, deduplicates repeated selections, and enforces the 200-record limit without partial updates.
- Red/green evidence: tests compare a mixed schema/table/column selection byte-for-byte, decode the result independently, reject invalid operation/zero-ID/empty selections, and prove a selection that would exceed the record limit leaves the previous builder state unchanged.
- Remaining: the external client still needs a network API for authorized metadata discovery; the builder itself accepts only records supplied by the caller and grants no authority.

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
- Remaining: add generated bindings and an owned batch-result lifetime API before claiming SDK-1 complete; complete the C SQL/runtime result format and client cancellation surface; connect requested-scope selection to metadata discovery.

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
- Remaining: typed helpers for schemas, tables, constraints and foreign keys; CLI/credential-file lifecycle; and live ADMIN operations through the real multi-process gateway path remain open.

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
