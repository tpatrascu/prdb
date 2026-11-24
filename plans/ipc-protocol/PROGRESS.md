# Internal IPC Progress

## IPC-7 / SQL-10 follow-up: batched index probe frame — implemented slice

- Contract: `0x1117` carries 1–128 typed leading-key or two-to-eight-column prefix probes and returns ordered result groups, or one unavailable status without partial groups. The fixed frame ceiling and aggregate 16,384-candidate/1 MiB response limits are enforced.
- Implementation: added generated operation code and executor role dispatch, Rust request/response codecs and connection handler, TinyCC request encoder and grouped-response iterator, and changed the SQL-10 nested-loop runner to issue one request per group of up to 128 outer rows. Cancellation is checked before and after the batch; group ordinals preserve duplicate outer-row association.
- Verification: generated ABI consistency tests, 8 executor IPC tests, 8 IPC tests, `cargo fmt --all -- --check`, maintained TinyCC SDK tests, and release-profile validation passed in the pinned Nix shell. The SQL v19 fixture verifies eight ordered result groups in one probe frame and no partial output when aggregate candidate limits are exceeded. Confined SQL acceptance passed with exact indexed/hash results under 192 concurrent operational reads and commits; the range-probe batch ran through the actual executor/storage path.
- Remaining: broader duplicate/NULL, overlay, denied-column and composite-prefix batch cases; batch mutation operations `0x1118`–`0x111a`; planner metadata batches `0x1209`/`0x120a`; and configurable profile propagation remain open.

## IPC-7 follow-up: mutation batch codec and dispatcher — implemented slice

- Contract: mutation batch payloads carry a version/count header and length-delimited scalar mutation records. The complete batch is decoded before staging, and a member failure aborts the invocation so no prefix can commit.
- Implementation: Rust codecs and executor dispatch now support `stage_inserts` (`0x1118`), `stage_updates` (`0x1119`), and `stage_deletes` (`0x111a`); the TinyCC SDK exposes bounded frame encoders for all three. Insert responses preserve per-row handle order. Existing scalar operations remain compatible.
- Verification: focused executor IPC tests pass (10 tests), covering round-trips, trailing/truncated records, duplicate handles, and ordered insert handles. The maintained TinyCC SDK suite and ABI generator checks pass. SQL INSERT/UPDATE/DELETE runners have not yet been switched to these batch helpers; no mutation-batch storage or confined acceptance is claimed.
- Remaining: migrate SQL multi-row INSERT and collected UPDATE/DELETE targets to payload-bounded batch calls; verify no-prefix abort and later-chunk rollback through storage, then add confined acceptance. Planner batching and configurable bounds remain open.

## IPC-2 follow-up: TinyCC unique-index probe uses the shared scalar-value codec — implemented

- Contract: the `0x1114` key scalar uses the common record `{type_code:u8, value_length:u32be, value_bytes[value_length]}`.
- Implementation: replaced the point-probe producer's handwritten type/length writes and value copy with the generated scalar header and byte-tail helpers. The existing TinyCC optimizer fixture independently checks the request's type, length, and value bytes.
- Verification: maintained TinyCC SDK tests, sandbox query fixture, locked workspace build, formatting check, warning-denied Clippy, full serial delegated-cgroup workspace suite (zero ignored tests), and `git diff --check` passed.
- Remaining: SDK consumption, optimizer selection, end-to-end optimized/reference result and error equivalence, and broader malformed process-level IPC coverage remain open.

## IPC-2 follow-up: TinyCC range bounds use the shared scalar-value codec — implemented

- Contract: optional `0x1115` lower and upper bounds encode as the common scalar record: one-byte type code, big-endian `u32` value length, then exactly that many value bytes.
- Implementation: replaced the SDK's handwritten type/length writer and value copy with the generated scalar-record header and byte-tail helpers. The range request's independent golden-vector fixture retains the byte-for-byte wire check.
- Specification: documented the exact scalar record alongside the generated bound flags and column-count record.
- Verification: maintained TinyCC SDK tests, sandbox query fixture, locked workspace build, formatting check, warning-denied Clippy, full serial delegated-cgroup workspace suite (zero ignored tests), and `git diff --check` passed.
- Remaining: SDK consumption, optimizer selection, end-to-end optimized/reference result and error equivalence, and broader malformed process-level IPC coverage remain open.

## IPC-2 follow-up: generated index-range bound flag record — implemented

- Contract: each `0x1115` bound slot starts with a one-byte flag: 0 absent, 1 inclusive, or 2 exclusive; a present flag is followed by one scalar record.
- Red evidence: the shared-schema generator regression failed because no Rust/C codec existed for the bound flag.
- Implementation: added the generated flag record and migrated Rust request encoding/decoding and the TinyCC SDK request builder. Updated the IPC spec with the record layout.
- Regression: the range request wire test checks both lower and upper flag bytes, including inclusive and exclusive values; existing C fixtures verify the assembled request bytes.
- Verification: generator tests/drift check, focused Rust range codec test, maintained TinyCC SDK tests, sandbox query fixture, locked workspace build, formatting check, warning-denied Clippy, full serial delegated-cgroup workspace suite (zero ignored tests), and `git diff --check` passed.
- Remaining: SDK consumption, optimizer selection, end-to-end optimized/reference result and error equivalence, and broader malformed process-level IPC coverage remain open.

## IPC-2 follow-up: generated index-range request column-count record — implemented

- Contract: the `0x1115` variable tail ends its optional lower/upper scalar records with a big-endian `u16` selected-column count followed by exactly that many 16-byte IDs.
- Red evidence: the ABI generator regression failed because no shared Rust/C record existed for that count.
- Implementation: generated the count record and migrated the Rust request encoder/decoder and TinyCC SDK request builder to it. Updated the IPC spec to make the count encoding explicit.
- Regression: the Rust request wire-vector checks the exact count bytes at the boundary between scalar bounds and column IDs; existing TinyCC executor fixtures compare the request against expected bytes.
- Verification: generator tests/drift check, focused Rust range codec test, maintained TinyCC SDK tests, sandbox query fixture, locked workspace build, formatting check, warning-denied Clippy, full serial delegated-cgroup workspace suite (zero ignored tests), and `git diff --check` passed.
- Remaining: SDK consumption, optimizer selection, end-to-end optimized/reference result and error equivalence, and broader malformed process-level IPC coverage remain open.

## IPC-2 follow-up: generated unique-index probe response records — implemented

- Contract: executor `0x1114` returns exact v1 unavailable/no-match records or a v1 match prefix followed by one executor batch-row record.
- Red evidence: the shared-schema generator regression failed because the three response variants had no generated Rust/C records.
- Implementation: defined generated variant headers and a shared version constant. Rust response encoding/decoding and TinyCC optimizer response validation now use the generated records; the IPC specification states the exact variant layouts.
- Regression: Rust wire-vector coverage checks all three status prefixes and round-trips the match row. Existing TinyCC optimizer fixtures exercise unavailable, no-match, and match behavior.
- Verification: generator tests/drift check, focused Rust response regression, maintained TinyCC SDK tests, sandbox query fixture, locked workspace build, formatting check, warning-denied Clippy, full serial delegated-cgroup workspace suite (zero ignored tests), and `git diff --check` passed.
- Remaining: SDK consumption, optimizer selection, end-to-end optimized/reference result and error equivalence, and broader malformed process-level IPC coverage remain open.

## IPC-2 follow-up: generated index-range row cell-count record — implemented

- Contract: every range result row begins with the existing big-endian `u16` cell count before its sanitized cell records.
- Red evidence: the shared-schema generator regression failed because no Rust/C `INDEX_RANGE_RESPONSE_ROW_RECORD` codec existed.
- Implementation: added the two-byte shared record and migrated Rust response encoding/decoding plus both TinyCC row-count parsing paths to its generated helpers. Added exact-byte coverage for the row cell count.
- Verification: generator tests and drift check, focused Rust codec regression, maintained TinyCC SDK tests, sandbox query fixture, locked workspace build, formatting check, warning-denied Clippy, full serial delegated-cgroup workspace suite (zero ignored tests), and `git diff --check` passed.
- Remaining: SDK consumption, optimizer selection, end-to-end optimized/reference result and error equivalence, and broader malformed process-level IPC coverage remain open.

## IPC-2 follow-up: generated index-range response headers — implemented

- Contract: `0x1115` returns either the exact v1 unavailable record `{version:u8=1,status:u8=0}` or the rows prefix `{version:u8=1,status:u8=1,row_count:u16be}` followed by sanitized rows.
- Implementation: added shared Rust/C codecs for both response headers. Rust response encoding/decoding and the TinyCC SDK parser now use generated codecs while retaining status, row-count, cell, payload-size, and no-partial-page validation. Updated the IPC specification with the byte layout.
- Regression: the Rust wire test checks the exact rows prefix and unavailable bytes; ABI generator tests require the generated declarations/codecs, and the TinyCC SDK fixture exercises the C parser.
- Verification: locked workspace build, formatting check, warning-denied Clippy, maintained TinyCC SDK tests, sandbox query fixture build, and the full serial workspace suite inside the delegated user service passed; all test targets reported zero ignored tests. `git diff --check` passed.
- Remaining: SDK consumption, optimizer selection, end-to-end optimized/reference result and error equivalence, and broader malformed process-level IPC coverage remain open.

## IPC-2 follow-up: bounded executor index range request — verified

- Behavior: executor operation `0x1115` accepts at least one typed leading-key bound and selected columns; storage revalidates the relation, catalog index, key readability, row policy, and staged-write visibility. Results expose sanitized cells only and return `unavailable` rather than a partial or oversized payload.
- Implementation: generated operation tag and role allowlist, bounded v1 request/response codecs, principal-bound catalog entry point, and executor dispatch are implemented. Information-schema relations safely return `unavailable`.
- Regression: bound flags and scalar types round-trip; malformed flags, truncation, trailing bytes, response status, oversized fallback, and executor/planner role separation are covered. The existing transaction range regression covers hidden and conditional keys and overlay reconciliation.
- Verification: ABI generator checks, focused Rust codec/IPC tests, workspace build, formatting, warning-denied Clippy, maintained TinyCC SDK tests and fixture build passed. The full serial workspace suite passed inside the delegated user service (193 library tests, 148 commit tests, 74 transaction tests; zero ignored tests).
- Remaining: SDK consumption, optimizer selection, end-to-end optimized/reference result and error equivalence, and broader malformed process-level IPC coverage remain open.

## IPC-3 dependency: generated supervisor control request validation — implemented

- Behavior: status and administrative-cancel requests are decoded and validated through their exact schema-generated operation layouts.
- Implementation: [supervisor control dispatcher](../../engine/src/supervisor_ipc.rs) no longer manually reads the common version prefix before invoking those decoders.
- Verification: focused owner-scoped status/cancel and control-socket tests passed; locked build, formatting, warning-denied Clippy, serial workspace tests (129 passed, 3 cgroup-dependent ignored), and TinyCC SDK suite passed.
- Remaining: role/handle tests across real processes, queue/deadline hardening, and worker lifecycle coverage remain open.

## IPC-6 dependency: generated terminal request encoders — implemented

- Behavior: the gateway sends the exact generated version-only request for finalize or abort and emits no terminal event for an unrelated operation.
- Changed code and regression: [supervisor terminal relay](../../engine/src/supervisor_ipc.rs) uses operation-specific generated encoders; the unit regression checks exact bytes and rejects status lookup as a terminal operation.
- Verification: focused regression, locked workspace build/format/warning-denied Clippy/serial tests and the maintained TinyCC SDK suite passed. Three cgroup-dependent tests remain ignored by default.
- Remaining: other hand-coded payloads, process-level queue/deadline hardening, and worker lifecycle coverage remain open.

## IPC-2 dependency: generated executor relation response prefixes — implemented

- Behavior: relation resolution encodes an exact unavailable variant or a successful fixed prefix followed by bounded authorized relation metadata.
- Implementation: [shared ABI schema](../../engine/abi/query_abi_constants.json) now defines both response prefixes; [executor IPC](../../engine/src/executor_ipc.rs) uses the generated codecs and keeps handle validation and variable-tail parsing in Rust.
- Regression and verification: exact-byte round-trip and malformed variant coverage pass, as do ABI generation checks, locked workspace build, formatting, warning-denied Clippy, serial workspace tests (129 passed, 3 cgroup-dependent ignored), and the maintained TinyCC SDK suite.
- Remaining: other IPC payload migrations, process-level queue/deadline hardening, and worker lifecycle coverage remain open.

## IPC-6 dependency: generated terminal acknowledgement validation — implemented

- Behavior: the gateway accepts storage finalize/abort acknowledgements only when the operation has its exact versioned response layout and a success status.
- Implementation: [supervisor terminal relay](../../engine/src/supervisor_ipc.rs) now uses shared schema-generated decoders rather than duplicating response widths and offsets.
- Regression and verification: valid responses and truncated, unsupported-version, nonzero-status, and trailing-byte responses are covered. The focused regression and full locked Rust/TinyCC validation suite passed; the serial workspace suite reported 129 passed and 3 cgroup-dependent ignored tests.
- Remaining: additional hand-coded IPC payload validation, process-level queue/deadline hardening, and worker lifecycle coverage remain open.

## IPC-6 dependency: canonical gateway error payload validation — implemented

- Behavior and contract: [`GatewayClientError`](../../specs/ipc-protocol.md) is a supervisor-to-gateway event whose payload must be one of the five fixed, sanitized v1 JSON errors. The gateway now compares the complete payload bytes against that allowlist before encoding a client ERROR frame; an unknown category, modified message, extra field, malformed JSON, or other payload closes the connection as a mismatched service response.
- Red evidence: `gateway_rejects_unsanitized_supervisor_error_payloads` failed before the change because the gateway wrote an 87-byte frame containing `secret principal detail` to the TCP client.
- Regression: the same test now verifies the client sees EOF and the gateway reports `MismatchedServiceResponse`. `gateway_bridge_returns_sanitized_supervisor_error_frames` verifies a canonical error still reaches the client unchanged.
- Verification: both focused gateway boundary tests passed. `cargo build --workspace --locked --offline`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`, and `cargo test --workspace --locked --offline -- --test-threads=1` passed in the pinned Nix shell. The full suite reported 3 cgroup-gated commit tests and 1 cgroup-gated metadata process test as ignored by default.
- Follow-up: the supervisor producer and gateway validator now share one typed `GatewayClientErrorCode` definition and canonical payload bytes, avoiding parallel JSON formatting and whitelist definitions. Unit coverage checks all five payloads and rejects missing fields or modified messages.
- Follow-up verification: canonical payload unit tests, the valid TCP relay regression, and the raw-payload rejection regression passed. Workspace build, formatting, warning-denied Clippy, and serial locked workspace tests passed; the serial suite reported 3 cgroup-gated commit tests and 1 cgroup-gated metadata process test as ignored.
- Schema follow-up: the five canonical payload strings now come from `query_abi_constants.json`; generation validates their exact v1 byte strings and emits both Rust constants and C string macros. Rust and TinyCC fixtures compare generated values to independent golden strings. The pre-generation TinyCC compile failed because the generated C macro was absent; the maintained suite passed after regeneration.
- Verification update: `gateway_rejects_error_frames_with_mismatched_request_ids` sends a canonical supervisor error with a different request ID and verifies that the gateway closes the TCP connection without forwarding the payload, reporting `MismatchedServiceResponse`.
- Verification update: `gateway_bridge_returns_sanitized_supervisor_error_frames` now relays and checks all five canonical v1 error categories over TCP, including request correlation and connection close after the error.
- Remaining: provision per-client Unix sockets/generation references, add queue/deadline and malformed-frame error coverage across separate processes, and complete worker lifecycle hardening.

## IPC-1 dependency: bounded Unix-stream frame v1

- Red evidence: the hand-checked frame fixture failed against the compiling `NotImplemented` scaffold.
- Added a 20-byte big-endian v1 header with magic, operation, nonzero request ID and payload length. Encoding/reading caps payloads at 1 MiB; reading rejects malformed/unsupported headers and oversized lengths before body allocation, distinguishes clean boundary EOF from truncation, and preserves following frames in a stream.
- Tests cover the byte fixture, split/coalesced Unix-stream writes, malformed/oversized headers, zero IDs, truncation and writer limits.
- Contracts: [IPC protocol](../../specs/ipc-protocol.md) and [IPC plan](../ipc-protocol/README.md).
- Verification: prescribed Nix-shell workspace build, formatting check, Clippy with warnings denied, and all 233 workspace integration tests passed.
- Remaining: operation payload schemas and allowlists, server-bound role dispatch, local service authentication, C parity, and gateway/supervisor/storage integration remain open.

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

## IPC-6 dependency: per-client gateway Unix service loop

- Behavior: one trusted gateway stream owns a client session through the entire upload/start sequence. It preserves request ordering, writes correlated responses, hands START's admission/immutable bytes back to supervisor code, and discards unstarted reservations or cancels started work on EOF and protocol/write failure.
- Changed code and test: [gateway Unix service loop and disconnect cleanup](../../engine/src/supervisor_ipc.rs), [authenticated owner helper](../../engine/src/supervisor.rs), and [real-redb UnixStream lifecycle](../../engine/tests/commit.rs).
- Verification: focused UnixStream test covers AUTH, PREPARE, artifact chunk, SEAL, STATUS, START, active CANCEL, EOF cleanup, and durable ABORTED outcome.
- Remaining: bind sockets to authenticated gateway process identity/generation and monitor network disconnects while workers execute; the TCP listener, launcher, sandbox, and result relay remain absent.

## SDK executor next-batch response bounds — implemented

- Contract: an executor response to `next_batch` must fit both request limits: visible row count cannot exceed `max_candidates`, and encoded response length cannot exceed `max_bytes`.
- Red evidence: the TinyCC executor fixture accepted a validly encoded two-row response to a one-row request and modified the caller's batch output. The new regression fails against the previous decoder.
- Implementation: `prdb_sdk_executor_next_batch_v1` now validates the response length and decoded row count against the request before copying the decoded batch to caller output; continuation validation also occurs before publication.
- Regression: C fixtures cover candidate overrun, byte overrun, unchanged output on each rejection, and the SQL v16 join runner failing closed on an executor page that exceeds its requested row count. Test mocks were corrected to request enough rows/bytes for their returned pages.
- Verification: focused executor, join, and grouped SQL fixtures passed; full TinyCC SDK suite and ABI generator checks (8 passed), locked workspace build, formatting, warning-denied Clippy, full workspace tests (138 passed; 4 delegated-cgroup tests ignored), and `git diff --check` passed in the pinned Nix shell.
- Remaining: broader malformed IPC response matrices and cross-process queue/deadline coverage remain open.
