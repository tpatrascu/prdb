# Toolchain Progress

## TC-1: locked release workspace build — verified

- Contract: the workspace builds in release mode using the repository's locked dependency set and pinned development toolchain.
- Verification: `XDG_CACHE_HOME=/tmp/prdb-nix-cache nix develop --offline --command cargo build --workspace --release --locked` passed; Cargo finished the optimized release build in 45.55 seconds.
- Limitation: this validates Rust release compilation only. Release artifact packaging, supported Linux/libc profile selection, TinyCC shared-library compatibility, and end-to-end confined loading remain open plan items.

## TC-2: generated executor close-cursor request layout — implemented

- Contract: close-cursor requests carry payload version and the connection-scoped cursor handle in the exact open-scan request shape.
- Implementation: a dedicated shared schema layout now drives the Rust and TinyCC request codecs instead of reusing the open-scan response layout.
- Regression: the executor socket-loop verifies the close acknowledgement; TinyCC executor fixture validates cursor close success and failures.
- Verification: focused executor socket-loop regression, TinyCC SDK suite, generator drift, locked workspace build/format/warning-denied Clippy/serial tests pass. Commit tests report 129 passed; three cgroup-dependent tests remain ignored.
- Remaining: generate layouts/codecs for remaining supervisor event payloads and other internal IPC payloads.

## TC-2: generated executor cancellation-check response layout — implemented

- Contract: cancellation checks return exactly payload version plus a boolean cancelled byte.
- Implementation: Rust encoding and TinyCC SDK response validation use generated header sizes and offsets; the maintained C fixture also uses the generated fields.
- Regression: Rust exact-byte tests cover both boolean values; the TinyCC fixture covers false, true, and invalid values.
- Verification: generator drift, focused regression, locked workspace build/format/warning-denied Clippy/serial tests, and full maintained TinyCC SDK suite pass. Commit tests report 129 passed; three cgroup-dependent tests remain ignored.
- Remaining: generate layouts/codecs for remaining supervisor event payloads and other internal IPC payloads.

## TC-2: generated supervisor terminal decision request layouts — implemented

- Contract: finalize and abort requests contain exactly the supervisor payload version and reject unsupported versions or trailing bytes.
- Implementation: shared layouts now describe each request and dispatch validates through their generated header sizes and version offsets.
- Regression: valid finalize/abort bytes and truncated, unsupported-version, and trailing-byte payloads are covered.
- Verification: malformed/valid request tests, generator drift, locked workspace build/format/warning-denied Clippy/serial tests, and full maintained TinyCC SDK suite pass. Commit tests report 129 passed; three cgroup-dependent tests remain ignored.
- Remaining: generate layouts/codecs for remaining supervisor event payloads and other internal IPC payloads.

## TC-2: generated supervisor terminal decision response layouts — implemented

- Contract: finalize returns version, status 0, and commit sequence; abort returns version and status 0.
- Implementation: schema layouts and generated-offset encoders cover both acknowledgements; independent exact-byte tests are added.
- Verification: exact-byte fixtures, generator drift, locked workspace build/format/warning-denied Clippy/serial tests, and full maintained TinyCC SDK suite pass. Commit tests report 129 passed; three cgroup-dependent tests remain ignored.
- Remaining: generate layouts/codecs for remaining supervisor event payloads and other internal IPC payloads.

## TC-2: generated supervisor status response variants — implemented

- Contract: supervisor status returns unknown, active/committing with optional deadline, or terminal status with observation time, commit sequence, and retention deadline.
- Implementation: the shared schema declares all four fixed response shapes. The Rust supervisor encoder allocates the matching generated header and writes values at generated offsets.
- Regression: exact-byte coverage checks unknown, active without deadline, committing with deadline, and committed terminal forms.
- Verification: generator drift, focused exact-byte regression, locked workspace build/format/warning-denied Clippy/serial tests, and full maintained TinyCC SDK suite pass. Commit tests report 129 passed; three cgroup-dependent tests remain ignored.
- Remaining: generate layouts/codecs for remaining supervisor result and event payloads.

## TC-2: generated supervisor cancellation result variants — implemented

- Contract: cancellation returns a short accepted/too-late/unknown status or a terminal result with outcome status, commit sequence, and retention deadline.
- Implementation: the shared schema declares simple and terminal response layouts. The supervisor encoder writes both variants using generated header sizes and field offsets.
- Regression: client status/cancel and authenticated multi-process ADMIN lifecycle tests pass for the result path.
- Verification: generator drift, focused IPC/process regressions, workspace build/format/warning-denied Clippy/serial tests, and TinyCC SDK suite passed.
- Remaining: generate layouts/codecs for remaining supervisor result and event payloads.

## TC-2: generated supervisor control request layouts — implemented

- Contract: invocation status/cancel payloads bind authority to the provisioned channel; status/client cancel carry version plus invocation ID, while administrative cancel also carries the reason code.
- Implementation: the shared schema declares status/client control and administrative cancellation request fields. Supervisor Rust dispatchers validate lengths, versions, invocation IDs, and reason codes using generated header offsets/sizes.
- Regression: metadata/admin process integration, exact client status/cancel non-enumeration, and TCP cancellation-during-commit regressions pass.
- Verification: generator drift, focused control tests, full workspace build/format/warning-denied Clippy/serial tests, and complete TinyCC SDK suite passed.
- Remaining: generate layouts/codecs for remaining supervisor result and event payloads.

## TC-2: generated optimizer SELECT request prefix — implemented

- Contract: the optimizer's bounded structured SELECT request starts with version, statement tag, and zero flags before its variable relation/projection/predicate fields.
- Implementation: the shared schema now owns the optimizer request version and SELECT tag constants plus prefix field offsets. The Rust decoder consumes the generated prefix and starts bounded parsing after the generated header size. The generator supports u8 registry constants.
- Regression: all four optimizer request parser tests pass, including unsupported version/tag, invalid flags, malformed names, and size/depth/collection bounds.
- Verification: generator drift, focused optimizer request tests, full workspace build/format/warning-denied Clippy/serial tests, and TinyCC SDK suite passed.
- Remaining: generate layouts and collection codecs for remaining executor/planner/supervisor payloads.

## TC-2: generated optimizer response variants — implemented

- Contract: optimizer responses use a version/status failure header, or a success header with u32 plan length followed by validated plan bytes.
- Implementation: the shared schema declares both variants and the raw byte tail. The Rust optimizer response codec consumes generated offsets and header sizes for both paths.
- Regression: optimizer response tests pass for validated plans, statuses, malformed lengths, malformed status values, and invalid plans.
- Verification: generator drift, focused optimizer codec tests, workspace build/format/warning-denied Clippy/serial tests, and full TinyCC SDK suite passed; 3 cgroup-dependent commit tests remain ignored by default.
- Remaining: generate layouts/codecs for remaining executor/planner/supervisor payloads.

## TC-2: generated stable plan-relation response prefixes — implemented

- Contract: optimizer metadata receives either version/status-only unavailability or a success prefix containing schema identity/version, table epoch, and relation identity before the variable relation description.
- Implementation: the shared schema declares both response prefixes. The Rust planner codec uses generated offsets and header sizes for encoding and decoding; the bounded relation-name and column codec remains explicit.
- Regression: planner estimate-session and concurrent planner/executor drain integration regressions pass through available and unavailable response handling.
- Verification: generator drift, focused planner integration tests, workspace build/format/warning-denied Clippy/serial tests, and full TinyCC SDK suite passed; 3 cgroup-dependent commit tests remain ignored by default.
- Remaining: generate layouts and codecs for remaining executor/planner/supervisor payloads.

## TC-2: generated planner relation-description response prefixes — implemented

- Contract: planner relation description returns either a version/status-only unavailable response or a success prefix with relation ID and planning handle before the variable name and column records.
- Implementation: the shared schema declares both response prefixes. Rust encoding/decoding and the TinyCC planner decoder consume their generated version/status/identity offsets and header sizes; the variable name and column records retain their bounded codec.
- Regression: `planner_stream_estimate_allowance_falls_back_without_aborting_invocation` and `supervisor_drains_both_worker_channels_before_durable_finalize` pass; the maintained TinyCC planner fixture passes.
- Verification: schema drift and negative schema tests, workspace build/format/warning-denied Clippy/serial tests, and full TinyCC SDK suite passed. Three cgroup-dependent commit tests remain ignored by default.
- Remaining: generate layouts and codecs for remaining planner/executor/supervisor payloads.

## TC-2: maintained payload schema validation tests — implemented

- Behavior: the SDK test target checks that the shared payload schema renders and rejects invalid version/count field widths.
- Implementation: `engine/tools/test_generate_query_abi.py` provides three standard-library unit tests; `abi-constants-test` is part of the maintained TinyCC `test` target.
- Regression: the focused test script and the Make target both pass all three cases.
- Verification: full locked Rust workspace checks and maintained TinyCC SDK suite pass with the new target enabled.
- Remaining: complete serializer generation and cover remaining IPC payloads.

## TC-2: validate payload version and collection count fields — implemented

- Contract: every payload version is a u16 field first in the payload and references a u16 version constant; count-delimited fixed-width collections use a u16 count.
- Implementation: the schema generator now rejects mismatched version declarations and collection count widths before emitting C/Rust constants.
- Regression: negative schema mutations with a u32 version field and a u32 collection count are both rejected; current generated files pass `--check`.
- Verification: locked workspace build/format/warning-denied Clippy/serial tests and TinyCC SDK suite passed using a temporary writable Nix cache after the default cache database returned read-only errors.
- Remaining: generate layouts and collection codecs for remaining executor/planner/supervisor payloads.

## TC-2: generated planner estimate collection declarations — implemented

- Contract: estimate requests carry a count-delimited array of 16-byte column IDs; exact estimate responses carry a count-delimited array of three 64-bit population values per column.
- Implementation: the shared payload schema now declares both trailing arrays. The generator accepts the population triple array encoding alongside its existing fixed-width ID array encoding.
- Regression: generator drift check and the planner estimate-session regression pass.
- Verification: generator `--check`, full workspace build/format/warning-denied Clippy/serial tests, and TinyCC SDK suite passed; commit integration tests reported 129 passed and 3 cgroup-dependent tests ignored.
- Remaining: generated collection codecs and layouts for remaining executor/planner/supervisor payloads.

## TC-2: generated executor next-batch response variants — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) encodes next-batch responses with distinct finished and continuation headers; continuation includes a cursor before the row count.
- Implementation: the shared schema declares both fixed header variants. The Rust response codec chooses generated offsets when encoding and decoding each variant; row/cell records keep their existing bounded codec.
- Regression: `executor_relation_socket_loop_ends_session_without_finalizing` passes through both response paths.
- Verification: generator drift, exact focused integration test, full workspace build/format/warning-denied Clippy/serial tests, and the full TinyCC SDK suite passed. Commit integration results: 129 passed, 3 cgroup-dependent tests ignored.
- Remaining: generate layouts for remaining executor/planner/supervisor payloads and serialization helpers.

## TC-2: generated planner estimate-scan response header — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) defines exact estimate responses as planner payload version/status, visible-row count, column count, then bounded per-column population triples; UNKNOWN responses end after status.
- Implementation: the shared schema now supports 64-bit fixed fields and declares the exact-response header. Rust response encoding/decoding uses generated offsets and header size while preserving the compact UNKNOWN form and explicit population-array codec.
- Regression: the planner estimate-session integration exercises exact and UNKNOWN response decoding.
- Verification: generator drift, focused planner regression, full workspace build/format/warning-denied Clippy/serial tests, and the full TinyCC SDK suite passed. Commit integration results: 129 passed, 3 cgroup-dependent tests ignored.
- Remaining: generate layouts for remaining executor/planner/supervisor payloads and serialization helpers.

## TC-2: generated mutation acknowledgement validation — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) uses the same version/status acknowledgement for cursor close, row update, and row delete.
- Implementation: the TinyCC executor SDK validates update/delete and close-cursor acknowledgements through generated field offsets and header size. Rust close-cursor, update, and delete dispatch now all use `MutationAcknowledgement`.
- Regression: the maintained TinyCC executor fixture checks successful and rejected acknowledgements; the executor socket-loop test checks the exact close-cursor response bytes.
- Verification: full workspace build/format/warning-denied Clippy/serial tests pass (129 commit tests passed; 3 cgroup-dependent tests ignored). The TinyCC SDK suite passed with the earlier C-codec conversion.
- Remaining: generate layouts for remaining executor/planner/supervisor payloads and serialization helpers.

## TC-2: generated planner estimate-scan request header — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) defines estimate-scan as planner payload version, connection-local planning handle, column count, and a fixed-width column-ID list.
- Implementation: the shared schema declares the fixed header fields. Rust planner request encoding and decoding consume generated offsets and header size; the trailing ID array retains its explicit bounded codec.
- Regression: the planner estimate-budget session regression passes through the request codec.
- Verification: generator drift, focused planner regression, full workspace build/format/warning-denied Clippy/serial tests, and the full TinyCC SDK suite passed. Commit integration results: 129 passed, 3 cgroup-dependent tests ignored.
- Remaining: generate layouts for remaining executor/planner/supervisor payloads and serialization helpers.

## TC-2: generated executor stage-insert request header — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) defines stage-insert as the batch payload version, relation handle, value count, and bounded value records.
- Implementation: the shared schema declares the fixed header fields. Rust and TinyCC encoders/decoders consume generated offsets and header size; variable value records retain their existing codec.
- Regression: Rust mutation codec round-trip and maintained TinyCC executor fixture verify stage-insert request bytes.
- Verification: generator drift, focused Rust mutation tests, full TinyCC SDK suite, and workspace build/format/warning-denied Clippy/serial tests (129 commit tests passed; 3 cgroup-dependent tests ignored).
- Remaining: generate layouts for remaining executor/planner/supervisor payloads and serialization helpers.

## TC-2: generated executor stage-update request header — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) defines stage-update as the batch payload version, row handle, patch count, and bounded patch records.
- Implementation: the shared schema declares the fixed header fields. Rust and TinyCC encoders/decoders use generated offsets and header size; patch records remain explicitly encoded by their existing codecs.
- Regression: Rust mutation codec round-trip and maintained TinyCC executor fixture verify update request bytes.
- Verification: generator drift, focused Rust mutation tests, full TinyCC SDK suite, and workspace build/format/warning-denied Clippy/serial tests (129 commit tests passed; 3 cgroup-dependent tests ignored).
- Remaining: generate layouts for remaining executor/planner/supervisor payloads and serialization helpers.

## TC-2: generated executor stage-insert response schema — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) defines the stage-insert success response as the batch payload version followed by an opaque row handle.
- Implementation: the shared schema declares the fixed response layout. Rust and TinyCC response codecs use generated offsets and header size.
- Regression: the Rust mutation codec round-trip and maintained TinyCC executor fixture check the response shape and row-handle bytes.
- Verification: generator drift, focused Rust mutation tests, workspace build/format/warning-denied Clippy/serial tests (129 commit tests passed; 3 cgroup-dependent tests ignored), and the full TinyCC SDK suite passed.
- Remaining: generate layouts for remaining executor/planner/supervisor payloads and serialization helpers.

## TC-2: generated executor stage-delete request schema — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) defines stage-delete as the batch payload version followed by an opaque row handle.
- Implementation: the shared schema declares the fixed-width request layout. Rust and TinyCC request encoders use generated offsets and header size.
- Regression: the Rust mutation codec round-trip and maintained TinyCC executor fixture check the request bytes against independent expected values.
- Verification: generator drift, workspace build/format/warning-denied Clippy, serial Rust tests (129 commit tests passed; 3 cgroup-dependent tests ignored), and the full TinyCC SDK suite passed.
- Remaining: generate layouts for remaining executor/planner/supervisor payloads and serialization helpers.

## TC-2: generated executor next-batch request schema — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) defines the next-batch request as version, opaque cursor handle, scan limit, and byte limit.
- Implementation: the shared schema now describes the fixed-width request fields, including the 32-bit byte limit. Rust and TinyCC encoders use generated offsets and header size.
- Regression: the Rust relation socket-loop regression and maintained TinyCC executor fixture exercise the request encoding.
- Verification: generator drift, workspace build/format/warning-denied Clippy, serial Rust tests (129 commit tests passed; 3 cgroup-dependent tests ignored), and the full TinyCC SDK suite passed.
- Remaining: generate layouts for remaining executor/planner/supervisor payloads and serialization helpers.

## TC-2: generated executor close-cursor payload schema — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) defines close-cursor as version plus opaque cursor handle and a version/status acknowledgement.
- Implementation: the shared payload schema now supports one-byte status fields and declares the mutation acknowledgement. The Rust close-cursor request reuses the generated open-scan cursor-handle layout, and Rust/C acknowledgement codecs consume generated offsets and sizes.
- Regression: the maintained executor IPC round-trip and TinyCC executor fixture verify close request and acknowledgement bytes.
- Verification: generator drift, workspace build/format/warning-denied Clippy, serial Rust tests (129 commit tests passed; 3 cgroup-dependent tests ignored), and the full TinyCC SDK suite passed.
- Remaining: add schemas for remaining executor/planner/supervisor payloads and generated serialization helpers.

## TC-2: generated executor open-scan request/response schemas — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) defines the open-scan request as version, relation handle, column count, and optional column-ID list; its response is version plus cursor handle.
- Implementation: the shared schema now models 16-byte fields and length-counted arrays in addition to u16 fields and UTF-8 tails. The generator emits Rust/C offsets and header lengths. Rust request/response codecs and TinyCC SDK request/response checks consume those generated constants.
- Regression: `executor_relation_socket_loop_ends_session_without_finalizing` exercises the Rust request path; the maintained `sdk_executor_v1.c` fixture checks exact request and response bytes.
- Verification: focused Rust/C regressions, generator drift, full workspace build/format/Clippy/serial tests (129 commit tests passed; 3 cgroup-dependent tests ignored), and the complete maintained TinyCC SDK suite passed.
- Remaining: field-level generation still covers only qualified relation resolution and open-scan payloads. Other IPC messages and serializer generation remain open.

## TC-2: generated executor relation request field schema — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) and [toolchain](../../specs/toolchain.md). The variable-length executor relation-resolution request has a six-byte fixed header: big-endian version, schema-name length, and relation-name length, followed by the two UTF-8 names in that order.
- Implementation: `engine/abi/query_abi_constants.json` declares the fixed and length-delimited fields. The generator validates supported encodings, unique names, and length-field references, then emits matching Rust/C offsets and header size. Rust and TinyCC encoders use those generated values.
- Regression: the Rust and C executor fixtures retain hand-checked request bytes, independently checking the generated layout. Generator `--check` verifies outputs have no drift.
- Verification: generator drift check, locked workspace build, format, warning-denied Clippy, serial workspace tests (129 commit tests passed; 3 cgroup-dependent tests ignored), and the full maintained TinyCC SDK suite passed.
- Follow-up: the C planner `describe_relation` helper shares the same generated request field offsets and version instead of maintaining its own literals. Its TinyCC fixture compares the emitted request against an independent exact-byte example.
- Follow-up verification: the focused `sdk-planner-v1` TinyCC fixture passes after rebuilding the SDK archive. The Rust request codec is shared with the executor and its exact-byte transaction regression passes.
- Remaining: extend the schema and generation to other IPC request/response payloads and codecs. This increment does not generate serializers.

## TC-2: generated IPC payload version registry — implemented

- Contract: [Toolchain](../../specs/toolchain.md). The shared interface schema now owns the executor relation/batch, planner, supervisor, and optimizer response payload-version constants.
- Implementation: the existing generator emits matching `u16` values for Rust and C. The Rust executor, planner, supervisor, and optimizer codecs import the generated constants rather than defining local version values. Payload fields and serializers remain in their Rust modules; this increment does not claim full IPC schema generation.
- Verification: `make -f engine/sdk/Makefile abi-constants-check`, locked workspace build, format check, warning-denied Clippy, serial workspace tests, and `make -f engine/sdk/Makefile test` passed in the locked Nix development shell. The commit integration suite reported 129 passed and 3 cgroup-dependent tests ignored.
- Remaining: define/generate IPC payload field schemas and serializers, complete C/Rust serialization parity, and validate the release Linux/libc/runtime profile and release artifacts. TC-2 remains partial.

## TC-2: generated fixed supervisor control codec — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) and [toolchain](../../specs/toolchain.md). The supervisor invocation-control request is exactly a big-endian v1 payload version followed by a 16-byte invocation ID; truncated, extended, and unsupported-version payloads are rejected.
- Red/green evidence: the byte-exact codec regression first failed to compile because generated encoder/decoder functions were absent. The generator now emits opt-in Rust codecs for fixed-width schema payloads, and both supervisor and gateway control dispatch use the generated decoder.
- Regression: the independent wire example checks exact encoded bytes, round-trip decoding, invalid version rejection, and truncation rejection. Generator tests assert the helper output and reject attempts to generate a fixed codec for trailing fields.
- Verification: `make -f engine/sdk/Makefile abi-constants-check abi-constants-test`, the focused codec test, full locked workspace build/format/warning-denied Clippy/serial Rust tests, and `make -f engine/sdk/Makefile test` all passed in the pinned Nix shell. Serial workspace tests included 132 commit integration tests; three cgroup-dependent commit tests and one cgroup-dependent metadata-process test remained ignored by default. Their explicit delegated-cgroup acceptances were previously run and recorded in the Linux sandbox and supervisor progress files.
- Remaining: extend field-level schema and generated codecs to other payloads, add C serializer parity, and complete the release profile and artifact gates.

## TC-2: C/Rust invocation-control codec parity — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) defines the fixed invocation-control payload shared by Rust supervisor code and C consumers. Both implementations must encode version 1 and the 16-byte invocation ID identically, and reject malformed lengths or versions on decode.
- Implementation: the shared schema can now opt fixed payloads into C codec generation for u8, u16, and bytes16 fields. The generated C header includes the generated constants header and uses the C-prefixed version constant. Rust fixed codecs also support signed and unsigned 64-bit fields.
- Red/green evidence: the maintained TinyCC fixture initially failed to compile because generated code referred to the Rust constant name instead of its C macro. Correcting the C name mapping made compilation succeed; the fixture now verifies an independent 18-byte wire example, round-trip data, unsupported version, and truncated length.
- Verification: generator drift/schema tests, locked workspace build, format check, warning-denied Clippy, serial Rust tests, and the complete maintained TinyCC SDK suite passed in the pinned Nix shell.
- Remaining: extend generated field codecs and cross-language parity to more payloads, especially variable-length collections, and complete release profile and artifact gates.

## TC-2: C/Rust invocation-control codec parity — verified

- The generated C and Rust codecs now consume the same fixed-width schema for the supervisor invocation-control request. The maintained TinyCC C fixture and Rust regression independently compare both implementations with the same 18-byte wire example, including exact decode, truncation rejection, and unsupported-version rejection.
- Verification: `make -f engine/sdk/Makefile abi-constants-check abi-constants-test`, locked workspace build, rustfmt check, warning-denied Clippy, serial workspace tests, and the complete maintained TinyCC suite passed. The SDK suite now compiles and runs `ipc-payload-codec-v1` with the pinned TinyCC build.
- Remaining: extend generated C codec parity to other fixed payloads and define safe bounded helpers for collections and variable-length payloads; complete release profile and artifact gates.

## TC-2: C/Rust simple cancellation-result codec parity — verified

- The generated C header now covers the fixed simple cancellation result as well, exercising a single u8 status field after the version. Its bytes match the Rust encoder and the existing internal IPC contract.
- Regression: the maintained TinyCC fixture checks the independent three-byte example and typed status round trip, alongside invocation-control and administrative-cancel request parity.
- Verification: generator drift/schema tests, locked workspace build, format, warning-denied Clippy, serial workspace tests, and the complete maintained TinyCC suite passed.
- Remaining: extend generated C codec parity to other fixed payloads and define safe bounded helpers for collections and variable-length payloads; complete release profile and artifact gates.

## TC-2: C/Rust terminal supervisor response codec parity — verified

- The shared schema now generates C codecs for terminal cancellation results and terminal STATUS responses. The C generator supports fixed u32/u64/i64 fields, including portable signed two's-complement decoding, and correctly copies 16-byte fields.
- Regression: the maintained TinyCC fixture checks both independent wire examples byte-for-byte and round-trips negative timestamps/deadlines, unsigned commit sequences, and status bytes. Generator tests assert both helper pairs are emitted.
- Verification: generated-file drift and schema tests, locked workspace build, format check, warning-denied Clippy, serial workspace tests (129 commit tests passed; 3 cgroup-dependent tests ignored by default), and the maintained TinyCC SDK suite passed in the pinned Nix shell.
- Remaining: extend generated C codec parity to other fixed payloads and define safe bounded helpers for collections and variable-length payloads; complete release profile and artifact gates.

## TC-2: C/Rust administrative-cancel codec parity — verified

- The C codec generator now also covers the administrative-cancel request with invocation ID and big-endian u16 reason code. Both the fixed 18-byte invocation-control request and 20-byte cancel request are emitted from the shared schema.
- Regression: the TinyCC fixture independently checks the administrative-cancel request bytes, decoded invocation ID, and decoded reason code. Rust has matching hand-checked tests for both payloads.
- Verification: generator drift/schema tests, locked workspace build, format, warning-denied Clippy, full serial workspace tests (129 commit tests passed; 3 cgroup-dependent cases ignored by default), and the maintained TinyCC suite passed.
- Remaining: extend generated C codec parity to other fixed payloads and define safe bounded helpers for collections and variable-length payloads; complete release profile and artifact gates.

## TC-2: generated terminal STATUS response codec — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) defines terminal STATUS as version, status, signed observed time, unsigned commit sequence, and signed retention deadline.
- Red/green evidence: the signed timestamp regression initially failed to compile because this response lacked generated helpers. The schema now opts in the fixed-width codec, and terminal status responses use its encoder.
- Regression: the independent wire example covers negative observation/retention times and a positive commit sequence, checking exact bytes and typed round-trip decoding.
- Verification: generator drift/schema tests, locked workspace build, format check, warning-denied Clippy, serial workspace tests (96 library tests), and the maintained TinyCC SDK suite passed in the pinned Nix shell.
- Remaining: extend field-level schema and generated codecs to other payloads, add C serializer parity, and complete the release profile and artifact gates.

## TC-2: generated terminal cancellation-result codec — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) specifies a terminal cancellation result as version, status, outcome status, `u64` commit sequence, and signed `i64` retention deadline. The shared schema had incorrectly declared the deadline `u64`; it now agrees with the wire contract and existing storage type without changing the field width or bytes.
- Red/green evidence: the signed-value regression initially failed to compile because the terminal result had no generated codec. The schema now opts into codec generation, and supervisor terminal cancellation responses use the generated encoder.
- Regression: the hand-checked example includes a negative retention value and verifies its two's-complement wire bytes plus signed round-trip decoding.
- Verification: generator drift/schema tests, locked workspace build, format check, warning-denied Clippy, serial workspace tests (95 library tests), and maintained TinyCC SDK tests passed in the pinned Nix shell.
- Remaining: extend field-level schema and generated codecs to other payloads, add C serializer parity, and complete the release profile and artifact gates.

## TC-2: generated simple cancellation-result codec — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) and [toolchain](../../specs/toolchain.md). A simple cancellation result is the v1 payload version plus one status byte. The generated codec validates exact length and version; role-specific code remains responsible for interpreting the status set.
- Red/green evidence: the codec regression initially failed to compile because the schema had no generated helper. The schema now opts in the simple result payload, and supervisor cancellation response encoding uses the generated function for accepted, too-late, and unknown results.
- Regression: an independent three-byte example checks encoding, decoding, and truncated-payload rejection. Generator render tests assert helper generation for all opted-in fixed payloads.
- Verification: generator drift/schema tests, locked workspace build, format check, warning-denied Clippy, serial workspace tests (including 94 library tests), and the full maintained TinyCC SDK suite passed in the pinned Nix shell.
- Remaining: extend field-level schema and generated codecs to other payloads, add C serializer parity, and complete the release profile and artifact gates.

## TC-2: generated administrative-cancel codec — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) and [toolchain](../../specs/toolchain.md). The administrative cancel request is the big-endian v1 payload version, a 16-byte invocation ID, and a big-endian u16 reason code; the decoder rejects invalid length or version.
- Red/green evidence: the new multi-field wire regression first failed to compile because this payload had no generated codec. The schema now opts it into the fixed-width Rust codec generator, and supervisor administrative-cancel dispatch uses the generated decoder.
- Regression: an independent 20-byte wire example checks encoding, tuple decoding, and truncated-payload rejection. Generator tests check both generated helper names and reject fixed-codec generation for trailing fields.
- Verification: generator drift and schema tests, locked workspace build, rustfmt check, warning-denied Clippy, serial workspace tests, and the maintained TinyCC SDK suite all passed in the pinned Nix shell.
- Remaining: extend field-level schema and generated codecs to other payloads, add C serializer parity, and complete the release profile and artifact gates.

## TC-1: locked Nix development shell — implemented

- Behavior: `nix develop --locked` provides the pinned x86_64-linux package set used for the engine and SDK checks. [`flake.nix`](../../flake.nix) and [`flake.lock`](../../flake.lock) pin Nixpkgs revision `718f670e845328e9c2cecdd832835712b2cbd065` with source NAR hash `sha256-Ee5BWo8sh3pGt70QUdRajnaginqFmWvrtdBc4gJzhks=`.
- Package versions: Rust/Cargo/Clippy 1.95.0, rustfmt 1.9.0, TinyCC `0.9.27-unstable-2025-01-06`, GCC 15.3.0, binutils 2.46, and GNU Make 4.4.1. `nix develop --offline --command` reproduced these versions from the lockfile.
- Verification: inside the locked shell, the maintained TinyCC SDK suite passed, `sandbox-query-fixture` built all three GNU-linked artifacts, and workspace build, format, warning-denied Clippy, and serial tests passed. The four cgroup-gated acceptances that the default suite reports as ignored were also run individually and passed with the pinned runtime profile and a temporary delegated cgroup parent; see [Linux sandbox progress](../linux-sandbox/PROGRESS.md).
- Changed contract: [`specs/toolchain.md`](../../specs/toolchain.md) records the development profile; [`AGENTS.md`](../../AGENTS.md) points checks to the locked shell and maintained SDK targets.
- TinyCC provenance: the pinned recipe records upstream commit `f6385c05308f715bdd2c06336801193a21d69b50`, source hash `sha256-tO3N+NplYy8QUOC2N3x0CO5Ui75j9bQzLSZQF1HQyhY=`, and Nix NAR hashes for the compiler, runtime-support library, and development/header outputs. These are recorded in [`specs/toolchain.md`](../../specs/toolchain.md).
- Remaining: validate the release Linux/libc/runtime profile, extend schema generation to native fields/serializers, record release build commands and panic profiles, and package signed/checkable release artifacts. The locked shell is a development profile, not release artifact packaging.

## TC-2: generated query ABI constants — partial

- Behavior: [`query_abi_constants.json`](../../engine/abi/query_abi_constants.json) is the common source for query ABI versions, symbol names, callback failure code, parameter/result bounds, IPC operation tags, IPC frame magic/version/header size/field offsets, payload cap, malformed-frame status values, the canonical gateway error payload strings, and v1-v3 native record declarations, sizes, alignments, and field offsets. [`generate_query_abi.py`](../../engine/tools/generate_query_abi.py) emits C record headers, Rust records, constants, layout assertions, and C operation-tag cases. Rust and C frame codecs consume the generated layout and operation tags. `make -f engine/sdk/Makefile abi-constants` regenerates them; the maintained SDK `test` target checks drift.
- Compatibility: The v3 invocation remains an alias of the v2 record. Generated assertions fail C and Rust builds if native layouts diverge from the schema; descriptor tests also reject unsupported versions/sizes.
- Verification: generated-file drift check and the full maintained TinyCC SDK suite passed, including the TinyCC-built C frame codec fixture with exact-byte, coalesced-frame, cap, malformed-input, no-partial-output, generated frame error-status, and canonical gateway error-payload checks. Rust tests check the same generated payload strings and frame status values. Workspace build, formatting, warning-denied Clippy, and serial workspace tests passed; the serial suite reported 3 cgroup-gated commit tests and 1 cgroup-gated metadata process test as ignored. The four ignored-by-default acceptance tests were separately run and passed; see [Linux sandbox progress](../linux-sandbox/PROGRESS.md).
- Remaining: move the remaining IPC payload schemas and serializers into the shared interface schema. TC-2 remains partial.

## TC-1 / TC-3: maintained TinyCC SDK and shared-artifact link profile — partial

- Observed development tools: Rust/Cargo 1.95.0 with rustfmt and Clippy; TinyCC `0.9.27-unstable-2025-01-06` on x86_64 Linux. The SDK archive and C fixtures build with TinyCC `-Wall -Wextra -Werror`.
- Red evidence: `tcc -shared` linked the archive, but its output omitted `PT_GNU_STACK`; the confined executor's glibc loader rejected it with `cannot enable executable stack as shared object requires`.
- Implementation: maintained query C objects and the archive remain TinyCC-built. The fixture link step now names `SHARED_LINKER` explicitly and applies GNU linker's `-z noexecstack` option. The Makefile documents why the linker differs from TinyCC.
- Verification: `make -f engine/sdk/Makefile test`, `make -f engine/sdk/Makefile sandbox-query-fixture`, and the DML/SQL and restricted-metadata artifacts linked with the maintained GNU linker passed their confined acceptance tests. `readelf` confirms `GNU_STACK` is RW, with no execute permission. The direct TinyCC-linked shared artifact was rejected at `dlopen` in the sandbox; it was not loaded in a trusted process.
- Remaining: define the release Linux/libc profile and measure clean SDK builds and per-query compile/link separately. The locked Nix shell makes development checks reproducible but is not the release artifact package. TC-2 tracks generated-interface parity separately.
