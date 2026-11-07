# Client Network Protocol v1: Plain TCP

## Scope

This is the initial external client protocol, terminated by the [gateway](client-gateway.md). It is not HTTP, HTTP/3, or executor IPC. It supports authenticated submissions, streamed arbitrary query output, status/cancellation, and a separate administrative connection mode. It is unencrypted; loopback is the default. Keep protected bootstrap on its local setup channel.

The client submission SDK implements this protocol outside query.so. The C SQL/runtime inside query.so uses only provisioned local host services. Neither TCP sockets nor client credentials enter the executor.

## Frame Encoding

TCP is treated as a byte stream: reads may contain part of a frame or several frames. Each frame has this 32-byte header, integers unsigned little-endian:

| Offset | Bytes | Field |
| --- | --- | --- |
| 0 | 4 | ASCII `PDBN` |
| 4 | 2 | Protocol major=1 |
| 6 | 2 | Protocol minor=0 |
| 8 | 2 | Message kind |
| 10 | 2 | Flags=0 |
| 12 | 4 | Payload byte length, excluding header |
| 16 | 8 | Request ID |
| 24 | 8 | Response sequence |

Hard payload maximum is 1 MiB; JSON control payloads are limited to 64 KiB and chunk data to 256 KiB. Negotiation may lower limits, never raise these maxima. Validate header/version/length before allocation. No compression, optional frame flags, or generic descriptor transport exists in v1. Invalid framing closes the connection after a bounded error if possible; do not scan arbitrary bytes for a resynchronization magic.

Clients allocate strictly increasing nonzero request IDs per connection and set response sequence=0. Responses echo the ID. All ordinary requests have one response at sequence 0. START produces a stream described below. No request pipelining/multiplexing on one connection initially; different clients/connections execute concurrently. Status and cancellation during a streamed invocation use another authenticated connection.

Control payloads are UTF-8 JSON objects, with duplicate/unknown keys rejected, nesting depth at most 16, and no nonfinite numbers. Encode u64 counters/offsets as canonical decimal strings to avoid JSON numeric precision loss. Opaque IDs are 32 lowercase hexadecimal characters; hashes are 64. Binary credential/scope/manifest fields use canonical padded base64 where specified. These representation rules are independent of on-disk formats.

## Messages and Connection Modes

| Kind | Direction | Operation |
| --- | --- | --- |
| 1 | Client | HELLO |
| 2 | Client | AUTH |
| 3 | Client | PREPARE |
| 4 | Client | ARTIFACT_CHUNK |
| 5 | Client | PARAMETER_CHUNK |
| 6 | Client | SEAL |
| 7 | Client | START |
| 8 | Client | STATUS |
| 9 | Client | CANCEL |
| 10 | Client | ADMIN |
| 11 | Client | PING |
| 100 | Server | RESPONSE, JSON operation-specific result |
| 101 | Server | ERROR, JSON bounded code/message |
| 102 | Server | RESULT_DATA, raw application bytes |
| 103 | Server | TERMINAL, JSON invocation outcome |

HELLO is the first request: `{ "mode": "query" | "control", "client_name": string }`, with client_name at most 128 UTF-8 bytes. RESPONSE reports selected version, mode, limit values, and `transport: "plaintext_tcp"`. Only version 1.0 is supported; reject unsupported versions, never silently negotiate weaker security. Before authentication allow only HELLO, AUTH, and PING under strict time/size/rate bounds.

AUTH supplies `{ "token_id": id, "token_secret": base64 }`; decoded secret is bounded to 4 KiB and its actual algorithm/length is checked by the separate credential profile. RESPONSE contains a nonsecret session ID and `authenticated: true`. The session ID is server-generated and binds supervisor staging to this connection and the principal resolved from storage. Invalid credentials use one generic failure category and disclose no principal/token existence. No user-supplied principal field is accepted. The gateway does not log or reflect credential bytes. A connection has one identity and mode; switching requires a new connection.

The gateway session state accepts HELLO as the first request, then authenticates through storage and applies the request allowlist for the selected mode. Before HELLO no other request is permitted; after HELLO but before AUTH only AUTH and PING are permitted. Query sessions may use PREPARE, chunks, SEAL, START, STATUS, CANCEL, and PING. Control sessions may use STATUS, CANCEL, ADMIN, and PING. A query staging owner is derived from the storage-authenticated session, not request fields. The gateway retains the bearer secret only for the connection lifetime, zeroizes its in-memory copy on disconnect, and asks storage to revalidate token, login, and current role closure before each execution or control request. PING does not require revalidation.

The initial control-payload decoder implements the exact flat string-object shapes for HELLO and AUTH. It rejects duplicate keys (including escaped-equivalent names), unknown or non-string fields, malformed JSON escapes, trailing bytes, noncanonical token IDs, and noncanonical or wrong-sized base64 secrets. Other request payload schemas remain unimplemented until their allowlisted operation increments.

Query mode supports PREPARE through START, STATUS, CANCEL, and PING. Control mode supports STATUS, CANCEL, ADMIN, and PING. Mode selection never grants administrative privileges. Every new execution/admin/status/cancel operation revalidates credential/current authority; an initially valid AUTH does not freeze permissions for the connection lifetime.

## Submission and Upload

PREPARE contains `manifest_version` (u32 JSON integer), `manifest` (base64), `artifact_bytes` (u64 string), `artifact_sha256`, `parameter_bytes` (u64 string), `parameter_sha256`, `scope_version` (u32), `requested_scope` (base64), `deadline_ms` (u64 string), and `workload_class` (`operational` or `analytical`). Manifest and requested-scope decoders are separately versioned SDK/admission contracts; reject unimplemented versions before accepting upload. Raw manifest is at most 16 KiB and scope at most 8 KiB, within the control-frame bound. Parameters are opaque application bytes, not admission credentials.

Baseline upper bounds: artifact 64 MiB, parameters 1 MiB, one live submission per query connection. Configured quotas may be lower. PREPARE allocates an authenticated supervisor-owned staging reservation and returns `invocation_id` and admitted upload limits. It creates no MVCC snapshot and launches no executor. The ID is known to the client before execution can have effects.

Chunk payloads are binary: invocation ID[16], offset u64 little-endian, then 1–256 KiB data bytes. ARTIFACT_CHUNK and PARAMETER_CHUNK track independent offsets. Require the exact next contiguous offset; reject duplicates, gaps, overflow, or bytes beyond the declaration. Acknowledgement reports the next offset. Zero-length parameters require no chunks and use the SHA-256 digest of empty bytes. Authenticate and bound staging globally and per principal; declared lengths do not authorize unlimited allocation.

SEAL `{ "invocation_id": id }` verifies both byte counts/digests and manifest compatibility, then makes staged bytes immutable. RESPONSE reports `state: "prepared"`. Hash agreement does not establish artifact safety. No untrusted library is loaded during validation.

START `{ "invocation_id": id }` is permitted once for that sealed reservation, on its owning query connection. The supervisor revalidates admission and atomically transitions it to execution, then returns RESPONSE sequence 0 with `state: "accepted"`. Execution may start before that response is received. Repeated START is rejected, not replayed; this live-reservation guard is not durable submission deduplication. A rejected pre-execution start returns ERROR, releases any temporary admission snapshot, and creates no durable outcome or other transaction effects.

Disconnect before START discards staging. After any START transmission with an uncertain reply, use STATUS; never automatically repeat START or submit a fresh invocation. Reservations are not durable across supervisor restart, and missing status cannot establish safe retry. Upload-resume and detached execution are outside v1.

## Results and Transaction Outcomes

After accepted START, RESULT_DATA frames use the START request ID and consecutive sequences starting at 1; TERMINAL takes the next sequence and ends the stream. Each data payload is untrusted application output chosen by the query library. Output media/schema conventions may be defined by the application; they are not raw storage batch structures. The transport does not claim arbitrary output is a truthful SQL result.

Only the supervisor can supply the invocation's terminal envelope. TERMINAL fields are `invocation_id`, `outcome` (`committed`, `aborted`, or `unknown`), and `reason_code` (bounded public category). Unknown requires subsequent reconciliation. `committed` requires storage's durable decision; `aborted` requires a definitive abort decision. Neither application bytes containing those words, TCP EOF, nor upload/START acknowledgements establish an outcome. Durable ABORTED status follows the WAL/outcome contract.

Every RESULT_DATA frame is provisional until COMMITTED. An aborted invocation invalidates all provisional data as successful transaction output. On EOF, timeout, or incomplete framing, retain the ID and reconcile through STATUS. A committed invocation may have incomplete/lost results; v1 has no retained result replay. After a terminal frame the connection may prepare a new invocation with a new ID and increasing request IDs.

## Control Operations and Administration

STATUS `{ "invocation_id": id }` returns an authorized state: `prepared`, `active`, `committing`, `committed`, `aborted`, `unknown`, or `expired`. Prepared is ephemeral and means no execution has started. Query mode is owner-scoped; control mode revalidates current database-administrator authority before returning another owner's live or retained terminal state. Missing and inaccessible IDs use the same non-disclosing response category. Committed/aborted recovery status comes from storage, not gateway memory.

CANCEL `{ "invocation_id": id }` returns `accepted`, `too_late`, or an authorized terminal state. Cancellation accepted before COMMIT submission prevents commit; after submission, report too_late/pending resolution. Cancelling prepared staging removes it without starting a transaction. Status/cancellation acknowledgements are not synthetic transaction terminal events.

ADMIN is a control-mode operation with one of these exact envelopes:

- Read-only: `{ "phase": "read", "operation_version": u32, "operation": string, "arguments": object }`.
- Prepare mutation: `{ "phase": "prepare", "operation_version": u32, "operation": string, "arguments": object }`; response contains `operation_id` and `state: "prepared"`. No mutation has run.
- Start mutation: `{ "phase": "start", "operation_id": id }`; this may execute only once on the owning connection, after current-authority revalidation. Response contains the ID, outcome (`committed`, `aborted`, or `unknown`), and operation-specific result if justified by that outcome. It is a single bounded response, not an executor result stream.

STATUS/CANCEL use their existing `invocation_id` field for either an invocation ID or an administrative operation ID; IDs share one opaque namespace. Reserve one prepared operation per control connection. Lost replies follow the same authenticated durable-outcome reconciliation rule; never replay an uncertain administrative mutation. Disconnect discards unstarted reservations and requests cancellation of started work subject to the commit barrier.

The operation allowlist covers schema/identity/permission DDL submission, credential lifecycle, and authorized metadata/runtime inspection under their existing specs. Each named operation requires an explicit bounded argument/result schema and authority check; unknown or not-yet-specified operations fail unsupported, not generic dispatch into internal IPC. The server classifies reads versus mutations and rejects attempts to run a mutation with phase=read. DDL uses one statement or bounded atomic batch. Credential issuance returns its secret only once in its operation response; lost delivery requires an explicit authorized revoke/reissue flow, not recovery of plaintext from status. Bootstrap retains its separate local handoff protocol.

PING has `{}` payload and returns `{ "alive": true }`; it is not a health/durability or authorization proof. ERROR has `code` and a sanitized message of at most 256 UTF-8 bytes. Initial code categories: invalid_frame, unsupported_version, unauthenticated, unavailable, invalid_state, invalid_request, resource_limit, cancelled, internal_error. Do not expose raw exceptions, SQL parameters, private names, paths, token details, or competing principals.

## Validation and Extension

Test partial/coalesced frames, oversized lengths, malformed JSON/base64, duplicate IDs/keys, incorrect sequences, cross-session submissions, revoked credentials between upload/start, slow uploads without snapshot pins, upload substitution, forged worker terminal bytes, reconnect/status, and gateway death around commit. Test multiple concurrent connections while retaining one stream per connection.

This framing can be implemented behind other transports, or another gateway may map the same admission/result operations to its own transport. Future TLS/HTTP/3 and multiplexing require separate explicit versions/capabilities and equivalent failure semantics. They must not change transaction identity, internal process roles, or the client's prohibition on automatic replay.
