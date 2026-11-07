# IPC Protocol

## Transport and Authority

This document specifies internal process IPC. External clients use the separate [network protocol](client-protocol.md) terminated by the [gateway](client-gateway.md); its service role translates an allowlist of admission/control operations and cannot tunnel arbitrary internal messages. Internal executor/planner connections start with copied, framed messages over a Unix socket. Each execution/planning connection is provisioned by the supervisor and bound server-side to one invocation context and role. Client admission is a separate authenticated control interface; it does not inherit an invocation role from a message field. The worker cannot authenticate as another principal by changing a message field.

Use connection-scoped handles with server-side ownership and lifetime checks. Handle possession or guessability alone must not bypass authorization. Do not pass database descriptors or accept arbitrary descriptor transfer.

## Frame Envelope v1

Unix-stream frames use a fixed 20-byte header followed by a copied payload. Header fields are big-endian and ordered as: four-byte magic `PRDI`, u16 frame version (`1`), u16 operation code, u64 nonzero request ID, and u32 payload length. Operation code zero is reserved and rejected. A payload is at most 1,048,576 bytes; reject larger lengths before allocating or reading the payload. The header has no client-controlled service-role, principal, invocation, handle, or descriptor fields. Channel provisioning binds the service role and execution context out of band. Operation payload schemas are independently versioned and validated by the selected channel handler.

Readers accept arbitrary stream fragmentation and leave following coalesced frames available for the next read. EOF is clean only between frames; EOF within a header or payload is a truncated-frame error. Unknown frame versions and malformed headers fail before payload allocation. Frame payloads contain encoded fields, never native struct memory.

### Version-1 Operation Tags and Service Roles

Operation tags are fixed independently of their payload schemas. The channel's service role is assigned during trusted connection provisioning and is never selected by a frame field. `Operation::from_code` rejects unknown tags; framing rejects them before payload allocation. A role/operation mismatch is denied before the selected handler runs. Database-principal authorization remains a separate StorageManager check.

| Role provisioned for the channel | Operation tags |
| --- | --- |
| Bootstrap | `0x0101` deliver setup credential; `0x0102` acknowledge delivery |
| Executor | `0x1101` resolve relation; `0x1102` open scan; `0x1103` next batch; `0x1104` close cursor; `0x1110` stage insert; `0x1111` stage update; `0x1112` stage delete |
| Planner | `0x1201` describe relation; `0x1202` estimate scan |
| Supervisor | `0x1301` finalize transaction; `0x1302` abort transaction; `0x1303` cancel invocation; `0x1304` get invocation status |
| Gateway service | `0x1401` authenticate session; `0x1402` close session; `0x1403` prepare submission; `0x1404` upload submission chunk; `0x1405` seal submission; `0x1406` start submission; `0x1407` get invocation status; `0x1408` cancel invocation; `0x1409` submit administrative operation; `0x140a` open client session |
| Administrative control | `0x1501` commit administrative DDL; `0x1502` issue credential; `0x1503` revoke credential; `0x1504` list credentials; `0x1505` cancel invocation |

The storage-side Supervisor handler accepts `0x1301` finalize and `0x1302` abort on a channel bound out of band to one active invocation. Both requests carry only `u16 version = 1`; finalize responds with version, status 0, and the committed sequence `u64`, while abort responds with version and status 0. The handler consumes the invocation on either decision, rejects further terminal requests, and aborts if the supervisor channel closes before a decision. Worker or planner EOF alone does not finalize. `0x1304` status requests carry version and invocation ID[16]; the connection's authenticated principal is bound out of band. The response starts with version and status: 0 UNKNOWN; 1 ACTIVE; 2 COMMITTING; 3 COMMITTED; 4 ABORTED. ACTIVE/COMMITTING append observed time `i64`, start time `i64`, deadline-present `u8`, optional deadline `i64`, and cancellation-requested `u8`. COMMITTED/ABORTED append observed time `i64`, commit sequence `u64`, and retention deadline `i64`. UNKNOWN covers absent or non-owned invocations. `0x1303` cancellation carries version, invocation ID[16], and nonzero reason code `u16`; the authenticated administrator is bound out of band and storage rechecks database-admin authority and persists the audit event. Its response status is 0 ACCEPTED, 1 TOO_LATE, 2 TERMINAL followed by outcome code (1 COMMITTED, 2 ABORTED), commit sequence `u64`, and retention deadline `i64`, or 3 UNKNOWN.

The administrative-control channel is bound to an already authenticated actor; its service role does not confer database privileges. Gateway operations go to the supervisor service and cannot be forwarded as arbitrary internal operation codes. The protected bootstrap socket accepts only its credential-delivery and acknowledgement operations. Executor and planner roles are distinct: only the planner receives estimate operations, and neither role can finalize or administer.

### Gateway Submission Service Payloads

Each gateway-service channel is provisioned for one client connection and one HELLO-selected mode. The mode and gateway generation are channel context; neither is supplied in request payloads. The gateway copies the client request ID into the IPC frame and permits only one outstanding client operation on the channel. The service owns the corresponding `ClientConnectionSession`, revalidates storage authority, and derives staging ownership from the authenticated session.

`0x140a` is the first operation on a new per-client service stream and carries the strict HELLO JSON payload; it establishes mode and connection-local limits but no identity. `0x1401`, `0x1403`, `0x1405`, `0x1407`, and `0x1408` carry the corresponding bounded client JSON payload without its TCP frame header. `0x1404` begins with one byte selecting client kind 4 (artifact) or 5 (parameters), followed by the unchanged binary chunk payload. `0x1406` carries the client START JSON payload. Responses preserve the operation and request ID and contain the client RESPONSE JSON payload.

START has an additional trusted in-process result: the supervisor-side dispatcher retains the admitted `SupervisorInvocation` and owner-checked immutable `SealedSubmission` handle for launch code while returning the sequence-zero `accepted` response. Those Rust objects are never serialized over gateway IPC. A failed START returns an error and does not expose staged bytes or a storage transaction. Result relay and terminal messages remain separate supervisor-generated events.

The server provisions one service stream and `ClientConnectionSession` per client connection. Request IDs must increase for that session. Clean EOF, malformed framing, a role/payload error, or a failed response write closes the context: unstarted staging is discarded, while started work is signalled for storage cancellation. After START, trusted supervisor code owns the invocation event; it may resume servicing that session only after execution/result handling returns control. The runner must separately monitor client disconnect while execution is active.

## Administrative Control

Authenticated clients submit bounded schema/user/permission DDL or structured equivalents on a dedicated control channel. Storage validates both definitions and current actor authority. Administrative operations may be atomically batched, but cannot be mixed with executor DML/native code. Request IDs, budgets, durable outcomes, and status lookup apply to DDL too.

Credential issuance/revocation uses protected control API fields, not SQL literals or executor messages. Separate database principals/roles from service roles assigned to executor, optimizer, and supervisor channels. Credentials never cross into the executor. See [schema DDL](ddl-schema.md), [identity DDL](ddl-identity.md), and [permission DDL](ddl-permissions.md).

## Discovery and Observability

Provide bounded `list_schemas`, `list_relations`, and `describe_columns` operations with snapshot-bound continuation tokens as defined in [metadata discovery](metadata-discovery.md). Built-in `information_schema` scans use the same visibility rules. Privileged object inspection stays on the control channel; secrets/verifiers never appear in responses.

Live status/monitoring APIs follow [operational metadata](operational-metadata.md) and are explicitly outside invocation MVCC snapshots. Own-invocation status is not authority to enumerate other users or global metrics. Durable outcome lookup may return UNKNOWN/EXPIRED; it must not invent an abort after a lost commit reply. Administrative cursors recheck current authority between pages.

## Relation Resolution and Handles

Structured SQL requests name relations. The optimizer resolves those names through authorized storage metadata, but must not embed its connection-local handles in returned plans. Plans carry logical relation/schema references, which the executor resolves independently on its storage connection. Resolution rechecks the invocation grant and pinned schema; a plan reference grants no authority.

The executor role supports authorized schema discovery and relation resolution for direct SDK access as well as plan execution. It does not receive physical access-path statistics or the optimizer's estimation role. Cursors and row handles remain connection-local. Snapshot/schema mismatch fails the request rather than silently rebinding a plan to another relation.

The storage engine provides an `ExecutorMetadataConnection` resolver session with opaque relation handles. An IPC handler must own one such session per provisioned executor connection, revalidate each handle through the invocation's pinned catalog before use, and drop it on connection close. These handles are references only; the scan and mutation operations still perform their own grant, policy, and snapshot checks. Optimizer-local handles are never accepted by executor handlers.

The initial Rust executor dispatcher implements `resolve_relation` (`0x1101`), `open_scan` (`0x1102`), `next_batch` (`0x1103`), and `close_cursor` (`0x1104`). Each payload schema uses version 1. The resolve request is `u16 version`, `u16 schema_name_bytes`, `u16 relation_name_bytes`, and the two nonempty UTF-8 names. Its response is `u16 version`, `u8 status`; status 0 is followed by a 16-byte relation ID, 16-byte relation handle, u16-length UTF-8 relation name, u16 column count, then column records. A column record is `column_id[16]`, zero-based ordinal `u32`, nullable `u8`, scalar type code `u8`, four type-parameter bytes, and a u16-length UTF-8 name. Type codes 1–11 are Boolean, SmallInt, Integer, BigInt, Decimal, Text, Varchar, Bytea, Uuid, Date, TimestampTz. Decimal parameters are precision/scale followed by two zero bytes; Varchar parameters are a big-endian u32 maximum character count; other parameters are zero. Status 1 means unavailable, covering hidden and absent names. Schema metadata is returned in catalog ordinal order.

The planner dispatcher implements `describe_relation` (`0x1201`) and `estimate_scan` (`0x1202`) with version-1 payloads. `describe_relation` uses the same qualified-name request as executor resolution; a successful response contains status 0, relation ID, planner-only handle[16], relation name, u16 column count, and column records in the format above. Status 1 means unavailable. `estimate_scan` carries version, planner handle[16], u16 column count, and column IDs[16]; zero requests the full declared shape. Its response is status 1 for UNKNOWN, or status 0, visible-row count `u64`, u16 column count, then one triple of `readable_values`, `nulls`, and `redacted` `u64` counts per requested column. Planning handles use kind 9 and are connection-local; executor handles and row handles are not accepted. The stream loop caps each planner connection at 256 requests. Clean EOF closes only the planning session; malformed frames or protocol violations abort the invocation. Predicate estimates, access-path disclosure, and supervisor provisioning remain unimplemented.

`open_scan` request: version, relation handle[16], u16 column count, then that many column IDs[16]; count zero requests the full declared shape. Its response is version and cursor[16]. `next_batch` request: version, cursor[16], u16 maximum candidate rows (1–128), u32 maximum response bytes (8–1,048,576). Its response is version, `u8 finished` (0 means continuation follows; 1 means end), optional cursor[16], u16 visible row count, then rows. Each row is an opaque row handle[16], u16 cell count, and cells. Cell state 0 is VALUE followed by scalar type code, u32 value length, and canonical bytes; state 1 is NULL; state 2 is REDACTED and carries no source bytes or length. Scalar values use big-endian fixed-width integers, booleans 0/1, UTF-8 Text/Varchar, raw Bytea/Uuid bytes, and signed two's-complement Decimal/Date/TimestampTz bytes. `close_cursor` request is version and cursor[16]; its acknowledgement is version and status 0. No row ID or storage address appears in a batch.

Mutation requests use the same version and scalar value encoding. `stage_insert` carries version, relation handle[16], u16 value count, then full-shape VALUE/NULL/DEFAULT cells; state 0 is VALUE followed by type, length, and bytes, state 1 is NULL, and state 2 is DEFAULT. Its response is version and a new opaque row handle[16]. `stage_update` carries version, row handle[16], u16 patch count, then zero-based ordinal `u32` and VALUE/NULL cell for each unique patch. `stage_delete` carries version and row handle[16]. Update/delete acknowledgements are version and status 0. REDACTED is response-only. Storage resolves the handle within its connection, derives schema, grants, policies, constraints, and indexes from the pinned catalog, and consumes all handles for a row after a successful mutation. INSERT requires every declared column; generated identities must be DEFAULT and cannot be overridden.

The executor stream loop accepts at most 256 frames per connection and rejects reused request IDs. Relation and row handles and scan cursors use server-side, connection-local state. At most 4,096 row handles may be issued per connection. An unsupported column, invalid cursor, oversized response, or malformed request aborts the bound invocation. Clean EOF ends only the executor session and does not commit or abort; the supervisor must independently observe worker completion and request finalization or abort through its trusted storage channel. Predicate pushdown is not part of this initial dispatcher. The supervisor remains responsible for provisioning the stream and invoking the loop with the correct invocation context.

## Conceptual Operations

```text
resolve_relation(logical_relation_ref) -> table_handle, authorized_schema
open_scan(table_handle, requested_columns, restricted_predicate) -> cursor
next_batch(cursor, max_bytes) -> authorized_batch | end
close_cursor(cursor) -> acknowledgement
stage_insert(table_handle, field_values) -> provisional_row_handle
stage_update(row_handle, field_patch) -> acknowledgement
stage_delete(row_handle) -> acknowledgement
```

Requested limits are capped by the server. Predicates use a bounded, validated expression language; native callbacks are forbidden. Exact expression support and wire encoding remain undecided.

## Mutation Ordering and Transaction Control

All operations bind to the invocation's MVCC transaction. Server-side row handles identify snapshot/private versions, never raw addresses or permission to update the latest row. Mutation messages cannot override the transaction, base version, grant, or policy checks. Updates carry field patches so the executor need not receive hidden unchanged fields.

Serialize each invocation's mutation stream and each cursor's advancement. Initially reject overlapping statement/request streams; use request IDs and handle generations to reject duplicates and stale responses. Bound write-set bytes and outstanding work. No transparent mutation replay after disconnect.

Only the supervisor control role may explicitly finalize/abort a transaction; storage also aborts autonomously on errors, limits, or disconnection. User library return is a request to the supervisor, not commit authority. Provide an authenticated client status endpoint keyed by opaque invocation ID for lost-reply reconciliation. Commit, abort, durable outcomes, and cancellation races follow [concurrency and MVCC](concurrency-mvcc.md). Storage enforces these role distinctions even if SDK definitions expose all message types.

## Planning Channel

There are two distinct planning interfaces. An executor-to-planner submission channel carries bounded structured queries, parameters, physical plans, and errors. The supervisor binds it to the admitted execution context. This allows the SDK SQL code inside query.so to request optimization without receiving the planner's storage-estimation role. The planner validates all input, and request counts/size and planning work are bounded.

Provision a separate planner-to-storage channel bound to the same execution authority, with a planning role and independent estimation budget. Role checks prevent workers from directly invoking storage estimation/maintenance operations and prevent the planner from acquiring unrestricted scan access.

```text
describe_relation(logical_relation_ref) -> planning_table_handle, authorized_schema_and_access_paths
estimate_scan(planning_table_handle, columns, predicate_ir) -> scoped_estimate
```

Estimates describe population, uncertainty, and context-scoped validity, or return unknown/unsupported. Do not export global statistics, physical counts, private costs, raw samples, or global refresh epochs. Expressions use bounded trusted built-ins, never native callbacks. No planner-accessible unrestricted statistics-refresh operation exists. See [statistics service](statistics-service.md).

Unknown estimates permit heuristic planning only while the invocation and planning request remain active. Cancellation, expired deadlines, and fatal protocol errors do not permit continued work or broader access. Planning handles and pending requests expire with the execution context. Connection teardown and cancellation must release estimation resources as well as cursors.

## Batch Representation

```text
scoped row handles
column values containing only authorized payloads
per-column cell states: VALUE | NULL | REDACTED
```

Invisible rows are omitted. Redacted variable-length values must not retain source bytes or original lengths. Initialize padding and avoid serializing unused buffer capacity. Returned metadata must not disclose physical storage addresses or hidden row counts.

## Validation and Errors

Version IPC independently of the native host ABI and structured query/plan formats, as described in the [SDK contract](client-sdk.md). Define maximum frame sizes, collection lengths, nesting, outstanding requests, cursors, and response sizes before implementation. Validate framing before allocation and reject malformed states, stale handles, and cross-context references. Host ABI structures are not wire encodings and must not be sent by copying native memory layouts.

Errors should distinguish invalid requests, denied operations, expired contexts, cancellation, and resource limits without exposing hidden values or invisible row identities. Precise error equivalence and inference behavior require further design.

Define cursor advancement and retry behavior explicitly; the prototype fails the invocation on a required channel disconnect instead of supporting transparent replay. Statement success closes its resources and leaves the transaction active; any statement error aborts the whole transaction. Cancellation accepted before redb commit begins invalidates all handles and discards private writes; after redb commit begins, report pending/terminal outcome rather than promising rollback; reclaim resources only after in-flight tasks release their pins. After a durable commit, cancellation cannot roll back data. Invocation-wide budget exhaustion is terminal before commit.

## Validation and Future Work

Fuzz decoding and state transitions; exercise oversized frames, forged cursors, premature disconnects, and backpressure. Test that denied data never appears in serialized bytes.

The copied socket transport is the initial baseline; serialize directly into final sanitized batch buffers and use bounded vectored sends when useful. [Memory storage](memory-storage.md) specifies the optional sealed-memfd batch extension, required ownership/sealing checks, and adversarial validation before enabling it. Descriptor transfer remains forbidden except for a future explicitly negotiated sanitized-batch capability. Raw storage mappings and writable worker/storage shared memory remain forbidden.
