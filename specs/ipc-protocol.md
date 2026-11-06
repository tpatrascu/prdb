# IPC Protocol

## Transport and Authority

This document specifies internal process IPC. External clients use the separate [network protocol](client-protocol.md) terminated by the [gateway](client-gateway.md); its service role translates an allowlist of admission/control operations and cannot tunnel arbitrary internal messages. Internal executor/planner connections start with copied, framed messages over a Unix socket. Each execution/planning connection is provisioned by the supervisor and bound server-side to one invocation context and role. Client admission is a separate authenticated control interface; it does not inherit an invocation role from a message field. The worker cannot authenticate as another principal by changing a message field.

Use connection-scoped handles with server-side ownership and lifetime checks. Handle possession or guessability alone must not bypass authorization. Do not pass database descriptors or accept arbitrary descriptor transfer.

## Frame Envelope v1

Unix-stream frames use a fixed 20-byte header followed by a copied payload. Header fields are big-endian and ordered as: four-byte magic `PRDI`, u16 frame version (`1`), u16 operation code, u64 nonzero request ID, and u32 payload length. Operation code zero is reserved and rejected. A payload is at most 1,048,576 bytes; reject larger lengths before allocating or reading the payload. The header has no client-controlled service-role, principal, invocation, handle, or descriptor fields. Channel provisioning binds the service role and execution context out of band. Operation payload schemas and operation-code allowlists are independently versioned and validated by the selected channel handler.

Readers accept arbitrary stream fragmentation and leave following coalesced frames available for the next read. EOF is clean only between frames; EOF within a header or payload is a truncated-frame error. Unknown frame versions and malformed headers fail before payload allocation. Frame payloads contain encoded fields, never native struct memory.

## Administrative Control

Authenticated clients submit bounded schema/user/permission DDL or structured equivalents on a dedicated control channel. Storage validates both definitions and current actor authority. Administrative operations may be atomically batched, but cannot be mixed with executor DML/native code. Request IDs, budgets, durable outcomes, and status lookup apply to DDL too.

Credential issuance/revocation uses protected control API fields, not SQL literals or executor messages. Separate database principals/roles from service roles assigned to executor, optimizer, and supervisor channels. Credentials never cross into the executor. See [schema DDL](ddl-schema.md), [identity DDL](ddl-identity.md), and [permission DDL](ddl-permissions.md).

## Discovery and Observability

Provide bounded `list_schemas`, `list_relations`, and `describe_columns` operations with snapshot-bound continuation tokens as defined in [metadata discovery](metadata-discovery.md). Built-in `information_schema` scans use the same visibility rules. Privileged object inspection stays on the control channel; secrets/verifiers never appear in responses.

Live status/monitoring APIs follow [operational metadata](operational-metadata.md) and are explicitly outside invocation MVCC snapshots. Own-invocation status is not authority to enumerate other users or global metrics. Durable outcome lookup may return UNKNOWN/EXPIRED; it must not invent an abort after a lost commit reply. Administrative cursors recheck current authority between pages.

## Relation Resolution and Handles

Structured SQL requests name relations. The optimizer resolves those names through authorized storage metadata, but must not embed its connection-local handles in returned plans. Plans carry logical relation/schema references, which the executor resolves independently on its storage connection. Resolution rechecks the invocation grant and pinned schema; a plan reference grants no authority.

The executor role supports authorized schema discovery and relation resolution for direct SDK access as well as plan execution. It does not receive physical access-path statistics or the optimizer's estimation role. Cursors and row handles remain connection-local. Snapshot/schema mismatch fails the request rather than silently rebinding a plan to another relation.

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
