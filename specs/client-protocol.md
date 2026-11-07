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

STATUS `{ "invocation_id": id }` returns an authorized state: `prepared`, `active`, `committing`, `committed`, `aborted`, `unknown`, or `expired`. Prepared is ephemeral and means no execution has started. Query mode is owner-scoped. Control mode can reconcile the caller's own live or retained outcome, including an ADMIN mutation outcome; looking up another owner's live or retained terminal state requires current database-administrator authority. Committed/aborted recovery status comes from storage, not gateway memory.

CANCEL `{ "invocation_id": id }` returns `accepted`, `too_late`, or an authorized terminal state. Query mode cancels only the caller's prepared or active invocation. Control mode revalidates current database-administrator authority and records an administrative cancellation audit before reporting its decision. Cancellation accepted before COMMIT submission prevents commit; after submission, report too_late/pending resolution. Cancelling prepared staging removes it without starting a transaction. Status/cancellation acknowledgements are not synthetic transaction terminal events.

ADMIN is a control-mode operation with one of these exact envelopes:

- Read-only: `{ "phase": "read", "operation_version": u32, "operation": string, "arguments": object }`.
- Prepare mutation: `{ "phase": "prepare", "operation_version": u32, "operation": string, "arguments": object }`; response contains `operation_id` and `state: "prepared"`. No mutation has run.
- Start mutation: `{ "phase": "start", "operation_id": id }`; this may execute only once on the owning connection, after current-authority revalidation. Response contains the ID, outcome (`committed`, `aborted`, or `unknown`), and operation-specific result if justified by that outcome. It is a single bounded response, not an executor result stream.

STATUS/CANCEL use their existing `invocation_id` field for either an invocation ID or an administrative operation ID; IDs share one opaque namespace. Reserve one prepared operation per control connection. Lost replies follow the same authenticated durable-outcome reconciliation rule; never replay an uncertain administrative mutation. Disconnect discards unstarted reservations and requests cancellation of started work subject to the commit barrier.

The operation allowlist covers schema/table/identity/permission DDL submission, credential lifecycle, and authorized metadata/runtime inspection under their existing specs. Each named operation requires an explicit bounded argument/result schema and authority check; unknown or not-yet-specified operations fail unsupported, not generic dispatch into internal IPC. The server classifies reads versus mutations and rejects attempts to run a mutation with phase=read. DDL uses one statement or bounded atomic batch. Credential issuance returns its secret only once in its operation response; lost delivery requires an explicit authorized revoke/reissue flow, not recovery of plaintext from status. Bootstrap retains its separate local handoff protocol.

The first implemented read operation is `tokens.list`, with arguments `{ "user_id": id, "after_token_id"?: id }`. It requires ownership of the token inventory or current `MANAGE_USERS`/database-administrator authority. Results contain at most 64 records ordered by token ID and a `next_after_token_id` cursor or `null`. Each record contains `token_id`, `label`, `issued_at_us`, `expires_at_us`, `revoked`, and `revoked_at_us`; verifier and bearer-secret material are never returned. The client sends the cursor as `after_token_id` to fetch the next page.

`runtime.invocations.list` is a database-administrator-only read with `{ "limit": positive_decimal_string, "cursor"?: string }` arguments. The limit is at most 64. Results contain an observation time, `consistency: "best_effort"`, and at most the requested number of invocation records ordered by invocation ID. Each record contains its invocation and principal IDs, state, start/deadline times where applicable, and cancellation state/reason. The optional `next_cursor` is an opaque lowercase-hex, process-local cursor bound to the administrator and current invocation-registry generation; clients pass it back unchanged. Every page rechecks current administrator authority. A registry change or process restart can invalidate the cursor, requiring a new first-page request.

`users.create` is a supported identity mutation. PREPARE uses `phase: "prepare"`, `operation_version: 1`, `operation: "users.create"`, and `{ "name": normalized_name }`; names are 1–128 bytes and use lowercase ASCII letters, digits, and underscores, starting with a letter or underscore. START consumes the connection-owned reservation and calls the audited storage identity path, which checks current `MANAGE_USERS`/database-administrator authority and publishes the LOGIN principal and matching user object with its DDL audit and durable outcome in one commit. New users receive no data privileges and no credential; issue a token separately. Success returns the operation ID, committed outcome, generated `user_id`, and commit sequence. The client sends STATUS with the operation ID to reconcile a lost reply. START is never replayed automatically.

`roles.create` uses the same PREPARE/START envelope and normalized `name` argument. START checks current `MANAGE_ROLES`/database-administrator authority and atomically publishes a non-login role principal and matching role object with audit and durable outcome. The role receives no memberships or privileges. Success returns the operation ID, committed outcome, generated `role_id`, and commit sequence.

`schemas.create` uses PREPARE/START with `{ "name": normalized_name, "owner_id": id }`. Names use the normalized lowercase ASCII profile. START requires inherited database-scoped `CREATE_SCHEMA` or database-administrator membership, verifies that the owner is a live user or role, and atomically publishes an empty schema and its owned object with audit and durable outcome. Success returns the operation ID, committed outcome, generated `schema_id`, and commit sequence. Schema ownership does not grant data privileges. `schemas.drop` uses PREPARE/START with `{ "schema_id": id }`; the owner (including inherited membership in an owning role) or a database administrator may drop it. System schemas, schemas with child objects, or schemas with remaining catalog dependencies are rejected. The serialized writer rechecks emptiness before deleting the schema and its object record atomically. Success returns the operation ID, committed outcome, `schema_id`, and commit sequence.

`tables.drop` uses PREPARE/START with `{ "table_id": id }`; the table owner (including inherited membership in an owning role) or a database administrator may drop it. Incoming catalog dependencies reject the operation. Storage atomically removes the table and its owned definitions, rows, and physical indexes. The row cleanup limit is 65,536 rows and 64 MiB of row payloads per commit. Success returns the operation ID, committed outcome, `table_id`, and commit sequence.

`tables.drop_column` uses PREPARE/START with `{ "table_id": id, "column_id": id }`; the table owner (including inherited membership in an owning role) or a database administrator may request it. The operation applies RESTRICT semantics to constraints, expressions, indexes, and policies that reference the column. It rewrites stored tuples and surviving index keys atomically, advances the table schema version and write epoch, and removes the column and its owned default expression. The scan is limited to 65,536 rows and 64 MiB of combined old and rewritten tuple payloads. Success returns the operation ID, committed outcome, `table_id`, `column_id`, and commit sequence.

`tables.create` uses PREPARE/START with `{ "schema_id": id, "name": normalized_name, "columns": [...] }`. The non-empty `columns` array contains 1–4096 objects with required `name`, `type`, `nullable`, and `generated_uuid_identity` fields; the last two are JSON booleans. An optional `default` is an object: `{ "kind": "literal", "value": "..." }` uses a type-specific canonical string, `{ "kind": "null" }` supplies SQL NULL, and `{ "kind": "current_principal_id" }` supplies the authenticated principal UUID (UUID columns only). Boolean literals are `true`/`false`; integer, date (days from 1970-01-01), and timestamptz (UTC microseconds from 1970-01-01) values are canonical decimal integers; decimal literals have exactly the declared scale; UUID literals are 32 lowercase hex digits; bytea literals are lowercase hex; text and varchar literals are their string contents. A generated UUID identity cannot also have a default. Type strings are `boolean`, `smallint`, `integer`, `bigint`, `decimal(p,s)`, `text`, `varchar(n)`, `bytea`, `uuid`, `date`, or `timestamptz`, with canonical positive decimal parameters. Names are normalized lowercase ASCII identifiers. START requires inherited schema `CREATE` or database-administrator membership, rejects reserved system schemas, and atomically publishes the table, ordered columns, default expressions, audit, and durable outcome. New tables receive no data grants. Success returns the operation ID, committed outcome, generated `table_id`, ordered `column_ids`, and commit sequence. Inline constraints are not accepted by this operation; add constraints through their separately authorized DDL route.

`users.rename` and `roles.rename` use PREPARE/START with `{ "user_id": id, "name": normalized_name }` or `{ "role_id": id, "name": normalized_name }`. START requires the corresponding `MANAGE_USERS` or `MANAGE_ROLES` privilege, or current database-administrator authority. It atomically updates the principal name and authority epoch, matching named object and object version, audit, and durable outcome.

`users.login` uses PREPARE/START with `{ "user_id": id, "enabled": "true" | "false" }`. START requires `MANAGE_USERS` or current database-administrator authority and changes the LOGIN flag and authority epoch atomically. Disabling LOGIN also revokes all outstanding user tokens in the same commit; enabling LOGIN does not restore revoked tokens.

`users.drop` and `roles.drop` use PREPARE/START with `{ "user_id": id }` or `{ "role_id": id }`. START requires the matching management privilege or current database-administrator authority and applies RESTRICT semantics: any remaining catalog dependency rejects the drop. A user drop atomically revokes outstanding credentials, removes the principal and named object, and writes a permanent identity tombstone.

`roles.grant` and `roles.revoke` use the same PREPARE/START envelope with `{ "member_id": id, "role_id": id }` arguments. START checks current membership authority, atomically updates the membership record, matching dependency edge, member authority epoch, audit, and durable outcome. It rejects nonexistent/wrong-kind endpoints, duplicate or missing membership changes, cycles, and changes that would remove the last usable database-administrator credential. Memberships target users or roles and always grant the complete role; no admin option exists.

`privileges.grant` and `privileges.revoke` use PREPARE/START with `{ "grantee_id": id, "privilege": "manage_users" | "manage_roles" | "create_schema" }`. START requires current database-administrator authority and atomically updates the database-scoped system privilege, its matching dependency edge, the grantee's authority epoch, audit, and durable outcome. Grants are rejected if already present; revokes are rejected if absent. The affected grantee must be a live principal.

`tokens.revoke` is a supported mutation. PREPARE uses `phase: "prepare"`, `operation_version: 1`, `operation: "tokens.revoke"`, and `{ "token_id": id }` arguments. It reserves one opaque operation ID on the authenticated control connection and returns `{ "operation_id": id, "state": "prepared" }`; it does not look up or revoke the target. START uses `{ "phase": "start", "operation_id": id }` on that same connection. The server revalidates the session, consumes the reservation once, and delegates to the audited storage token-revocation API, which checks `MANAGE_USERS`. Success returns the operation ID, `outcome: "committed"`, and a commit sequence. Unknown, already revoked, and unauthorized targets share the storage operation's non-disclosing failure. Disconnect discards the unstarted reservation; an uncertain start is reconciled with STATUS and is never replayed automatically.

`tokens.issue` uses the same prepare/start ownership rule. PREPARE arguments are `{ "user_id": id, "label": string, "expires_at_us": positive_decimal_string }`; label length is 1–128 UTF-8 bytes. START delegates to the audited storage credential issuance path. Its single success response includes the operation ID, committed outcome, token ID, one-time canonical padded base64 token secret, and commit sequence. The secret is never included in STATUS, later reads, storage records, or audit events. A lost issue response requires explicit revoke/reissue; the server cannot recover the plaintext secret.

PING has `{}` payload and returns `{ "alive": true }`; it is not a health/durability or authorization proof. ERROR has `code` and a sanitized message of at most 256 UTF-8 bytes. Initial code categories: invalid_frame, unsupported_version, unauthenticated, unavailable, invalid_state, invalid_request, resource_limit, cancelled, internal_error. Do not expose raw exceptions, SQL parameters, private names, paths, token details, or competing principals.

ERROR is response kind 101, uses the failed request ID and response sequence zero, and has `{ "code": string, "message": string }`. The gateway closes the client connection after an ERROR response. Its message is selected from fixed public text by category and never copies an internal exception.

`tables.rename` uses PREPARE/START with `{ "table_id": id, "name": normalized_name }`. The table owner or a database administrator may rename a non-system table. START checks the final schema-local name set while serialized, keeps the table ID and tuple schema version, and increments the write epoch and object version. Success returns the operation ID, committed outcome, `table_id`, and commit sequence.

`tables.alter_column_nullability` uses PREPARE/START with `{ "table_id": id, "column_id": id, "nullable": "true" | "false" }`. The table owner or a database administrator may change nullability on a non-system table. Setting `nullable` to "false" validates persisted rows under the table DDL fence and returns a constraint violation if any value is NULL; setting it to "true" drops the matching internal NOT NULL constraint when present. Success returns the operation ID, committed outcome, table and column IDs, the resulting nullable value, and commit sequence.

## Validation and Extension

Test partial/coalesced frames, oversized lengths, malformed JSON/base64, duplicate IDs/keys, incorrect sequences, cross-session submissions, revoked credentials between upload/start, slow uploads without snapshot pins, upload substitution, forged worker terminal bytes, reconnect/status, and gateway death around commit. Test multiple concurrent connections while retaining one stream per connection.

This framing can be implemented behind other transports, or another gateway may map the same admission/result operations to its own transport. Future TLS/HTTP/3 and multiplexing require separate explicit versions/capabilities and equivalent failure semantics. They must not change transaction identity, internal process roles, or the client's prohibition on automatic replay.
