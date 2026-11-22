# Interactive SQL Sessions and Transactions

## Status and Scope

This document specifies the higher-level SQL client and server behavior. The feature is implemented; validation evidence is tracked in [`plans/interactive-sql/PROGRESS.md`](../plans/interactive-sql/PROGRESS.md). It extends the current one-package/one-transaction submission model so a client can issue `BEGIN`, execute multiple query packages in one MVCC transaction, and finish with `COMMIT` or `ROLLBACK`. It also defines `RUNAS username ... ENDRUNAS` blocks, which compile the enclosed SQL text into one query package executed with that user's authenticated permissions. The client may also route supported DDL statements through the authenticated administrative control API.

The executor remains untrusted. It does not receive administrative credentials, does not own transaction state, and cannot commit or roll back storage directly. The trusted supervisor and storage manager own the transaction across package executions. Administrative DDL remains a separate control operation.

This extension does not promise transactional DDL. DDL is permitted only when the SQL client has no active explicit transaction and is not inside a `RUNAS` block; each DDL operation is an independent durable administrative operation. DDL inside `BEGIN` or `RUNAS` is rejected before dispatch. Rejected DDL inside `BEGIN` leaves the transaction active. Interactive SQL requires a separately versioned protocol/API and cannot silently fall back to v1.

## Client Statement Routing

The higher-level client accepts one statement at a time interactively or a bounded SQL script. A script is parsed into statements before execution; statement boundaries are found by a lexer/parser that understands strings and comments, never by splitting on every semicolon. The client executes statements sequentially and does not pipeline them.

`prdb-client interactive-sql [--instance NAME] [--credential PROFILE]` uses the configured endpoint, default credential, and persistent per-instance SDK release selection defined in [SDK library repository](sdk-library-repository.md); when omitted, it uses `default_instance` and that instance's `default_credential`. It uses the same SDK selection for all generated query packages and submits query.so without an SDK DLL. Additional `RUNAS` credential profiles are loaded from the instance's client configuration. When `--sql-file FILE|-` is supplied, it runs a bounded script; otherwise a terminal runs the prompt-driven REPL and dispatches each complete statement through the same sequential session dispatcher used by scripts; a non-terminal stdin is treated as a bounded script. The prompt accepts multiline SQL and multiline `RUNAS` blocks, waits for a statement terminator or `ENDRUNAS;`, and retains transaction state between entries. REPL parse and statement errors are reported without losing the local transaction state, so the controller can issue `ROLLBACK` after an abort-only package failure. EOF attempts rollback of an open explicit transaction.

The client classifies each statement and routes it as follows:

| Statement | Client action |
| --- | --- |
| `BEGIN` | Request a new explicit transaction from the query service. |
| Supported DML/query | Compile/package and execute it against the current transaction, or use one implicit transaction in autocommit mode. |
| `COMMIT` | Ask the query service to durably commit the active explicit transaction. |
| `ROLLBACK` | Ask the query service to abort and release the active explicit transaction. |
| `RUNAS username ... ENDRUNAS` | Resolve the named local credential profile and execute the complete enclosed SQL body as one query package, either in an implicit transaction or the active explicit transaction. |
| Supported DDL | Only when no explicit transaction or `RUNAS` block is active, translate SQL locally to a typed `ADMIN` control operation and wait for its durable outcome. |
| Other transaction/session commands | Reject as unsupported. |

DDL text is never sent to the query executor or gateway as executable SQL. DDL parsing and name resolution follow the existing authenticated `admin-sql` contract. SQL query parsing/binding/execution remains the versioned SDK behavior in the confined executor. The client may split and classify statements, but classification is not an authorization decision; storage independently validates every query and administrative operation.

When reading a script, the client may detect statement boundaries and obvious unsupported transaction syntax before sending anything. It cannot promise that all query statements are semantically valid before execution unless a compatible trusted parse-only SDK is available. In autocommit mode, statements completed before a later parse or execution error remain committed. In an explicit transaction, a query error aborts the whole transaction as specified below.

### `RUNAS` block syntax and packaging

The block form is:

```sql
RUNAS username
  -- one or more supported SQL statements
ENDRUNAS;
```

`username` is a normalized database user name and selects a locally loaded credential profile. The block body is parsed as ordinary SDK SQL, not as DDL. It must contain at least one supported query/DML statement. The client compiles the whole body into exactly one query package; statements in the body execute sequentially through the versioned SDK within that package. The body is bounded to 65,536 UTF-8 bytes and 64 statements in the initial profile. The package has one ordered result stream, with each statement result tagged by its statement ordinal, and one package outcome. A block is not split into a package per statement. Nesting, crossing block boundaries, an omitted `ENDRUNAS`, empty blocks, and DDL or transaction-control statements inside a block are syntax/unsupported-operation errors. String literals and comments containing `ENDRUNAS` do not terminate a block.

Each block selects one effective database user for every statement in its package. It does not change the transaction controller, create a nested transaction, or grant the executor a credential. SQL parameters and output use bounded package interfaces. The invocation's PSP1 parameter block is supplied to each statement containing a SQL parameter placeholder; a parameterless statement receives an empty parameter slice. PSP1 stores fixed-width signed numeric, date, and timestamp values in little-endian form. The statement dispatcher decodes the borrowed parameter block and copies those payloads to the SDK's big-endian scalar representation before binding and execution. Parameterized statements in one block share that package-level parameter block and remain subject to the SDK's per-statement placeholder validation. The SDK must expose a sequential batch entry point (or equivalent generated wrapper behavior) that executes each statement once, emits statement-ordinal result boundaries, and latches any statement error as package failure. A malformed or unsupported body is rejected before package execution when the local trusted parser can establish that fact; the SDK remains authoritative for parse, bind, and execution errors.

## Credential Profiles and Effective Identity

The client can load multiple named database credential profiles from the instance entry in the persistent config. Each profile maps one normalized database username to a token ID and secret; the profile name is an explicit local selector because the credential itself does not prove a username. Existing `--credentials FILE --default-user USER` and `--credential-profile USER=FILE` options may override configured profiles for one process, subject to the same owner-only, no-follow, canonical-format, duplicate-name, and zeroization rules. The username in a profile is a selector/expectation only; it is never proof of identity.

Authentication proofs remain bearer secrets: client, gateway, and supervisor code must not log them, reflect them, place them in query artifacts/parameters, or retain them in transaction state. The server retains only authenticated principal IDs and the minimum credential/authority epochs needed for revalidation. V2 is available only on loopback listeners in its first release; the gateway rejects non-loopback v2 binds. Remote v2 access requires a later confidential authenticated transport and cannot inherit v1's explicit plaintext bind override.

The client selects one default profile at startup. Plain query packages, `BEGIN`, transaction control, and DDL use this profile. A `RUNAS` block selects another loaded profile for that package only and does not change the default. Changing the default requires an explicit client action while no explicit transaction is active; implicit identity changes inside a transaction are unsupported.

For a `RUNAS username` package, the client authenticates the selected profile to the server. Storage must verify that the supplied token authenticates the exact named live user and revalidate token, login, and current role closure at package admission. Unknown usernames, missing profiles, revoked/expired credentials, or a username/credential mismatch fail closed with a bounded non-disclosing error. Possession of the target user's valid credential is the authorization to execute as that user; the initial feature adds no separate impersonation privilege. The query artifact receives neither token bytes nor the credential profile name as authority; trusted admission binds the invocation to the authenticated effective principal.

In an explicit transaction, the transaction controller and a `RUNAS` package actor may be distinct identities. `BEGIN`, `COMMIT`, `ROLLBACK`, transaction attachment, and transaction status are authorized as the controller that opened the transaction. A normal query package uses that controller as its effective actor. A `RUNAS` package authenticates its named user independently while also proving authority to attach to the controller's transaction. The controller's permissions do not augment the `RUNAS` user's permissions, and the `RUNAS` user's permissions do not grant transaction-control authority. A caller must possess valid credentials for both identities to execute a `RUNAS` block inside a transaction owned by a different user. In autocommit mode, the `RUNAS` actor is also the owner of that block's implicit package transaction; only that actor's credential is needed.

Every package is authorized as exactly one effective actor. Storage applies that actor's grants, role closure, row policies, cell policies, and package scope; it never unions the controller's or another participant's privileges into the actor's view. Every package's requested scope is an independent ceiling. Within an explicit transaction, the storage overlay is also filtered through the current package actor's authorized view: prior private writes by another actor do not become readable solely because they are in the shared write set. Writes retain their authorizing actor for commit-time credential/authority epoch revalidation. The transaction controller may observe returned `RUNAS` results in the local client; possession and use of multiple local credentials is not a data-isolation boundary between people sharing that client process.

## Transaction Semantics

### Autocommit

Autocommit is the default. Each query/DML statement runs as one transaction and is committed or aborted using the current invocation lifecycle. A successful statement becomes durable before the client advances to the next statement. Thus a script without `BEGIN` has statement-level transaction boundaries, not script-wide atomicity.

Each DDL statement also runs independently through its existing authenticated administrative operation and durable outcome protocol. A successful DDL statement is durable before the client advances. A later script failure does not undo earlier query or DDL statements.

### Explicit transaction

`BEGIN` creates a transaction controlled by the authenticated principal. Storage opens one MVCC snapshot at that point and retains the transaction's private write set and validation state across multiple package executions. The transaction is identified by a server-generated opaque ID and is not bound to a TCP connection. Each later package attaches to this transaction after authenticating the controller; a `RUNAS` package additionally authenticates its effective actor. Packages execute sequentially against the same snapshot/write set. An actor can read prior private writes only through that actor's own authorized view. Other transactions do not see the write set before commit.

There is one transaction snapshot, one commit decision, and one durable transaction outcome for the entire explicit transaction. Individual package completion is provisional and does not publish writes. `COMMIT` asks the supervisor/storage path to validate and atomically publish the accumulated write set. Existing snapshot-isolation conflict rules, authority revalidation, and durable-commit requirements apply. A conflict or failed validation aborts the full transaction. `ROLLBACK` aborts it without publishing any writes.

`BEGIN` takes no arguments in the initial version. Isolation levels, read-only modes, nested transactions, savepoints, and transaction-local configuration are unsupported. `BEGIN` while a transaction is active, or `COMMIT`/`ROLLBACK` with no active transaction, returns a bounded `invalid_state` error and performs no state change. The transaction can be reattached from a different TCP connection after authenticating as its controller; an ID alone is never sufficient.

### Failure behavior

Any query package error, SDK statement failure, worker crash, cancellation, deadline expiry, or fatal protocol error marks the explicit transaction abort-only and discards its private writes. The session may issue `ROLLBACK` to release the retained transaction state. Further query execution and `COMMIT` are rejected while abort-only; `COMMIT` reports the aborted outcome and closes the transaction. No partial package writes are retained for a later package. Savepoints and recovery within the transaction are not provided.

A failure in any statement inside a `RUNAS` body fails that single package. In autocommit mode, the block's package transaction aborts without publishing any of its writes. Inside `BEGIN`, the failure marks the enclosing transaction abort-only and discards its complete write set, including writes from packages executed under other actors.

An unsupported DDL statement inside an explicit transaction or `RUNAS` block is rejected by the client before sending it and does not make the explicit transaction abort-only. The client may continue with query statements or issue `ROLLBACK`. The client must not transparently commit, rollback, split the transaction, or change `RUNAS` identity to run DDL.

## Results, Cancellation, and Outcomes

Query results produced inside an explicit transaction are provisional until `COMMIT` returns a storage-confirmed committed outcome. The client must label or otherwise keep them distinguishable from committed results. `ROLLBACK`, abort, or an unknown transaction outcome means those results are not committed database output. The client cannot retract bytes already shown to an interactive user.

For a transaction-bound v2 START, the service acknowledges `accepted`, streams result-data frames using the START request ID, then emits one terminal event whose JSON contains exactly `transaction_id`, `package_id`, `state` (`completed` or `failed`), and `result_lost`. This package terminal event closes only the result stream; `completed` does not commit the transaction. TX_COMMIT or TX_ROLLBACK is still required to decide the enclosing transaction. A worker/setup failure returns `failed` after storage has marked the package failed and transaction abort-only. A result relay failure also fails the package and aborts the enclosing transaction.

Cancellation of a running package aborts the enclosing explicit transaction. Cancellation after commit submission follows the existing commit barrier: the result may be committed, aborted, or unknown and must be reconciled, never inferred from a client acknowledgement or socket close.

Every explicit transaction has an opaque server-generated transaction ID. If the `COMMIT` reply is lost, the client reconnects and authenticates, then queries the transaction outcome by ID; it never sends `COMMIT` again blindly. A response of `unknown` requires reconciliation according to the transaction outcome contract and does not authorize retrying the work as a new transaction.

The client assigns a random nonzero `package_id` before every explicit-transaction package request. The server records package state (`running`, `completed`, `failed`, or `unknown`) against that transaction ID before execution and returns it through `PACKAGE_STATUS`. Repeating an `EXECUTE_PACKAGE` with a previously seen `package_id` never reruns the package; it returns the stored state. Completed results are not replayed: a client that loses result bytes learns that the package completed but must report the output as lost. Worker failure marks the transaction abort-only. After supervisor restart, undecided transactions are aborted and package status cannot authorize replay. In autocommit mode the existing invocation ID and STATUS contract reconcile package commit outcomes.

The first version permits transaction reattachment on different authenticated connections but does not permit concurrent execution. A disconnected client does not immediately destroy the transaction; the controller may reconnect, authenticate, and resume by transaction ID until the server-enforced idle or lifetime limit expires. Losing the client process leaves the transaction open only until those bounded limits expire. BEGIN durably records a recovery marker before acknowledging the transaction. On supervisor/storage startup, a marker without an outcome becomes a durable ABORTED outcome; an existing COMMITTED or ABORTED outcome is preserved. Losing the supervisor/storage process therefore aborts any transaction that has not crossed the durable commit barrier. A lost commit reply is the exception: the transaction may already have reached a durable decision, so its outcome must be looked up. Committed outcomes are retained for the configured outcome-retention period.

## DDL Boundaries

The SQL client uses the query service for `BEGIN`, query package execution, `COMMIT`, `ROLLBACK`, transaction attachment, and transaction status. It uses the control service for DDL. These may be separate connections and can be recreated between operations. For a normal package, the query request authenticates the transaction controller as its effective actor. For a `RUNAS` package, the protocol authenticates both the transaction controller (to attach to/control the transaction) and the selected effective actor (to authorize the package). Neither a connection mode nor a transaction ID grants authority. The control connection authenticates the principal whose privileges are used for the DDL operation.

DDL is allowed only when the SQL client is in autocommit state, with no active explicit transaction. The client waits for the DDL operation's durable outcome before sending the next statement. A successful DDL operation commits independently; a failed or uncertain DDL operation follows the existing ADMIN outcome lookup and no-replay rules. A subsequent query starts a fresh snapshot and observes DDL only if it is authorized and committed.

The initial client does not implement implicit transaction boundaries around DDL. It does not implicitly commit before DDL, nor automatically begin a new transaction after DDL. If a script reaches DDL while an explicit transaction is active, or inside a `RUNAS` block, the DDL is rejected and any active transaction remains active. Users must commit or roll back explicitly, then issue DDL.

## Server and Protocol Requirements

The existing client protocol v1 binds a query connection to one staged invocation and one invocation transaction. It cannot preserve a transaction across multiple START requests or authenticate a separate transaction controller and `RUNAS` actor for one package. The interactive SQL protocol is v2.0: it reuses the 32-byte PDBN frame header and payload limits, but uses major version 2 and strictly versioned payload schemas. A v1 peer rejects v2 before accepting HELLO; a v2 client never retries a failed v2 operation as v1. V2 is query-mode and loopback-only in its first release. A listener configured for non-loopback v2 binding fails startup until confidential authenticated transport is implemented.

V2 retains kinds 1–12 with their v1 meanings unless specifically extended below. It adds these client request kinds:

| Kind | Operation |
| --- | --- |
| 13 | TX_BEGIN |
| 14 | TX_ATTACH |
| 15 | TX_COMMIT |
| 16 | TX_ROLLBACK |
| 17 | TX_STATUS |
| 18 | PACKAGE_STATUS |

PREPARE in v2 accepts an optional pair `transaction_id` and client-generated random nonzero `package_id`; both are required for an explicit transaction and absent for ordinary staging. The fields are exactly 32 lowercase hexadecimal characters. V1 rejects them. The supervisor retains the pair with the staged reservation and carries it into the immutable sealed submission; artifact bytes cannot supply or change the binding. SEAL therefore freezes package identity together with the immutable package bytes and manifest. V2 START for a bound package has exactly `invocation_id`, `transaction_id`, and `package_id`; these must match the staged invocation and retained pair. Autocommit START has only `invocation_id`. If no actor credentials are supplied, the effective actor is the authenticated transaction controller. A RUNAS package whose user differs from the controller supplies `expected_actor_name`, `actor_token_id`, and `actor_token_secret` together. The server authenticates the actor token and checks the exact normalized name; partial actor-authentication fields reject. The controller's normal connection AUTH remains independently required.

Transaction-control requests use strict JSON objects with no unknown or duplicate keys. `transaction_id`, `package_id`, and `begin_request_id` are exactly 32 lowercase hexadecimal characters encoding 16 bytes. TX_BEGIN has exactly `{ "begin_request_id": "<32 lowercase hex characters>" }`; the client generates this nonzero idempotency key before sending BEGIN and preserves it until the outcome is known. The key is scoped to the authenticated controller. Repeating TX_BEGIN with the same key returns the original transaction ID and its current state; it never creates a second transaction. The server retains this mapping through the transaction outcome-retention deadline. TX_ATTACH, TX_COMMIT, TX_ROLLBACK, and TX_STATUS each have exactly `{ "transaction_id": "<32 lowercase hex characters>" }`. PACKAGE_STATUS has exactly `{ "transaction_id": "<32 lowercase hex characters>", "package_id": "<32 lowercase hex characters>" }`.

TX_BEGIN replies with exactly `transaction_id`, `state`, and positive integer `idle_timeout_ms`, `lifetime_ms`, `max_package_bytes`, and `max_packages` fields. A first successful begin returns `state: "active"`; replaying its idempotency key returns the same transaction ID and current state. The state is one of `active`, `abort_only`, `committing`, `committed`, `aborted`, or `expired`. TX_ATTACH, TX_COMMIT, TX_ROLLBACK, and TX_STATUS reply with exactly `transaction_id` and `state`; TX_ATTACH may return only `active` or `abort_only`; TX_COMMIT may return only `committed`, `aborted`, or `expired`; TX_ROLLBACK may return only `aborted` or `expired`. TX_STATUS may return any state and is subject to owner/current-administrator status authorization. PACKAGE_STATUS replies with exactly `transaction_id`, `package_id`, `state` (`running`, `completed`, `failed`, or `unknown`), and boolean `result_lost`. `result_lost` may be true only for a completed package whose result bytes are no longer available. Every successful response uses the generic response frame kind 100; failures use the existing bounded error response kind 101.

These requests authenticate the controller on the current connection; the ID alone is never authority. TX_ATTACH confirms that the live transaction can be resumed and serializes access; it does not reserve the TCP connection. TX_COMMIT returns a bounded durable transaction outcome. TX_ROLLBACK releases the transaction after abort.

PACKAGE_STATUS takes `{ "transaction_id": id, "package_id": id }` and returns `running`, `completed`, `failed`, or `unknown`, plus whether result bytes were lost. Before a package starts, the supervisor reserves this ID against that transaction and binds it to the sealed package digest and effective actor. A duplicate start with the same IDs and digest never executes twice; a conflicting digest or actor is rejected. Package state is retained with the live transaction and its outcome record. Results are never replayed. A completed package with lost output is reported as completed/output-lost; the client may continue or roll back but must not rerun it. A worker failure marks the transaction abort-only. If the server cannot establish a package state, the client rolls back or reconciles the enclosing transaction and never replays the package.

The new query service must provide semantic operations equivalent to:

- `BEGIN`: authenticate/revalidate the controller, reserve transaction resources, open the snapshot, and return a transaction ID and negotiated limits.
- `ATTACH`: on any new authenticated connection, verify the controller identity and transaction ownership; the transaction ID alone is not a capability. Serialize attachment so only one package/control operation can run at a time.
- `EXECUTE_PACKAGE`: bind one sealed query package to the active transaction, create a disposable executor, and run it sequentially. Normal packages use the controller as actor. A `RUNAS` package also supplies independently authenticated actor credentials and the expected normalized username; storage checks the match. The artifact cannot choose or change the transaction ID or actor. Package execution does not finalize the transaction.
- `COMMIT`: authenticate the controller, stop new package executions, validate conflicts and authority/credential epochs for every actor whose package contributed writes, durably publish atomically, and return the transaction outcome.
- `ROLLBACK`: authenticate the controller, stop new work, discard private writes, release the snapshot and resources, and return an aborted outcome.
- `STATUS`: authenticate the controller or an authorized current administrator and report a transaction state/outcome, including reconciliation after a lost commit reply.
- `PACKAGE_STATUS`: authenticate the controller and report the recorded state of one package ID. It never returns/replays package result bytes and never starts execution.

The supervisor/storage own the transaction ID, controller identity, snapshot, write set, write-actor attribution, cumulative accounting, cancellation state, and durable outcome. Each executor remains a disposable, separately confined process and receives only invocation-scoped host capabilities bound by trusted server state to that transaction and package actor. The executor cannot access transaction control operations or name another transaction. A package's requested scope remains a per-package ceiling; transaction reuse does not widen any package's authority. The client authenticates the controller on every attach/control operation and the selected actor on every `RUNAS` package. Storage revalidates credentials, role closure, and permission epochs for each package and validates the recorded write actors at commit.

The initial runtime profile uses a five-minute idle timeout, the existing configured 15-minute invocation lifetime, at most 128 packages per transaction, at most 65,536 rows and 64 MiB of cumulative staged writes, and a maximum 64 MiB sealed package artifact. The existing storage manager's configured active-invocation quotas cap active transactions globally and per controller. These values are returned at `BEGIN` where applicable and may be lowered by server policy, but cannot be raised by the client. Only one package may run in a transaction at a time. Expiration aborts the transaction and releases its retained resources. The transaction outcome uses the configured invocation outcome retention period.

The implementation crosses the client SQL parser/packager, SQL SDK batch dispatcher and result encoding, client credential storage, gateway authentication/session service, supervisor transaction registry, executor admission, storage MVCC context, authorization projection, cancellation, and durable outcome lookup. Update the corresponding [client protocol](client-protocol.md), [gateway](client-gateway.md), [supervisor](supervisor.md), [executor](query-worker.md), [authorization](authorization.md), [storage manager](storage-manager.md), and [SQL library](sql-library.md) contracts when this proposal is accepted. The `RUNAS` client feature does not weaken the existing rule that untrusted query code cannot access ADMIN operations or credentials.

## Compatibility and Failure Cases

- Existing v1 clients retain current behavior: one query package owns one transaction; DDL uses the control API.
- New clients require the transaction protocol version and reject unsupported versions/capabilities without silent fallback.
- Reusing a transaction ID without authenticating its controller is rejected. Reattachment by that controller on another connection is supported.
- A `RUNAS` package is rejected unless the actor credential independently authenticates the exact requested username. Controller privileges are never added to the actor's package authority.
- Duplicate, out-of-order, or concurrent package execution requests are rejected; there is no implicit multiplexing.
- A transaction ID is not a bearer capability. Every status/control request authenticates and checks ownership or current administrator authority.
- Gateway/supervisor restart aborts active transactions that have no durable decision. Durable commit outcomes remain recoverable through STATUS.
- DDL authorization failure, name-resolution failure, or uncertain outcome never changes the state of a query transaction, because the interactive client rejects DDL while a transaction or `RUNAS` block is active.

## Acceptance Evidence

The implementation must demonstrate, across the real client, gateway, supervisor, executor, and storage boundaries:

1. Two sequential packages for the same effective actor in one explicit transaction read each other's writes, while a different actor sees only that actor's own authorized overlay entries and a concurrent transaction cannot see any uncommitted writes.
2. COMMIT publishes the combined write set atomically; ROLLBACK publishes none of it.
3. A package failure, cancellation, crash, timeout, authorization-epoch change, or write conflict prevents the whole explicit transaction from committing.
4. DDL outside a transaction commits independently and is visible to a later transaction; DDL attempted inside an explicit transaction or `RUNAS` block is rejected without silently committing or aborting the transaction.
5. A lost COMMIT reply is resolved by authenticated STATUS without replay; active transactions can be reattached on another connection and are released after configured idle/lifetime limits.
6. A hostile query artifact cannot read/write outside its per-package actor authorization, inspect another actor's private writes without authorization, forge a transaction ID/actor, invoke ADMIN, or commit/rollback its transaction.
7. `RUNAS` blocks with multiple SQL statements execute as one package; a block failure aborts the package and, inside an explicit transaction, the entire transaction.
8. The transaction can be detached and reattached across separate connections by its authenticated controller, but cannot be taken over by another principal or executed concurrently.
9. A `RUNAS` block under a lower-privilege user cannot use the controller's additional grants; missing, revoked, or mismatched target credentials fail closed. Its result and writes use only the target actor's authorization.
10. Existing v1 submission, ADMIN, and sandbox integration behavior remains unchanged.

## Frozen Initial Profile

- SQLB result records stream incrementally over v2. The client labels explicit-transaction output provisional until COMMIT reports a durable committed outcome. Autocommit output remains provisional until its package reaches a committed terminal event. The client does not buffer a whole package or transaction before displaying rows.
- Batch statements use the maintained SDK dispatch surface: supported SELECT statements and the SDK's INSERT v3, UPDATE v1, and DELETE v1 implementations. Unsupported syntax fails through the normal package error path; DDL and transaction-control statements are rejected by the trusted client before batch generation.
- The controller authenticates with the normal v2 connection AUTH. When the effective RUNAS actor differs, START carries the complete `expected_actor_name`, `actor_token_id`, and `actor_token_secret` tuple, independently checked by the server. A partial tuple is invalid. The actor proof is never copied into the artifact or parameters.
- V2 remains loopback-only. Non-loopback listeners reject v2 startup until a confidential authenticated transport profile is specified.

These initial choices are versioned protocol and runtime requirements. Changing them requires a profile revision; none changes the rules that transactions span packages and connections, each package has one independently authenticated effective actor, DDL remains outside transactions and `RUNAS` blocks, and storage alone decides durable outcomes.
