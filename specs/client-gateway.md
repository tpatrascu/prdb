# Client Gateway

## Decision and Process Boundary

Introduce one dedicated Rust gateway process between network clients and the supervisor. The initial transport is the [unencrypted framed TCP protocol](client-protocol.md). One asynchronous gateway serves many connections; do not create a process per connection. The gateway is separate from storage and the supervisor even if the latter two share a process initially.

No additional intermediary process is required: the gateway is the network intermediary. The supervisor provides a transport-independent admission/result service over bounded local IPC. Future TLS/TCP, HTTP/3, or other gateways can implement that service without changing executor-storage IPC, transaction semantics, or query-library ABI. They are not enabled by this decision, and the TCP endpoint does not negotiate or downgrade from a secure transport.

## Ownership

| Component | Owns |
| --- | --- |
| Gateway | TCP sockets, frame parsing, connection/session limits, upload forwarding, response framing, network backpressure |
| Supervisor | Admission service, immutable artifact staging, submission reservations, invocation lifecycle, sandbox launch, bounded result relay |
| Storage | Credential/authority validation, snapshots, permissions, transaction decisions, durable outcomes |
| Executor | Untrusted query execution and provisional output on its assigned local channel |

The gateway must not load query libraries, launch executors, open database files, own storage I/O resources, or access arbitrary storage operations. Its service channel can request admission and authorized client operations, but cannot impersonate the supervisor's commit role. Do not hand network connections or session credentials to executors.

The gateway handles bearer credentials and plaintext results and therefore remains within the confidentiality boundary. A compromised gateway can steal credentials and alter client-visible traffic. Separation reduces access to storage/process-control capabilities; it is not an end-to-end confidentiality guarantee against gateway compromise.

## Initial Deployment

Bind only loopback by default. Enabling a non-loopback bind requires explicit operator configuration that names the plaintext endpoint. TCP provides no encryption, peer authentication, or protection against an active network attacker here; bearer tokens and results are exposed to anyone able to intercept that traffic. This is a local/trusted-network development transport, not a secure remote deployment profile. Do not infer database identity from client IP or the host's OS user.

Keep initialization, first-token delivery, and offline recovery on the permission-restricted local setup channel. A READY database may expose authenticated query and administrative client operations through the gateway. An administrative connection is a distinct connection mode, not an authorization grant. Ordinary token/role/DDL checks still apply.

## Gateway–Supervisor Service

Provision a private local Unix-socket channel with authenticated service identity and a distinct gateway role. Use bounded framed messages and immutable, copied bytes initially; no generic descriptor forwarding or paths from clients. In protocol v1, bind each logical session to one TCP connection and its dedicated Unix service stream; the supervisor owns one `ClientConnectionSession` for that stream. This channel pair represents `(gateway generation, connection ID, session ID)` and is the only session reference; v1 does not serialize reusable session IDs. A disconnect or gateway restart closes the pair and destroys its session, and reconnect requires HELLO and AUTH again. If a future version multiplexes sessions or reuses service streams, it must add trusted generation/connection/session identifiers and reject cross-session replies. Gateway claims of a principal ID are never authoritative.

Expose these semantic operations:

- Authenticate/close session; validate token with storage, return a scoped session reference.
- Prepare/upload/seal submission; stage bytes under an internal supervisor-owned identity, enforce cumulative limits, and return an opaque reserved invocation ID.
- Start a sealed submission; revalidate credentials/scope, create the storage context, launch execution at most once for that live reservation, and open a bounded result stream.
- Get status/cancel invocation; check current owner/admin authority with storage.
- Submit bounded administrative operations; route to the separate storage administrative role and preserve atomic audit/outcomes.

For interactive SQL protocol v2, kinds 13–18 are accepted only on the loopback query listener and forwarded through the dedicated gateway-only `InteractiveTransaction` IPC operation. The transaction ID and package ID remain storage-owned bindings; every request reauthenticates the controller, and RUNAS START separately authenticates its actor. The executor service role cannot invoke this IPC operation or the ADMIN control operation. The gateway forwards each v2 request but does not own the MVCC context or pin a TCP connection to it. See [interactive SQL](interactive-sql.md) for the exact wire schemas and recovery behavior.

Every request carries a correlation ID and server-scoped session reference; responses cannot cross sessions. Exact local encoding may reuse the engine IPC envelope but has an independent gateway-service version. Never tunnel arbitrary worker, planner, or supervisor messages from the network. Storage remains authoritative for authentication, permission revalidation, and commit status.

Upload authentication and execution admission are separate. Initial authentication authorizes bounded staging; it does not pin MVCC history or guarantee later admission. The supervisor owns the staging bytes, validates actual lengths/digests, seals them against modification, and never trusts a client path or gateway assertion of immutability. Hashes detect mismatches, not malicious code.

## Result Routing

The required route is:

```text
executor -> bounded local output -> supervisor result relay
         -> gateway service stream -> TCP result frames -> client submission SDK
```

There is no executor-to-client socket and no storage-page-to-network shortcut. Executor bytes are arbitrary untrusted application output, not protocol messages. The supervisor wraps them in data events associated with its own invocation context. Only storage-confirmed outcomes forwarded by the supervisor can generate terminal transaction events. A worker emitting bytes resembling COMMITTED must remain ordinary provisional data.

Bound output per invocation at every hop. The executor host, supervisor result relay, and gateway enforce the trusted deployment's cumulative provisional-result budget. The supervisor configures worker and drain limits with `--max-total-result-bytes`; the gateway configures its independent relay limit with the same option. Both default to 64 MiB, and operators can raise them together for larger production results. The gateway counts RESULT_DATA payloads independently and rejects the event that would exceed its configured budget before forwarding it. The terminal control frame is outside this application-output budget. Encoded frame/chunk limits remain fixed transport safety envelopes. The supervisor may relay owned buffers without repeatedly decoding/copying payloads, but buffers remain bounded and separate from raw database pages. Client slowness cannot retain storage locks or pin the entire buffer pool. Socket write completion means delivery progress, not database commit. The gateway's process-local result-payload byte counter advances only after a complete client frame is written and is exposed only in administrator runtime-list protocol version 6.

In the initial TCP bridge, result writes are synchronous. When a client stops reading and the TCP socket blocks, the gateway stops consuming that client's service stream, propagating pressure to the supervisor's bounded result relay and executor output channel. Do not add an unbounded gateway-side queue to hide a slow client. This propagation does not itself satisfy the separate deadline/cancellation requirement below.

## Disconnects, Backpressure, and Restart

Use bounded queues, upload/result byte budgets, unauthenticated connection limits, failed AUTH throttling, idle deadlines, and per-principal admission quotas. The initial gateway permits at most 10 failed AUTH attempts per source IP in a rolling 60-second window (operator-configurable); it reserves in-flight attempts so concurrent requests cannot bypass the limit. The source-history map is bounded at 4,096 IPs and denies new AUTH attempts while full. Credential bytes are not retained by the limiter, and client IP remains a throttling key only, never a database identity. Reserve a separate control path and queue capacity for status/cancellation; a blocked result stream must not monopolize it. If backpressure exceeds the invocation's budget/deadline, request cancellation; do not buffer unbounded output or silently detach execution.

The Rust gateway provides this reservation through two loopback-default TCP listeners. Configure the ordinary listener with `--bind` and the control listener with `--control-bind`; each defaults to an ephemeral loopback port and reports its bound address at startup. The ordinary listener has 32 workers and a 64-connection pending queue by default. The control listener has 2 workers and an 8-connection pending queue, configurable with `--control-workers` and `--control-queue-capacity`. It accepts HELLO, AUTH, STATUS, CANCEL, ADMIN, and PING requests and rejects submission upload/seal/START kinds with the canonical `invalid_request` error. Both listeners share the per-IP connection and failed-AUTH limiters. Clients that need status or cancellation during saturated query output must connect through the configured control address; the supervisor remains authoritative for session and invocation permissions.

For v1, client disconnect or gateway failure invalidates its connection-bound session and requests cancellation of associated active invocations. Interactive v2 transaction state is storage-owned and independent of a TCP connection: disconnect or gateway restart does not immediately destroy it, and the controller may reauthenticate and reattach until its idle/lifetime limit. Supervisor/storage startup records ABORTED for any undecided transaction while preserving a durable terminal outcome. Before irreversible COMMIT submission, accepted cancellation aborts private writes. After submission, resolve the durable result; a lost connection cannot promise rollback. Supervisor/storage continue resolving terminal outcomes even if no gateway is available. Discard unstarted staging reservations on disconnect/expiry; do not start them in the background.

After gateway restart, clients authenticate anew and query invocation status by ID. Result replay/resumption and durable submission deduplication are not provided initially. A durable commit outcome does not imply that result bytes were retained or delivered. Supervisor channel failure closes affected client sessions; never replace an uncertain in-flight request by replaying it to a new supervisor.

## Extension and Validation

Additional gateway transports must preserve authenticated operations, admission phases, provisional results, final outcomes, cancellation races, and all quotas. Keep transport-specific connection IDs, HTTP status codes, or QUIC streams outside the transaction identity. Test capability/version rejection rather than implicit fallback.

Test fragmented frames, upload floods, revoked sessions, cross-connection IDs, immutable-artifact substitution, forged result envelopes, slow clients, client/gateway crashes at commit transitions, control-path progress during saturated output, and recovery/status through a newly authenticated connection. Real gateway integration supplements internal socket tests; it does not replace executor sandbox tests.
