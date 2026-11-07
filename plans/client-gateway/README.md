# Client Gateway Implementation Plan

## Contract and Dependencies

Implement the dedicated asynchronous Rust [gateway](../../specs/client-gateway.md), separate from storage and supervisor. Use [client framing](../client-protocol/README.md) and a distinct bounded local [supervisor service](../supervisor/README.md). Gateway handles credentials/results within the confidentiality boundary; separation does not protect clients from a compromised gateway.

## Decisions Before Implementation

Define listener configuration with loopback default and explicit non-loopback plaintext opt-in, connection/time/rate limits, local service identity, generation/session binding, queue capacities and restart semantics. The gateway has no database files, artifact loader, worker-launch or transaction-finalization capability.

## Increments

The first gateway foundation increment implements the loopback-default TCP bind policy and requires an explicit plaintext opt-in for non-loopback addresses. The returned listener exposes TCP accept; connection framing, limits, and the supervisor service bridge remain unimplemented.

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| GW-1 | Default listener accepts loopback only and malformed/slow unauthenticated peers stay within limits | One asynchronous process serving multiple connections; strict framing, idle deadlines and bounded allocations |
| GW-2 | Spoofed session/principal or cross-generation replies fail without crossing client connections | Authenticated gateway-service role, trusted session references and correlation IDs; forward only an explicit operation allowlist |
| GW-3 | Real upload/seal/START forwards owned bytes while supervisor enforces immutability and admission | Translate network operations without client paths/descriptors, snapshot allocation or gateway assertions of authority |
| GW-4 | Slow result receiver cannot starve another connection's status/cancel or retain unbounded buffers | Bounded queues at each hop, output budgets, no storage locks during sends and cancellation on exceeded deadlines |
| GW-5 | Worker-crafted terminal-looking data remains RESULT_DATA; only trusted relay events become TERMINAL | Keep result payload and control envelopes separate, validate IDs/sequences and preserve provisional semantics |
| GW-6 | Client/gateway/service-channel death before and after commit gives correct cancellation/unknown/reconciliation behavior | Close sessions, discard unstarted staging, continue storage outcome resolution and require fresh authentication after restart |

## Acceptance and Handoff

Run real TCP clients against separate gateway and service processes, including saturated output, session revocation, forged service requests and reconnect/status after crash. Verify no automatic replay, result retention claims or arbitrary IPC tunnel. First behavior: loopback listener with bounded malformed-frame rejection after protocol fixtures exist.
