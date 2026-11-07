# Supervisor Progress

## SU-2 dependency: bounded submission staging

- Behavior: `StagingStore` binds each reservation to an authenticated connection/principal, enforces one reservation per connection, caps artifact/parameter sizes and chunk lengths, reserves global/per-principal quotas without preallocating declared bytes, requires exact contiguous offsets, validates content lengths and SHA-256 at seal, and exposes immutable bytes only during one-shot start admission. Disconnect reclaims unstarted reservations and returns started IDs for cancellation. Unstarted reservations have configurable expiry and an explicit reaper.
- Contract: [client upload protocol](../../specs/client-protocol.md) and [supervisor lifecycle](../../specs/supervisor.md).
- Changed code: [bounded staging state machine](../../engine/src/supervisor.rs) and [engine module export](../../engine/src/lib.rs).
- Decisions: protocol hard limits remain 64 MiB artifact, 1 MiB parameters, 16 KiB manifest, 8 KiB scope, and 256 KiB chunks. Staging defaults to 256 MiB global reserved bytes, 65 MiB plus metadata limit per principal, 1,024 reservations, and five-minute unstarted TTL; deployments may lower byte limits or adjust TTL.
- Verification: Nix-shell format, workspace build, warning-denied Clippy, and `git diff --check` pass. Tests were not run.
- Remaining: provide a concrete manifest/scope validator, connect authenticated gateway sessions, integrate START revalidation with storage admission and sandbox launch, and run upload/disconnect/expiry race coverage when tests are authorized.

## SU-2 dependency: prepared status and cancellation

- Behavior: owner-scoped staging STATUS distinguishes upload/seal/prepared/start states while hiding foreign IDs. CANCEL removes an unstarted reservation without creating a storage transaction and marks an in-progress seal for discard; active work is returned to the storage cancellation path.
- Changed code and contract: [staging state machine](../../engine/src/supervisor.rs) and [supervisor staging semantics](../../specs/supervisor.md).
- Verification: workspace format, build, warning-denied Clippy, and `git diff --check` pass in the repository Nix shell. Tests were not run.
- Remaining: connect STATUS/CANCEL to authenticated gateway frames and cover concurrent seal/cancel behavior in runtime tests when tests are authorized.

## SU-5 verification: drain paired worker channels before commit

- Evidence: a real-redb test closes planner/executor Unix peers, serves both channels, then sends FINALIZE through the supervisor-only terminal stream and verifies the durable owner outcome is COMMITTED.
- Test: [paired worker drain and terminal commit](../../engine/tests/commit.rs).
- Remaining: worker process launch/exit observation must decide whether successful user return justifies FINALIZE; clean socket EOF by itself is not evidence of process success.
