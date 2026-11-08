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

## SU-2 verification: staging state-machine lifecycle and races

- Behavior: internal tests cover owner isolation, exact chunk offsets, seal digest/metadata acceptance, one-shot START, quota release after finish, prepared cancellation, disconnect and TTL reclamation, and cancellation while metadata validation is blocked outside the staging lock.
- Changed fixture: [staging unit tests](../../engine/src/supervisor.rs).
- Verification: `cargo test -p prdb-engine --lib supervisor::tests --locked` passed all 4 staging tests in the repository Nix shell.
- Remaining: bind the authenticated gateway protocol to staging and exercise quota limits under concurrent connections.

## SU-2 dependency: preserve workload class and deadline

- Behavior: PREPARE now stores `deadline_ms` and operational/analytical `workload_class` in the sealed submission metadata. It rejects zero or over-ceiling deadlines; the default ceiling is 15 minutes and is configurable.
- Changed code and contract: [staging metadata](../../engine/src/supervisor.rs) and [supervisor admission contract](../../specs/supervisor.md).
- Verification: staging tests cover deadline ceiling rejection and preservation of the accepted class/deadline through START.
- Remaining: use these fields in scheduler quotas and launch configuration.

## SU-3 dependency: token-bound START and deadline enforcement

- Behavior: a sealed owner-scoped submission invokes storage token admission exactly once; the storage invocation receives the submitted deadline, rechecks it during idle-channel polling, and commits no effects for a revoked pre-START credential.
- Changed code and fixture: [client START callback](../../engine/src/client_protocol.rs), [storage deadline owner](../../engine/src/storage_manager.rs), and [START regression](../../engine/tests/commit.rs).
- Verification: focused real-redb test covers no snapshot/status before START, deadline ABORT after admission, and revoked token rejection with no outcome.
- Remaining: actual worker process launch, process exit observation, and result relay.

## SU-3 / SB-1: sandbox capability preflight before storage admission — implemented

- Behavior: `StagingStore::start_with_sandbox_preflight` probes the configured cgroup parent and checks the complete required sandbox profile before calling storage admission. If the profile is unsupported, the callback is not entered, no storage snapshot is created, and the sealed reservation remains prepared for a later retry.
- Regression: [supervisor staging test](../../engine/src/supervisor.rs) points the preflight at an ordinary temporary directory and verifies it rejects before the callback and preserves the prepared state.
- Contract: [supervisor START lifecycle](../../specs/supervisor.md) distinguishes capability preflight from installing confinement.
- Verification: focused `sandbox_preflight_rejects_before_storage_admission` test passed in the configured Nix shell.
- Remaining: the real START path does not yet use a worker launcher; passing this probe alone does not establish confinement. Implement namespace, Landlock, seccomp, cgroup, clean descriptor/environment setup, and process launch before opening any artifact.

## SU-3 / SB-1: provision worker cgroup before storage admission — implemented

- Behavior: `StagingStore::start_with_worker_cgroup` validates the prepared reservation, checks the host profile, and creates/configures the invocation cgroup before entering the one-shot START callback. Setup errors leave the reservation prepared and do not enter storage admission. The callback receives ownership of the cgroup and must complete confinement and process launch.
- Regression: an ordinary directory used as the cgroup parent is rejected before callback entry; the prepared reservation remains available. This verifies the fail-closed setup boundary only, not successful cgroup provisioning or process launch.
- Contract: [supervisor START lifecycle](../../specs/supervisor.md).
- Verification: the focused regression passed. In the repository Nix shell, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, all workspace tests, and `git diff --check` passed.
- Remaining: connect the callback to executor command construction/spawn and process-exit observation; verify successful launch on a writable delegated cgroup host before treating the worker path as integrated.

## SU-3 / EX-1: prepare and spawn the executor after admission — implemented, IPC provisioning pending

- Behavior: `start_and_launch_executor` creates the cgroup before consuming START, materializes the artifact privately, seals the parameter memfd, prepares Landlock and pre-exec restrictions, calls the supplied admission callback, then spawns only the trusted executor host. The returned `RunningExecutor` kills/reaps on drop and exposes process-exit observation that delegates cgroup emptiness and success authorization to `SupervisorInvocation`.
- Regression: artifact staging checks read/execute-only permissions and directory removal; parameter tests verify all required memfd seals, rewind, and write denial. An unsupported cgroup profile rejects before the storage callback and retains the prepared reservation. No artifact was loaded.
- Contract: [supervisor lifecycle](../../specs/supervisor.md) and [executor lifecycle](../../specs/query-worker.md).
- Verification: launcher unit tests, authenticated START fail-closed regression, and gateway listener integration passed; after routing START, workspace build, format check, warning-denied Clippy, all workspace tests, and `git diff --check` passed in the repository Nix shell.
- Limitation: this machine's cgroup mount is read only, so successful process launch, real cgroup cleanup, and dynamic loading remain unverified. Runtime filesystem paths are explicit caller configuration and need a pinned deployment profile.
- Remaining: gateway START and paired worker channel provisioning are integrated. Add documented host callbacks to a new query ABI version, result relay, then validate a real launch on a writable delegated cgroup host before claiming process-level integration.

## SU-5 / EX-1: bind worker storage channels to the executor lifetime — implemented, ABI callbacks pending

- Behavior: each prepared executor owns separate planner and executor Unix socket pairs. The launcher passes only the worker endpoints through the cleaned descriptor list and command line; `RunningExecutor::observe_exit` serves the trusted endpoints concurrently through `SupervisorInvocation`, waits for both streams to drain, then reaps the process and checks cgroup emptiness before authorizing success.
- Regression: launcher unit coverage verifies both connected pairs and distinct descriptors. The existing storage-manager drain/finalize regression covers concurrent handler drain and refusal to finalize before worker exit observation. No user library was loaded.
- Contract: [supervisor lifecycle](../../specs/supervisor.md), [query worker](../../specs/query-worker.md), and [storage manager](../../specs/storage-manager.md).
- Limitation: successful cgroup-backed spawn remains unavailable on this host; ABI v1 has no callbacks, so SDK query code cannot make documented requests over these channels.
- Remaining: define and implement a new sized query ABI with bounded executor/planner operations and output relay, then exercise the actual C callback path inside the complete sandbox harness.

## SU-2 / NP-3: real gateway submission upload and seal — verified

- Behavior: the supervised service accepts authenticated TCP submission traffic from the real gateway binary, stages TinyCC artifact bytes and confirms PMF1/PSC1/ELF validation at SEAL.
- Regression: the binary integration compiles an ABI-v3 TinyCC fixture, authenticates using the bootstrap credential, uploads it through PREPARE/chunk, and verifies SEAL returns `prepared`. It does not load or execute the fixture.
- Verification: full workspace build, formatting, warning-denied Clippy, tests and maintained TinyCC SDK tests passed.
- Remaining: real START and confined process execution are gated on a host with a writable delegated cgroup; this machine's cgroup v2 mount is read only.

## SU-2 / NP-5: prepared reservation control over TCP — verified

- Behavior: the authenticated real gateway path reports a sealed upload as prepared, cancels it without creating an invocation outcome, then hides it as unknown on STATUS.
- Regression and verification: included in the real supervisor/gateway TinyCC upload test; workspace build, format, Clippy, Rust tests and SDK tests pass.
- Remaining: active cancellation and terminal STATUS through a started worker still require the delegated-cgroup process path.
