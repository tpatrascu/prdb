# Supervisor Progress

## SU-2: per-class cgroup resource profiles — implemented

- Behavior: deployments may configure an optional analytical per-invocation cgroup profile with CPU quota/period, memory, and process-count ceilings. Sealed `workload_class` selects the profile before cgroup creation; operational remains the required default and analytical falls back to it when no separate profile is configured.
- Security/consistency: the class comes from immutable sealed submission metadata, and START still revalidates reservation ownership/state after cgroup setup. The class selects a trusted configured ceiling; it does not grant additional storage authority or override any limit.
- Interface: `prdb-supervisor` accepts the complete optional flag set `--analytical-cpu-quota-us`, `--analytical-cpu-period-us`, `--analytical-memory-max-bytes`, and `--analytical-max-processes`; partial sets are rejected.
- Verification: `cargo test -p prdb-engine --lib supervisor_ipc::worker_result_event_tests::workload_class_selects_the_configured_cgroup_profile --locked --offline -- --exact` passed. Locked workspace build, rustfmt check, warning-denied Clippy, and serial workspace tests passed in the documented Nix shell; cgroup-dependent sandbox tests remained ignored.
- Remaining: class-wide active invocation quotas are implemented below. Separate queue reservations/depth limits and analytical prefetch/cache controls remain unimplemented.

## SU-2 / SU-7: class-wide and per-principal active invocation quotas — implemented

- Behavior: START atomically reserves a slot by immutable sealed workload class, counting `Starting` and `Started` reservations. Global defaults are 64 operational and 8 analytical active invocations; per-principal defaults are 16 operational and 2 analytical. Deployments configure the four limits with `--max-active-operational`, `--max-active-analytical`, `--max-active-operational-per-principal`, and `--max-active-analytical-per-principal`. At either cap, START returns a resource-limit error before storage admission and preserves the sealed reservation for retry. Slots release when a started reservation reaches durable finish/cleanup.
- Changed code and contract: [staging state machine](../../engine/src/supervisor.rs), [supervisor CLI](../../engine/src/bin/prdb-supervisor.rs), and [workload profile](../../specs/workload-profile.md).
- Verification: deterministic staging tests check same-class rejection, per-principal active rejection, independent principal/class capacity, prepared retention, and retry after slot release; supervisor help coverage checks all four flags. In the pinned Nix shell, locked workspace build, format check, warning-denied Clippy, and `cargo test --workspace --locked --offline` passed; commit integration reported 128 passed and 3 cgroup-gated ignored tests.
- Remaining: bounded wait queues and analytical prefetch/cache controls remain open.

## SU-2 / SU-7: per-principal staging reservation quota — implemented

- Behavior: PREPARE atomically enforces a live reservation ceiling per authenticated principal in addition to the global count and byte quotas. The default is 128 per principal; `--max-principal-reservations` configures it. All live staging and active states count. A denied PREPARE creates no reservation, and cancellation/expiry/disconnect/terminal cleanup releases capacity.
- Changed code and contract: [staging state machine](../../engine/src/supervisor.rs), [supervisor CLI](../../engine/src/bin/prdb-supervisor.rs), and [supervisor admission contract](../../specs/supervisor.md).
- Verification: the deterministic principal-isolation/cancellation regression and supervisor help test passed. In the pinned Nix shell, locked workspace build, format check, warning-denied Clippy, and `cargo test --workspace --locked --offline` passed; commit integration reported 128 passed and 3 cgroup-gated ignored tests.
- Remaining: execution wait-queue scheduling/fair progress, overall planner search budgets, and analytical prefetch/cache controls remain open. The storage estimate allowance is tracked in the [statistics plan progress](../statistics-service/PROGRESS.md).

## SU-2 / SU-7: class-bounded pending submission backlog — implemented

- Behavior: PREPARE counts uploading, sealing, sealed, and discard-requested reservations against independent operational and analytical pending limits. Defaults are 256 operational and 64 analytical; `--max-pending-operational` and `--max-pending-analytical` configure them. At the class limit PREPARE returns a resource-limit error without creating a reservation. Starting/started work uses the active invocation quotas above.
- Changed code and contract: [staging state machine](../../engine/src/supervisor.rs), [supervisor CLI](../../engine/src/bin/prdb-supervisor.rs), and [workload profile](../../specs/workload-profile.md).
- Verification: deterministic staging test checks cap rejection, class independence, cancellation release, and successful retry; supervisor help coverage checks both flags. In the pinned Nix shell, locked workspace build, format check, warning-denied Clippy, and `cargo test --workspace --locked --offline` passed; commit integration reported 128 passed and 3 cgroup-gated ignored tests.
- Remaining: execution wait-queue scheduling/fair progress, overall planner search budgets, and analytical prefetch/cache controls remain open. The storage estimate allowance is tracked in the [statistics plan progress](../statistics-service/PROGRESS.md).

## SU-2 dependency: bounded submission staging

- Behavior: `StagingStore` binds each reservation to an authenticated connection/principal, enforces one reservation per connection, caps artifact/parameter sizes and chunk lengths, reserves global/per-principal quotas without preallocating declared bytes, requires exact contiguous offsets, validates content lengths and SHA-256 at seal, and exposes immutable bytes only during one-shot start admission. Disconnect reclaims unstarted reservations and returns started IDs for cancellation. Unstarted reservations have configurable expiry and an explicit reaper.
- Contract: [client upload protocol](../../specs/client-protocol.md) and [supervisor lifecycle](../../specs/supervisor.md).
- Changed code: [bounded staging state machine](../../engine/src/supervisor.rs) and [engine module export](../../engine/src/lib.rs).
- Decisions: protocol hard limits remain 64 MiB artifact, 1 MiB parameters, 16 KiB manifest, 8 KiB scope, and 256 KiB chunks. Staging defaults to 256 MiB global reserved bytes, 65 MiB plus metadata limit per principal, 1,024 global reservations, 128 reservations per principal, and five-minute unstarted TTL; deployments may lower byte limits or adjust TTL.
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
- Remaining: queue reservations/depth limits and analytical prefetch/cache controls remain open; per-invocation cgroup selection and active invocation quotas are implemented above.

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
- Verification update: the confined SQL acceptance verifies the query itself receives a positive SDK cancellation poll response and relays `CANCEL_OBSERVED` before ABORTED. The supervisor now preserves executor channels for a 500 ms cooperative exit window, then forcefully reaps a remaining worker; hard deadlines remain active during that window. The delegated-cgroup TCP process test starts an invocation for a separately created query user, observes it as active through an authenticated bootstrap-administrator control session, accepts administrative CANCEL, kills the gateway before terminal delivery, and reconfirms ABORTED after restarting the supervisor/storage process. Its non-cooperative query reaches durable ABORTED within the two-second acceptance bound.
- Remaining: reject control-mode cancellation after COMMITTING over TCP and broaden process-level worker failure and resource cleanup coverage.

## SU-3 / SB-1: process-boundary START preflight failure — verified

- Behavior: with a regular directory configured as the cgroup parent, the real binaries reject START with the sanitized `unavailable` error before storage admission; a fresh authenticated session reports `unknown` for the invocation.
- Regression: covered in the TinyCC upload/status/cancel binary test. The fixture is compiled and sealed, then discarded without loading or execution.
- Verification: the focused integration and full Rust/TinyCC validation pass. This proves the fail-closed boundary on an invalid host profile, not successful worker launch.
- Remaining: successful cgroup provisioning, confinement installation, and execution still need a delegated-cgroup Linux host.

## SU-5 / SM-6: metadata control reads across real service processes — verified

- Behavior: an authenticated `MetadataClient` can read the public schema, relations, and column descriptions through separate `prdb-gateway` and `prdb-supervisor` processes backed by a READY storage instance.
- Regression: [`metadata_process.rs`](../../engine/tests/metadata_process.rs) exercises the real binaries and bearer-token control path. It does not start or load a query worker.
- Verification: `cargo test --locked --test metadata_process` passed in the configured Nix shell.
- Remaining: this covers metadata control reads only; process-level query execution still depends on successful cgroup provisioning and the executor callback ABI path.
