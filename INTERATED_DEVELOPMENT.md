# Iterated Development: TDD and Specification Refinement

## Purpose

Implement this database in small, verified increments while refining its specifications. Treat the specs as executable contracts to make precise, not as a one-time document to translate wholesale into code. This guide describes future development; it does not claim that a build, harness, or implementation exists.

Start with [AGENTS.md](AGENTS.md), the [specification index](specs/README.md), and the [architecture](specs/architecture.md). Follow the relevant component specs for each increment. User instructions and established project requirements take precedence over workflow suggestions here.

## Workload and Storage Priorities

Build an operational database first, with analytical queries as an explicit supported roadmap. Follow the [workload profile](specs/workload-profile.md), [redb backend](specs/redb-storage.md), and [memory model](specs/memory-storage.md). Add engine codec fixtures, then test redb transaction atomicity, durable reopen, snapshot pressure, and the selected platform profile. Do not build a parallel page store/WAL or claim io_uring unless redb supplies a reviewed backend.

## Preserve the Established Design

- TinyCC compiles C user code against a prebuilt C SDK containing SQL support; Rust implements the engine. Compilation occurs in the client.
- The executor loads the submitted library only after confinement. Treat its entire address space as compromised, including SDK code and Rust callbacks.
- Storage independently enforces identity, invocation scope, row visibility, cell permissions, transactions, and resource limits.
- Concurrent reads and writes, MVCC snapshot isolation, atomic durable commit, and recovery belong to the initial database milestone. Do not substitute a read-only release or single-writer execution model.
- Each invocation owns one transaction; statements are sequential, private writes are visible to that transaction, and statement errors abort it.
- External clients use the dedicated Rust TCP gateway and framed network protocol. Supervisor result relay separates arbitrary worker output from storage-confirmed terminal outcomes; internal process IPC stays local.
- Schema, user, role, credential, and policy administration use a separate authenticated control channel. Native queries receive no administrative credentials.
- The optimizer returns structured plans and receives only authorized metadata/estimates. No native code runs inside trusted storage.

Simple internal implementations are welcome: reference scans, interpreted plans, conservative dependency epochs, and a serialized commit-validation gate are already compatible with the design. They do not justify weakening externally observable guarantees.

## Start Each Work Session

1. Inspect the current tree, applicable instructions, build configuration, and existing changes. Preserve unrelated work. Do not assume the repository still matches an earlier chat.
2. Read the specs and tests for the affected behavior. Check adjacent contracts: changing a handle may affect IPC, the SDK ABI, cursor lifetime, and authorization.
3. Identify the smallest useful behavior to implement and the evidence needed to establish it. Prefer an observable vertical slice over a broad collection of stubs.
4. State the scope briefly and proceed. Resolve routine implementation choices independently. Ask about material ambiguity in user intent or incompatible requirements while continuing work that does not depend on the answer.

Do not begin by implementing every interface, generating all crates, or rewriting the entire specification set. Establish only enough structure for the next testable behavior.

## The Red–Green–Refactor–Specify Loop

### 1. Define an observable contract

Record the relevant spec section, inputs, expected result/error, state transitions, and forbidden effects. Include the trust boundary and concurrency assumptions. For example: two transactions starting from the same row version may stage updates, but only one can commit; the loser publishes no writes.

If the spec is ambiguous, write the proposed clarification before encoding it in a test. Distinguish an established requirement from an implementation choice or unresolved question. Do not make the implementation its own oracle.

### 2. Write a failing test

Create the smallest meaningful test that fails for the intended missing behavior. Run it and inspect the failure. A missing compiler, unavailable sandbox feature, syntax error, or broken fixture is not evidence that the behavioral test is red. Establish minimal scaffolding separately when necessary, then demonstrate the behavioral failure.

Test results and externally observable effects, not private function names, exact internal call sequences, or a duplicate implementation. Use hand-checked examples or an independently structured reference model. Include a denial/failure case when it is part of the contract.

### 3. Implement the minimum coherent change

Make the test pass without bypassing permissions, confinement, transaction boundaries, or real protocol validation. A test-only storage fixture is acceptable for a unit test; it is not evidence that cross-process execution works. Keep unsafe Rust small and document its invariants. Keep C allocation ownership and error propagation explicit.

### 4. Refactor with tests green

Remove duplication and improve boundaries only where the new behavior warrants it. Run focused tests after edits, then the relevant component/integration checks. Do not introduce unrelated abstractions or performance changes merely because the code is open.

### 5. Reconcile specifications and evidence

Update affected specs in the same increment: precise behavior, formats, limits, defaults, failures, compatibility, and remaining questions. Synchronize the index and neighboring contracts. Then run the appropriate regression checks once and record actual results. Do not declare completion while a required behavior is represented only by a placeholder or skipped test.

Repeat for the next small behavior. A regression fix starts with a reproducing test whenever feasible.

## Refining the Specs Without Weakening Them

Use implementation evidence to replace open choices with concrete decisions: tested TinyCC flags, protocol encodings, supported targets, error codes, lock ordering, or bounded resource defaults. Document the reason and affected contracts near the decision. Keep normative behavior in `specs/`, rather than scattering competing rules in source comments and progress notes.

When a test exposes a mismatch, determine whether the code is wrong, the test is wrong, or the spec is genuinely incomplete. Never redefine success merely to preserve passing code. Do not remove concurrent writes, cell permissions, durable acknowledgement, or sandbox requirements to get a milestone green.

For incompatible ABI/protocol changes, define version rejection or migration behavior and test it. A specification change must identify affected tests and components. New requirements need corresponding acceptance tests; purely editorial changes need documentation checks, not artificial runtime tests.

The final specs should describe the chosen behavior, with limitations clearly marked. Keep brief rationale for important tradeoffs; avoid retaining contradictory alternatives as if both were current.

## Implementation Sequence and Exit Evidence

These milestones are dependency guides, not permission to defer correctness until the end. Use tiny cycles within each stage and add cross-component tests as soon as interfaces exist.

| Stage | Deliverable | Required evidence before advancing |
| --- | --- | --- |
| 1. Reproducible toolchain | Pinned Rust/Cargo and TinyCC profile, minimal workspace, C archive build, test runner | Clean builds and maintained C/Rust smoke tests; record exact target and versions. Compile shared fixtures without loading them until confinement exists. |
| 2. Boundary and confinement harness | Rust launcher, fail-closed sandbox, versioned host ABI, bounded IPC | A TinyCC library loads only in confinement; constructor attacks, missing features, ABI mismatch, crashes, and forged frames fail safely. |
| 3. Identity and authorized data access | Real admission, minimal catalog, grants/policies, scoped connections, reference scans | Two authenticated users receive different cell views; forged handles and SDK bypass attempts cannot widen access. Test helper fixtures do not replace control-path tests. |
| 4. First writable storage slice | redb snapshots, private overlays, serialized writer validation/commit, cleanup | Own-write visibility, conflict abort, multi-row atomicity, policy revalidation, durable reopen, and retained-reader safety. Stages 3–4 form the first usable data milestone, not a read-only release. |
| 5. Client-to-commit path | TCP gateway/client framing, C SDK SQL calls, Rust optimizer, supervisor result relay | A real TinyCC-built invocation uploads/seals/starts over TCP, runs sequential reads/writes, commits durably, and reconciles lost outcomes after client/gateway failure. Errors abort all writes; no test-only authorization bypass. |
| 6. Initial DDL and constraints | User/role lifecycle, supported table/index/constraint DDL, policy administration | Real control-channel tests, role changes, unique/FK races, index publication, old readers versus DDL, and stale-writer aborts. Full initial scope is not complete before this stage. |
| 7. Complete initial planning and hardening | Scoped statistics, bounded queues, supported SQL/type coverage, fuzz/property suites | Feature-to-spec coverage, fallback behavior, cancellation, fair progress, pressure handling, and repeatable performance measurements. |

After the operational baseline, complete the required analytical SQL operators with bounded-memory tests and mixed-workload operational tail-latency measurements. The small initial SQL subset alone does not complete analytical support.

Include the data-dictionary record/index contracts, filtered metadata APIs, bootstrap interruption recovery, redb open/recovery validation, and operational status disclosure in the relevant stages. Offline upgrade fixtures must preserve permissions and reject unsupported formats; live telemetry must not be tested as a repeatable MVCC snapshot.

Build the minimal catalog/control functions needed for early slices, then complete DDL coverage; do not hard-code the demonstration users or table into production logic. Begin durability work with the first write set, not as a later replacement for acknowledged in-memory commits.

## Testing Strategy

Use Rust unit tests for pure/versioned storage behavior and C unit tests for SDK parsing, values, and ownership. Use real binaries, internal Unix sockets, the external TCP gateway/client protocol, and TinyCC artifacts for boundary tests. Verify lost replies and gateway restarts through authenticated status lookup. Mocks are suitable for controlled faults, not for claiming that the sandbox, compiler, filesystem durability, or process isolation works.

For concurrency, coordinate explicit barriers or test hooks rather than relying on sleeps. Test both interleavings of cancellation/commit, conflicting writes, policy changes, DDL fences, and version reclamation. Give tests deadlines so deadlocks fail rather than hang. Record randomized seeds and minimize failing schedules into deterministic regressions.

For recovery, inject failures before/after log flush and publication, terminate processes, restart from the same isolated test directory, and verify both data and durable outcome status. A process-kill test alone does not prove power-loss durability; test the logging/flush contract with fault injection and document the limit.

For permissions, vary only hidden data and verify that authorized values and released estimates remain consistent with the specified disclosure model. Separately test declared inference limitations such as constraint outcomes; do not claim complete noninterference or serializability.

Use property/model tests for version visibility, policy composition, decoding bounds, and handle lifetimes. Fuzz the C SQL parser and Rust protocol/DDL decoders independently. Supplemental sanitizer builds do not replace the TinyCC acceptance path. Run hostile libraries only in the sandbox harness.

## Build and Verification Discipline

Use [AGENTS.md](AGENTS.md) for commands. During setup, replace its templates with actual repository paths/targets when those exist. Keep builds reproducible, commit the workspace lockfile, and make generated headers checkable against their source schema.

Run focused tests during the red/green loop. Before completing a code increment, run applicable formatting, lint, Rust tests, C SDK tests, and cross-boundary integration checks. Include documentation tests and required target/profile coverage. Broaden testing when failures, changed dependencies, or new risks justify it; avoid rerunning unrelated expensive suites without cause.

Report missing toolchains or unsupported kernel features explicitly. An integration test skipped because confinement is unavailable does not pass its security gate. Never weaken the sandbox to run a test. Performance measurements must separate SDK release build, query compile/link, loading, first result, and durable commit; optimize only after correctness evidence and a measured bottleneck.

## Completion and Handoff

An increment is complete when its meaningful failure was observed, the implementation passes applicable checks, the specs agree with the behavior, and required paths have no hidden stubs/bypasses. Clearly distinguish development scaffolding from supported functionality.

Leave a compact, reviewable record in the change description or session handoff:

```text
Behavior and spec sections:
Observed failing test and reason:
Implementation and spec decisions:
Commands actually run and outcomes:
Known limitations / checks not run:
Next smallest behavior:
```

After an interruption, inspect the current files and test results before resuming. Preserve unfinished changes and avoid repeating completed setup. Report facts, not inferred passes. Passing tests are evidence for the covered contracts, not a claim that this database is production-ready or security-audited.
