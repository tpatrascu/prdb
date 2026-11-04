# Agent Execution and Handoff

## Starting a Session

Read [AGENTS.md](../../AGENTS.md), the [development guide](../../INTERATED_DEVELOPMENT.md), and this plan's [index](README.md). Inspect actual code, tests, progress, and user changes. The absence of code when this plan was written is not permission to recreate a workspace that now exists.

Choose the earliest uncompleted dependency and one observable behavior within it. Do not attempt a whole phase in one change merely because it has one heading. Resolve routine implementation choices autonomously; material requirement conflicts belong in explicit spec clarification.

## Suggested First Tasks

1. Establish the minimal Rust workspace and toolchain checks; report setup failures separately from behavior failures.
2. Clarify one required redb table/key/value mapping decision and add a hand-checked valid/invalid logical codec fixture.
3. Observe the intended decoder test fail, implement bounded decoding, and run the focused checks.
4. Add one versioned engine key/value codec and its negative case.
5. Continue the foundation phase in dependency order; introduce the selected redb durability/fault model before making durable-commit claims.

These are starting tasks, not instructions to skip inspection or to implement all codec families before testing anything.

## Per-Increment Procedure

1. State the behavior, source spec section, trust boundary, and expected failure.
2. Refine ambiguous normative details in `specs/` and record the decision in the affected phase/register.
3. Write and run a meaningful failing test. Record its assertion failure; unavailable tools or broken scaffolding are not red evidence.
4. Implement the smallest coherent change, preserving quotas, permissions, ownership, and transaction guarantees.
5. Refactor with tests green; run relevant regression/integration checks once after the final edits.
6. Synchronize contracts, commands, fixture versions, and progress. Keep unfinished integration work visible.

## Commands and Environment

Use the actual commands in [AGENTS.md](../../AGENTS.md). After the workspace and lockfile exist, baseline checks are:

```sh
cargo build --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

During TDD run the actual selected package/test target rather than repeatedly running every suite. Add documented commands for fuzzing, fault scenarios, redb durability profiles, C fixtures, and benchmarks when those targets exist. Do not invent executable command names in handoff notes.

TinyCC is required for real C SDK/query acceptance; GCC/Clang or Rust-only mocks are not replacements. Never use `tcc -run` or a trusted-process loader for hostile query libraries. Missing sandbox support is a failed/unverified integration gate, not a reason to relax confinement.

## Progress Tracking

When implementation begins, maintain `plans/storage-manager/PROGRESS.md` with phase/task IDs and states: `not started`, `in progress`, `verified`, or `blocked`. Do not pre-mark any phase verified based on this documentation. A blocked dependency does not prevent independent work with valid evidence.

Use one compact entry per completed increment:

```text
Task / phase:
Behavior and normative spec links:
Changed code and fixture paths:
Observed red assertion:
Commands actually run and outcomes:
Integration/backend/profile evidence:
Decisions and spec updates:
Remaining limitations or blockers:
Next smallest task:
```

Link actual tests and code; preserve earlier evidence rather than replacing it with an unsupported “all tests pass.” Update this implementation plan when dependencies change, while leaving normative semantics in the specs. A later agent must be able to tell what exists, what was tested, and what remains.

## Stopping and Resuming

Stop at a coherent tested increment where possible. If interrupted, record incomplete edits, failing tests, temporary fixtures, and the exact next action. Do not mark a phase complete because its happy path works or because adjacent integration is unavailable. Resume by checking the actual repository and rerunning only the focused evidence needed to establish the current state.
