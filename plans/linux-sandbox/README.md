# Linux Sandbox Implementation Plan

## Contract and Dependencies

Implement [Linux confinement](../../specs/linux-sandbox.md) before any artifact constructor or dependency initialization. Use the pinned [runtime profile](../toolchain/README.md), minimal [supervisor](../supervisor/README.md) launcher and [executor](../query-worker/README.md). The entire executor address space is untrusted, including Rust callbacks and the SDK.

## Decisions Before Implementation

Select minimum kernel features, deployment privileges/rootless requirements, namespace/filesystem restrictions, seccomp allowlist, capability dropping, `no_new_privs`, descriptor policy and resource controls. Evaluate Landlock as an additional layer. Specify permitted loader dependencies, scratch access and threading/syscalls with positive and forbidden-operation fixtures. Required features fail closed; optional storage I/O fallback is unrelated.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| SB-1 | Missing required kernel feature or resource delegation prevents launch before artifact loading | Capability probe and explicit supported-profile diagnostics; no weaker execution fallback |
| SB-2 | Clean host sees only provisioned descriptors/environment/resources, with no storage memory inheritance | Launch a clean execution image, close inherited descriptors, scrub secrets/loader overrides and restrict filesystem/process/network views |
| SB-3 | A TinyCC constructor attempting file/network/process access is denied before the query entry point | Establish all required controls before dynamic loading; constrain loader search/dependencies and test dependency constructors too |
| SB-4 | Hostile artifacts cannot trace/signal/read trusted processes, escalate privileges, spawn outside limits or discover database files | Add narrowly scoped syscall policy plus isolation/resource layers; document unsafe syscall invariants and profile-specific residual limits |
| SB-5 | Infinite loops, memory/output floods, child attempts and crashes terminate within deadlines and release descendants | Supervisor kill/reap and storage abort/drain integration; resource limits include process count and wall time |
| SB-6 | Supported profile runs legitimate C SDK/SQL work while every negative fixture remains confined | Run the real artifact suite per deployment profile and test launch failures at each restriction boundary |

## Acceptance and Handoff

The hostile-artifact harness is a prerequisite to loading query libraries anywhere in tests. Never use `tcc -run` or a trusted-process loader. Missing confinement means an unverified/failed gate, not a skipped security success. Verify no home/credential/database descriptors, external network, unauthorized IPC or trusted-process access. Record kernel/profile/privileges and known kernel/timing limitations; containment does not attest to query correctness. First behavior: fail-closed capability detection without loading an artifact.
