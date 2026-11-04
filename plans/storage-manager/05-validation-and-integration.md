# P11: Acceptance and Integration Evidence

## Test Layers

Build these incrementally during P0–P10. Proposed locations are `crates/storage-manager/tests/` for contract tests, `tests/fixtures/storage-v1/` for golden files, and a separately configured fuzz target directory. Choose actual paths when the workspace exists and update this plan; none of these targets exist merely because they are named here.

| Layer | What it establishes | What it cannot establish alone |
| --- | --- | --- |
| Pure codec/unit tests | Bounds, versions, scalar/key semantics, policy evaluation | Filesystem durability or process confinement |
| Independent model/property tests | MVCC histories, overlays, allocation/handle lifetimes | Kernel completion ownership |
| Deterministic scheduler/I/O faults | Interleavings, sync dependencies, modeled power loss | Every physical device's power-loss behavior |
| Real file/restart tests | Directory lifecycle, WAL replay, outcomes, backend integration | Power-loss durability from process kill alone |
| Raw socket adversarial clients | SDK-independent protocol enforcement | Native-code sandbox isolation |
| Real sandbox + TinyCC library | Client-to-storage authority and lifecycle boundary | Honesty of arbitrary user computations |
| Mixed-load benchmarks | Copy/WAL costs and operational latency under scans | Universal performance guarantees |

Use an independently structured logical reference model, not a copy of page algorithms. Minimize randomized failures into deterministic regressions and retain seeds. Tests should assert visible outcomes and forbidden effects, not incidental private call sequences.

## Mandatory Acceptance Matrix

| Contract | Required scenario / oracle | First phase |
| --- | --- | --- |
| Format safety | Corrupt lengths, flags, generations, checksums, cycles; bounded failure | P1 |
| No-steal/WAL-before-data | Crash drops unsynced state; never recover private or partially committed pages | P2 |
| Buffer ownership | Cancel/drop with I/O outstanding; no free/reuse before original completion | P3/P9 |
| Atomic snapshots | Admission races publication; all or none of a multi-row transaction | P4 |
| Concurrent writes | Same-row conflict; distinct rows on one page both survive | P4 |
| Isolation boundary | Stable reads + own writes; documented write-skew example | P4 |
| Transaction poisoning | Any later statement failure removes all earlier staged writes | P4 |
| Commit cancellation | Both sides of submission barrier, sync failure, lost reply, status reconciliation | P4 |
| Cell permissions | Visible rows with different readable columns; hidden bytes/lengths absent | P5 |
| Authority changes | Grant/role/token/policy/schema change rejects stale pending writes | P5/P7 |
| Service roles | Worker cannot choose identity, commit, administer, or use planner handles | P6 |
| Constraint integrity | Duplicate unique keys and both FK race directions under concurrent commits | P7 |
| Access-path equivalence | Scan/index results and authorization errors agree, including old snapshots | P7 |
| DDL visibility | Failed build absent; old readers survive; affected stale writers abort | P7 |
| Recovery retention | Both roots recover; required files/logs retained through checkpoint/reuse | P8 |
| History safety | Snapshot admission races reclamation; retained rows keep decoding schemas | P8 |
| Backend equivalence | Reference/io_uring same committed state under equivalent fault traces | P9 |
| Scoped estimates | Hidden-only changes do not alter normalized authorized estimate content | P10 |
| Control metadata | Own/global visibility, audit atomicity, revocation while paging | P10 |
| Resource progress | Slow clients/scans cannot monopolize commits, cancellation, or reclamation | P6/P10 |

Assert absence of protected bytes in serialized responses, diagnostic/log captures, and padding. Separate declared constraint/timing inference limits from accidental direct leaks; do not claim complete noninterference.

## Fault Campaign

Enumerate durable transitions rather than only testing random crashes: BEGIN/image/COMMIT writes; segment creation; each file and directory sync; publication; outcome response; checkpoint data sync; manifest sync; CHECKPOINT_END sync; root switch; file/WAL retirement; bootstrap token handoff; DDL/index publication.

For each relevant cut, restart in a fresh process using the same isolated data directory. Compare recovered rows, indexes, catalog/authority epochs, audit, and outcomes with the reference history. Outcomes may be unknown where durability cannot be established, but must never claim abort for a known commit. Test complete loss, torn writes, and corrupt required history separately; corruption must not be treated as routine tail truncation.

Do not delete or mutate an operator's database in a fault test. Use generated fixtures and disposable test directories with explicit ownership.

## Real Component Integration

Include the real [gateway](../../specs/client-gateway.md) and [TCP client protocol](../../specs/client-protocol.md): upload/seal before snapshot admission, explicitly START, stream provisional results through the supervisor/gateway, and reconcile outcomes after network/gateway failure. Internal Unix-socket tests alone do not pass this external boundary gate.

When adjacent components are available, create two authenticated users and a table with genuinely different cell visibility through the real control channel. Build a maintained C query fixture with TinyCC and the prebuilt SDK, load it only after sandbox confinement, issue sequential reads/writes, and finalize via the supervisor. Restart storage and verify data and authenticated status.

Repeat with hostile raw requests, a constructor attempting forbidden access, executor death during a scan, invalid planner output, lost result delivery after commit, and revoked credentials before commit. Ensure the worker owns no database/ring descriptors and cannot issue storage io_uring operations. Compilation smoke tests never substitute for this path.

If a supervisor, SDK, optimizer, or sandbox dependency is missing, complete storage-local evidence and record the exact blocked integration scenario. Do not add a production authentication bypass or mark the final integration gate passed.

## Performance and Copy Measurements

Record hardware, kernel/filesystem, selected backend, dependency versions, durability settings, dataset, cache state, concurrency, quotas, and seed. Measure point/range latency and commit p50/p95/p99, conflicts, throughput, WAL bytes per transaction, allocated/copied bytes, peak pins/history, cache behavior, and sync duration.

Run small writes alone, concurrent independent writes, conflicting writes, and small writes alongside long bounded scans/checkpoints. Separate query compilation from storage execution. Report the full-page WAL amplification and buffered-I/O duplication plainly. No optimization is accepted solely on throughput if it removes permission checks or changes durability.

## Completion Gate

P11 passes when each applicable matrix row links to maintained passing tests, real backend/confinement evidence is available for the supported profile, specs match the implementation, required functionality has no bypass/stub, and known limits are documented. Optional features and unsupported upgrade transitions may remain deferred explicitly. Analytical SQL follow-on completion is assessed separately from storage acceptance.
