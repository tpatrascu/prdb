# Statistics Service Progress

## ST-2 follow-up: measure predicate-estimate planner IPC — measured

- Harness: a real `serve_planner` stream profile seeds a fresh authorized 2,052-row INTEGER table and a readable leading index. Across 11 fresh invocation sessions per run, it verifies one exact equality match and times direct `PlannerMetadataConnection` estimation, `DescribePlanRelation`, and `EstimateScan` request/response roundtrips.
- Evidence: five fresh-database runs produced median-of-run p50s of 2.579 ms for direct planner service (2.500–2.609 ms), 1.786 ms for relation description (1.767–1.845 ms), and 2.584 ms for the `EstimateScan` IPC roundtrip (2.500–2.603 ms). Median-of-run p95s were 2.865, 2.077, and 2.982 ms respectively. The `EstimateScan` IPC cost was within measurement variation of the direct service call; a filtered plan makes a separate relation-description request.
- Scope: this measures an indexed single-column equality through the real storage planner stream. It excludes the isolated optimizer process, other predicate shapes, full SQL planning/execution, operational mixed load, and target hardware; those remain open.
- Verification: the focused real-redb planner stream test passed in all five profile runs, preserving the exact `(2,052 visible, 1 matching)` response. `cargo fmt --all -- --check` passed.

## ST-2 / OP-4 follow-up: extend exact join-pair estimates to 16,384 — implemented

- Contract: authorized exact join-cardinality estimates now cover up to 16,384 rows per side, while retaining the 8 MiB total decoded-row budget, 4 KiB per encoded composite key, one-to-eight key components, policy/readability requirements, and the shared estimate allowance. A request under the full population bound returns `Unknown` when the actual population exceeds its requested maximum; it never returns a partial pair count.
- Red/green: the real-redb regression first requested a 4,097 maximum on a small population and received `Unknown` because the service rejected every request above 4,096. After widening the service bound, the regression validates exact duplicate-key multiplicity at 4,097 rows and exact output at 16,384 rows per side, while smaller requested bounds remain `Unknown`.
- Verification: the focused transaction regression passed at 4,097 and 16,384 rows per side. A delegated 17×4,097 SQL acceptance now receives the exact pair count, selects kind 17, returns all 17 rows, and overlaps 192 operational reads and commits. Workspace build, format check, warning-denied Clippy, and the complete delegated serial workspace suite passed: 409 library tests, 206 commit tests, 3 metadata-process tests, 85 transaction tests, all other targets, and no ignored tests.
- Remaining: populations above 16,384 and inputs exceeding the 8 MiB decoded-row limit remain `Unknown`; broader fanout distributions, estimate overhead, general cost comparison, and target-host evidence remain open.

## ST-2 / OP-4 follow-up: extend authorized join-population estimates to 16,384 — implemented

- Contract: join planning may request exact authorized population counts through 16,384 rows, with the row-key fallback stopping after 16,385 candidates. Fresh committed population records remain eligible only under the full-visible policy proof and pinned snapshot; current-actor overlay deltas remain bounded to 16,384 row IDs. Exact join-pair estimation remains bounded at 16,384 rows per side and 8 MiB decoded row data.
- Red/green: planner IPC initially rejected the newly requested 16,384-row population limit. After widening the request bound, the optimizer-process regression exposed a second issue: a known 17×4,097 population with unavailable pair cardinality selected hash instead of retaining the measured indexed fallback. Planning now retains that deterministic index preference only when pair cardinality is `Unknown`; known high fanout still selects hash.
- Verification: direct transaction profiles returned exact counts for 4,097 and 16,384 visible rows and `Unknown` for 16,385. The planner IPC boundary and process-protocol regressions passed. A delegated confined SQL acceptance selected kind 17 for the 17×4,097 case, returned all 17 pairs, and overlapped 192 operational reads and 192 commits. Workspace build, format check, warning-denied Clippy, and the complete delegated serial workspace suite passed: 409 library tests, 206 commit tests, 3 metadata-process tests, 85 transaction tests, all other targets, and no ignored tests.
- Remaining: this expands bounded population orientation, not exact pair estimates or general cost comparison. Populations above 16,384, broader join distributions, estimate overhead, and target-host workload evidence remain open.

## Fresh delegated workspace verification — passed

- The earlier delegated `cargo test --workspace` unit remained blocked in the commit integration binary for over three hours. A fresh delegated run of the complete commit integration target passed all 206 tests in 153 seconds, including sandboxed SQL and optimizer cancellation.
- Stopped the stale unit and ran the full workspace suite in a new delegated `systemd --user` unit with the documented TinyCC artifacts/runtime and an isolated Cargo target. `cargo test --workspace --locked -- --test-threads=1` completed successfully in 4m43s. All Rust unit, integration, sandbox, and process tests ran; no test was skipped.

## ST-2 follow-up: use adjusted populations for constant predicates — verified slice

- Contract: `TRUE`, `FALSE`, and other supported row-independent constants derive exact matches from the same bounded authorized population estimate as planner scan choices. A fresh committed population record with current-actor insert/delete deltas can supply the count; stale/missing statistics fall back to the row-key probe. No row payloads are decoded for constant evaluation.
- Red/green: the constant-predicate regression first expected a private insert to yield `(3, 0)` for `FALSE` but received `Unknown`. After routing the constant path through the bounded population estimator, the private-overlay case passed. The follow-up committed a row after the stats record became stale and verified the row-key fallback returned the new count `(3, 0)` rather than the stale baseline.
- Verification: the focused regression passed, followed by all 85 transaction integration tests. The isolated locked workspace build, format check, and all-target Clippy passed. A later fresh delegated full workspace run passed; see the full verification entry above.

## ST-2 follow-up: adjust fresh population estimates for private writes — verified slice

- Contract: when a fresh exact committed population record and a literal full-visible policy proof establish the baseline, adjust only for the invocation's own bounded overlay rows using pinned row-key existence. Staged inserts add one visible row; updates retain the count; deletes subtract one. Other actors' writes remain invisible. The estimator examines at most 16,384 overlay row IDs and falls back to the bounded probe on limit or storage uncertainty.
- Red/green: the authorized population regression stores a fresh two-row population record, stages deletion of one row, and asks for the exact result at bound one. The previous empty-overlay-only reuse returned `Unknown`; bounded delta adjustment returns one. The existing insert, update, multi-row bound, and private-overlay estimate cases remain covered.
- Verification: the new red assertion first failed with `Unknown` at bound one, then the focused test and all 85 transaction integration tests passed. The isolated locked workspace build, format check, and all-target Clippy passed. A later fresh delegated full workspace run passed; see the full verification entry above.

## ST-2 follow-up: adjust indexed predicate counts for private overlay writes — verified slice

- Contract: a fresh committed population and an eligible readable leading index still provide the exact committed baseline. The estimator now adjusts that baseline only for bounded overlay entries authored by the current actor, using each affected row's pinned committed value and staged replacement/deletion. Other actors' writes remain outside the invocation view. Invalid or over-bound overlay values fall back to the bounded row estimator; cumulative old/staged payload bytes are capped at 16 MiB.
- Red/green: the regression adds a private insert matching the indexed predicate while an unrelated committed row has malformed tuple bytes. The existing empty-overlay-only fast path returned `Unknown`; the new path returns `(3,2)` without reading the unrelated row. The same regression checks a private update that removes the match and a private delete that removes both the match and visible row.
- Verification: the focused transaction regression passed, followed by all 85 transaction integration tests in an isolated locked Cargo target. A later fresh delegated full workspace run passed; see the full verification entry above.

## ST-2 / OP-4 follow-up: count exact indexed predicates without row decoding — implemented and profiled

- Contract: a simple equality/range predicate may use a direct pinned-snapshot index-entry count only when a fresh exact committed population record proves the full authorized population, row visibility is independent of row values, the overlay is empty, the index begins with the predicate column, and every index component is unconditionally readable. The bounded path returns no row values and does not debit executor scan work; all other cases retain the bounded row estimator.
- Red/green: extended the fresh-population regression with one valid matching indexed row and one malformed row outside the range. The old full-row estimator returned `Unknown`; the index-count path returns exact `(2, 1)`. A one-unit executor scan-work budget still permits the subsequent real index range query, proving estimate work is separate. Candidate/encoded-key bounds remain 1,024 and 16 MiB; unsupported and oversized cases fall back.
- Evidence: five fresh-database 2,052-row TEXT profiles at 882 matches measured indexed estimator medians of 1.537 ms (1.494–1.599 ms range), versus the earlier full-row estimator at 26.103 ms. This is direct transaction-layer evidence; planner IPC, SQL execution, end-to-end latency, and target-host profiling remain open.
- Validation: five benchmark invocations passed; all 85 transaction integration tests passed; maintained TinyCC SDK tests and sandbox fixture build passed; workspace build, format check, and warning-denied all-target Clippy passed; the full serial delegated-cgroup workspace suite passed with 409 library tests, 206 commit tests, 3 metadata-process tests, and all remaining targets passing with zero ignored tests.

## ST-2 / ST-6 follow-up: reject stale population reuse for constant predicates — verified

- The constant-predicate transaction regression now commits another row after writing the protected population record, advancing the engine sequence. On the new snapshot, the stale count is rejected and the estimate falls back to row evaluation; the deliberately malformed existing tuple makes that fallback return `Unknown` rather than reuse the old exact count.
- Verification: the focused `constant_predicate_reuses_fresh_population_without_reading_rows` transaction regression passed. `cargo fmt --all -- --check`, locked workspace build, warning-denied all-target Clippy, and the full serial delegated-cgroup workspace suite passed. The suite included 409 library tests, 206 commit integration tests, 3 metadata-process tests, 85 transaction integration tests, all remaining targets, and zero ignored tests.

## ST-2 / ST-6 follow-up: exercise constant estimates through planner IPC — verified slice

- The real planner-stream integration regression now sends a constant integer comparison through `EstimateScan` after committing a row and collecting a fresh protected population record. It verifies the IPC response contains the exact one-row visible population and zero matches, covering the service dispatch path for constant-predicate reuse.
- The existing transaction regression continues to cover literal `TRUE`, `FALSE`, `NULL`, `1 = 2`, `NULL IS NULL`, malformed stored row bytes, and private-write invalidation; this planner-stream addition checks IPC encoding, relation resolution, and response wiring on a nonempty committed table.
- Verification: the focused `planner_stream_estimate_allowance_falls_back_without_aborting_invocation` integration test passed. `cargo fmt --all -- --check`, locked workspace build, warning-denied all-target Clippy, and the full serial delegated-cgroup workspace suite passed. The full run included 409 library tests, 206 commit integration tests, 3 metadata-process tests, 85 transaction integration tests, all remaining targets, and zero ignored tests.

## ST-2 follow-up: reuse fresh populations for constant predicates — implemented slice

- The bounded predicate estimator now derives exact results for row-independent constant expressions, including literal `TRUE`, `FALSE`, `NULL`, `1 = 2`, and `NULL IS NULL`, from a fresh protected committed population count when the pinned authorization proves all rows visible, the population fits the request bound, and the invocation has no private writes. It avoids reading or decoding row values in this case; stale, unknown, over-bound, unsupported-policy, and overlay-bearing cases keep the bounded scan/fallback behavior.
- Red/green: `constant_predicate_reuses_fresh_population_without_reading_rows` first returned `Unknown` when the existing estimator tried to decode a deliberately malformed row tuple. It now returns exact constant-expression counts from the fresh population record. The regression covers `TRUE`, `FALSE`, `NULL`, an integer comparison, `NULL IS NULL`, and a private write that suppresses committed-stat reuse.
- Validation: the focused constant-predicate regression and all 85 transaction integration tests passed; `cargo fmt --all -- --check`, locked workspace build, warning-denied all-target Clippy, and the full serial delegated-cgroup workspace suite passed. The suite included 409 library tests, 206 commit tests, 3 metadata-process tests, 85 transaction tests, every remaining target, and zero ignored tests.

## ST-2 / OP-7 follow-up: profile minimum 9-row join ratio at the fanout cutoff — measured

- Contract: complement the measured 9×128 distribution with the minimum eligible indexed-side population for a nine-row outer: 9×72 (eight-to-one), producing exactly four pairs per outer row.
- Evidence: five fresh-database delegated-cgroup runs selected indexed nested-loop, returned all 36 rows equal to the hash reference, and overlapped 192 operational reads and 192 commits each. Median of per-run p50s was 174.5 ms indexed (168.0–185.6 ms range) and 186.8 ms hash (172.7–200.8 ms range).
- Scope: this adds evidence at the minimum population ratio and inclusive pair threshold for the smallest eligible outer. It is a local fixture, not a general cost model or target-host performance claim.
- Validation: all five confined profiles passed; maintained TinyCC SDK tests and sandbox query fixture build passed. `cargo fmt --all -- --check`, locked workspace build, warning-denied all-target Clippy, and the full serial delegated-cgroup workspace suite passed (409 library tests, 206 commit tests, 3 metadata-process tests, 84 transaction tests, all remaining targets, no ignored tests).

## ST-5 follow-up: invalidate cached join cardinality after an overlay write — covered

- Regression: the planner-stream estimate test now requests the same self-join cardinality twice, stages a new key between requests, and repeats it again. The exact result changes from 8 to 9 after the private write and remains 9 on the unchanged generation, proving cache reuse is scoped to the overlay generation. All three requests consume the existing shared estimate allowance.
- Validation: focused planner-stream regression, format check, locked workspace build, warning-denied all-target Clippy, and the complete serial delegated-cgroup workspace suite passed (409 library tests, 206 commit tests, 3 metadata-process tests, 84 transaction tests, all remaining targets, no ignored tests).

## ST-2 / OP-7 follow-up: verify the inclusive 17-row fanout boundary — covered

- Contract: for a 17-row outer input with an eligible indexed relation, the exact estimate selects indexed nested-loop at 68 pairs (four per outer row) and hash at 69 pairs.
- Regression: optimizer-service unit tests check the exact decision edge and one pair above it for every eligible outer size from 9 through 17 at that size's minimum population ratio. Confined benchmark modes use a 17×119 input and exact seven-to-one population ratio: one distributes 68 matches evenly, the other adds a 69th match to one key. Both validate the exact estimate, selected plan, every output against hash, and concurrent operational work.
- Evidence: five fresh-database delegated-cgroup runs per case each overlapped 192 operational reads and 192 commits. At 68 pairs, the median of per-run p50s was 250.3 ms indexed (248.5–266.2 ms range) and 292.3 ms hash (281.3–341.7 ms range). At 69 pairs, all five plans selected kind-4 hash and returned 69 rows equal to the hash reference.
- Scope: these local runs check both sides of the pair threshold at the population-ratio boundary; they do not establish a general cost model or target-host performance.
- Validation: both five-run confined profiles passed; maintained TinyCC SDK tests and sandbox fixture build passed. Formatting, locked workspace build, warning-denied all-target Clippy, and the complete serial delegated-cgroup workspace suite also passed (408 library tests, 206 commit tests, 3 metadata-process tests, 84 transaction tests, all remaining targets, no ignored tests).

## ST-2 follow-up: verify join-estimate byte limits — covered

- Contract: an authorized exact join estimate remains `Unknown` without partial counts when combined decoded row payload exceeds 8 MiB or an encoded join key exceeds 4 KiB.
- Regression: added a real-redb transaction test with nine wide rows crossing the combined row-byte bound and a second estimate using their oversized text keys. Both return `Unknown`.
- Validation: the focused transaction regression passed; `cargo fmt --all -- --check`, workspace build, workspace Clippy with warnings denied, SDK tests, and the serial full workspace suite in the delegated cgroup service all passed after this addition.

## ST-2 / OP-7 follow-up: verify hash fallback for hot-key fanout above threshold — measured

- Contract: complement the 16-match hot-key case with a 17×128 join where one outer key matches all 128 indexed rows and the other 16 are unmatched. The exact 128-pair estimate exceeds four pairs per outer row; the optimizer must retain kind-4 hash and preserve the full result.
- Evidence: five fresh-database delegated-cgroup runs selected hash, verified the 128 matching rows, and overlapped 192 operational reads and 192 commits per run. Median of per-run p50 hash latency was 341.9 ms (340.7–363.2 ms range).
- Scope: the pair estimate is skewed to one hot key; this verifies the bounded threshold's high side for one input shape only.
- Verification: the focused confined run and four-run repeat passed. `cargo fmt --all -- --check`, locked workspace build, warning-denied all-target Clippy, maintained TinyCC SDK tests, sandbox fixture rebuild, and the complete serial delegated-cgroup workspace suite passed (407 library tests, 206 commit tests, 3 metadata-process tests, 83 transaction tests, all remaining targets, no ignored tests).

## ST-2 / OP-7 follow-up: profile skewed join fanout — measured

- Contract: add a confined 17×128 join profile with one hot outer key matching 16 indexed rows and 16 unmatched outer keys. The real authorized pair estimate is 16, below the four-pairs-per-outer gate; kind 17 must preserve all result rows and equal the hash reference.
- Evidence: five fresh-database delegated-cgroup runs passed. Every run selected kind 17, verified 16 identical rows against hash, and overlapped 192 operational reads and 192 commits. The median of per-run p50 latencies was 257.8 ms indexed (247.8–334.4 ms range) and 299.1 ms hash (288.6–361.7 ms range).
- Scope: this checks one highly skewed key distribution and does not establish general cost-based selection or other hot-key/population shapes.
- Verification: the focused confined run and four-run repeat passed. `cargo fmt --all -- --check`, locked workspace build, warning-denied all-target Clippy, maintained TinyCC SDK tests, sandbox fixture rebuild, and the complete serial delegated-cgroup workspace suite passed (407 library tests, 206 commit tests, 3 metadata-process tests, 83 transaction tests, all remaining targets, no ignored tests).

## ST-2 / OP-7 follow-up: extend exact join estimates to 4,096 rows — implemented slice

- Contract: authorized join population probes and pair estimates now accept up to 4,096 rows per input. The estimator reads at most 4,097 row IDs per side, retains the 8 MiB combined decoded-row cap, 4 KiB encoded-key cap, eight-key-component limit, value-independent visibility and readable-key rules, and the shared planner request allowance. Populations beyond the requested bound remain `Unknown` without partial counts.
- Red/green: the real-redb duplicate-key regression first returned `Unknown` for the 4,096×4,096 bound. After increasing both planner and estimate limits, it returns the same exact 1,043,463 pairs as the 1,024-row baseline; a 2,048-row requested bound still returns `Unknown`. A planner IPC integration regression sends a 4,096-row exact join request and decodes the 16,777,216-pair response. The optimizer process protocol regression confirms it requests the expanded bound.
- Remaining: broader join distributions and population shapes, cost-based path comparison, populations above 4,096, richer statistics, broader global-statistics consumption under an explicit disclosure policy, and target-host evidence remain open.
- Validation: the real-redb transaction regression, 4,096-row planner IPC response, optimizer-process bound request, and both population/join request codec boundary regressions passed. `cargo fmt --all -- --check`, locked workspace build, and warning-denied all-target Clippy passed. The complete serial delegated-cgroup workspace suite passed with 407 library tests, 206 commit integration tests, 3 metadata-process tests, 83 transaction tests, all remaining targets, and no ignored tests.

## ST-6 follow-up: reuse a fresh committed population inside the authorized planner estimate — implemented slice

- Contract: a committed count can answer the existing bounded population estimate only from the invocation's own pinned redb snapshot, when literal policy shape proves the full table is visible and the private row overlay is empty. The record must be exact and within the requested maximum. Missing, stale, malformed, over-bound, unknown, or overlay-bearing cases fall back to the bounded snapshot estimate or return `Unknown`; raw records never cross IPC.
- Red/green: the pinned-snapshot freshness regression initially failed because the snapshot had no trusted committed-population reader. It now verifies that a retained old snapshot continues to match its exact record after a later engine commit, while a new snapshot rejects that stale record. The planner-stream allowance regression collects a fresh exact record, uses it through the authorized estimate API, and then verifies that private staged rows produce the updated count.
- Validation: the pinned-snapshot record regression and real planner-stream estimate regression passed. `cargo fmt --all -- --check`, locked workspace build, and warning-denied all-target Clippy passed. The complete serial delegated-cgroup workspace suite passed with 407 library tests, 206 commit integration tests, 3 metadata-process tests, 83 transaction tests, all remaining targets, and no ignored tests.
- Remaining: broader join distributions and population shapes, cost-based path comparison, populations above 2,048, richer statistics, broader global-statistics consumption under an explicit disclosure policy, and target-host evidence remain open.

## ST-2 / OP-7 follow-up: profile intermediate join fanouts two and three — measured

- Contract: the maintained confined benchmark accepts 9×128 joins with two or three matches per outer row, asserts the exact pair estimate and kind-17 indexed plan, and compares against equivalent hash results under operational load.
- Evidence: five fresh-database delegated-cgroup runs per fanout each overlapped 192 operational reads and commits. At fanout two, indexed median-of-run p50 was 182.7 ms (169.6–193.7 ms range), compared with 309.4 ms for hash (278.4–321.7 ms range). At fanout three, indexed was 181.5 ms (173.7–194.0 ms range), compared with 301.9 ms for hash (288.9–312.5 ms range). Together with prior fanout-one, four, and five evidence, all five tested integer fanouts now cover the current cutoff and its adjacent hash case.
- Validation: ten confined runs passed, including plan-kind, result-equivalence, and operational-load assertions; the TinyCC sandbox query fixture rebuilt with its constrained `$ORIGIN` runpath.
- Remaining: broader key distributions, outer/indexed population shapes, general cost comparison, populations above 2,048, global-statistics consumption, and target-host evidence remain open.

## ST-2 / OP-7 follow-up: extend exact join estimates to 2,048 rows — implemented

- Contract: authorized join population probes and exact pair estimates now accept up to 2,048 rows per input. Candidate row reads remain bounded to 2,049 per population; pair estimation retains the 8 MiB combined decoded-row cap, 4 KiB per encoded key, value-independent visibility requirement, statically readable-key requirement, and shared estimate allowance. Results above the requested bound remain `Unknown` without partial counts.
- Red/green: the real-redb transaction regression first returned `Unknown` for a 2,048×2,048 join with NULL-only added keys. It now returns the same exact 1,043,463 pairs as the 1,024-row baseline; a smaller requested bound still returns `Unknown`. A planner-process regression verifies a 17×2,048 low-fanout join receives the expanded estimate request and chooses indexed nested-loop.
- Scope: the estimate ceiling is higher; fanout rules and pair-per-outer threshold are unchanged. Populations above 2,048, broader fanout/distribution profiles, general cost comparison, global-statistics consumption, and target-host evidence remain open.
- Validation: focused real-redb transaction and optimizer-process regressions passed. `cargo fmt --all -- --check`, locked workspace build, warning-denied all-target Clippy, and the full serial delegated-cgroup workspace suite passed. The suite reported 406 library tests, 206 commit integration tests, 3 metadata-process tests, 83 transaction tests, all other targets, and no ignored tests. The sandbox runtime allowlist included the pinned loader, libc, and `libgcc_s.so.1` required by the executor binary.

# ST-2 / OP-7 follow-up: profile 17×2,048 indexed nested-loop input — measured

- Harness: added `PRDB_TEST_INDEXED_JOIN_FANOUT17_2048_BENCHMARK_ONLY` to the confined SQL acceptance. A 17-row outer probes a 2,048-row indexed relation with one match per outer key. It verifies the authorized exact pair count, selected kind-17 plan, 17 output pairs and checksum, while 192 operational reads and 192 commits overlap the query.
- Evidence: five fresh-database delegated-cgroup runs passed. The two indexed samples per run produced a median of per-run p50 latencies of 280.3 ms (273.1–310.6 ms range). The case records indexed execution only because the SDK hash join is bounded to 128 materialized rows; existing 17×128 and other supported profiles continue to compare equivalent hash and indexed outputs.
- Contract: the expanded profile exercises the already specified 4,096-row population-estimate cap and 17-row low-fanout rule; it adds evidence without changing optimizer selection. It is host-local evidence and does not establish deployment performance or a general cost model.
- Verification: the maintained TinyCC sandbox fixture build and five-run confined mixed-load profile passed. The preserved 17×119 indexed/hash profile passed. `cargo fmt --all -- --check`, locked workspace build, warning-denied Clippy, TinyCC SDK tests, and the full serial delegated-cgroup workspace suite passed (407 library tests, 206 commit tests, 3 metadata-process tests, 83 transaction tests, remaining targets, no ignored tests).

## ST-2 / OP-7 follow-up: exercise the 4,096-row join-estimate cap end to end — measured

- Harness: added `PRDB_TEST_INDEXED_JOIN_FANOUT17_4096_BENCHMARK_ONLY`. A 17-row outer probes a 4,096-row indexed relation with one matching row per outer key. The confined test verifies the exact authorized pair estimate, kind-17 plan, all 17 joined outputs and checksum while 192 operational reads and commits overlap. Marker `E` is unique among the existing fixture modes; an initial full-suite run caught a collision with the UPDATE stage-failure mode before the marker was changed.
- Evidence: five fresh-database delegated-cgroup runs passed. Two indexed samples per run produced a median of per-run p50 latencies of 308.1 ms (297.1–342.0 ms range). This cap-boundary profile records only index nested-loop because the SDK hash join supports at most 128 materialized rows.
- Scope: this validates the existing 4,096-row estimate cap end to end; it does not raise the cap or establish performance beyond it. Broader cost comparisons and target-host repeats remain open.
- Verification: TinyCC sandbox fixture build, all five focused confined runs, `cargo fmt --all -- --check`, locked workspace build, warning-denied Clippy, TinyCC SDK tests, and the complete serial delegated-cgroup workspace suite passed (407 library tests, 206 commit tests, 3 metadata-process tests, 83 transaction tests, all other targets, no ignored tests). The full suite initially caught the marker collision; the rerun passed after using the unique marker.

## ST-2 / OP-7 follow-up: preserve indexed fallback above the estimate cap — measured

- Harness: added `PRDB_TEST_INDEXED_JOIN_FANOUT17_4097_UNKNOWN_BENCHMARK_ONLY`. A real planner transaction verifies the 4,097-row join estimate is `Unknown` with no partial count. The confined query still selects kind 17 through the deterministic index fallback and returns all 17 expected pairs from a 4,097-row indexed input.
- Red/green: the first focused run failed because the fixture incorrectly expected an exact count. After asserting `None` at 4,097, the confined query passed. Five fresh-database delegated-cgroup runs each overlapped 192 operational reads and commits; the median of per-run p50 indexed latency was 256.5 ms (223.1–270.9 ms range).
- Scope: this verifies only the first over-cap population and the existing fallback. The hash join comparison remains unavailable for inputs above 128 materialized rows; populations substantially above the cap and broader workload comparisons remain open.
- Verification: TinyCC sandbox fixture build, all five focused confined runs, `cargo fmt --all -- --check`, locked workspace build, warning-denied Clippy, TinyCC SDK tests, and the complete serial delegated-cgroup workspace suite passed (407 library tests, 206 commit tests, 3 metadata-process tests, 83 transaction tests, all other targets, no ignored tests).

## ST-2 / OP-7 follow-up: validate the exact 17×119 skew boundary — measured

- Contract: exercise the current 17-row outer / 7× indexed-side eligibility boundary through the full confined SQL path, including the authorized exact pair estimate, selected plan, indexed execution, hash reference result, durable outcome, and concurrent operational workload.
- Acceptance: a new maintained benchmark mode seeds 17 outer rows and exactly 119 indexed rows with one match per outer row; it verifies 17 equivalent output rows from kind-17 indexed nested-loop and hash execution.
- Evidence: five fresh-database delegated-cgroup runs each overlapped 192 operational reads and commits. Indexed run-level p50 median was 259.6 ms (242.7–270.9 ms range); hash median was 313.8 ms (296.1–334.0 ms range). This is local evidence for the exact boundary shape, not a deployment performance claim.
- Validation: five exact-boundary confined runs passed, as did the default 42-second confined acceptance. The full serial delegated-cgroup workspace suite passed with 406 library tests, 206 commit tests, 3 metadata-process tests, 83 transaction tests, all other targets, and no ignored tests. Workspace build, format check, warning-denied all-target Clippy, TinyCC SDK tests, sandbox fixture, and release-profile verification passed. A first full-suite attempt caught a fixture parameter collision with the existing redacted-count branch; the benchmark now uses a distinct lowercase `z` marker, and both that default acceptance and the final full suite pass.
- Remaining: generalized cost comparison, broader fanout/distribution profiles, populations above 1,024, global-statistics consumption, and target-host evidence remain open.

## ST-2 / OP-7 follow-up: extend the low-fanout boundary to 17 outer rows — measured

- Contract: exact authorized pair counts may orient indexed nested-loop when the smaller outer side has 9–16 rows and the indexed side is at least eight times larger, or it has 17 rows and the indexed side is at least seven times larger, and output is at most four pairs per outer row. Unknown estimates and higher fanout keep the existing fallback/hash choices.
- Red/green: optimizer tests cover a 17-row outer at fanout one and five, and process eligibility tests cover the 17×119 ratio boundary, reversed orientation, the 18-row cap, and unknown populations.
- Evidence: five fresh-database confined 17×128 runs each overlapped 192 operational reads and 192 commits. Indexed run-level p50s had median 257.3 ms (243.1–270.0 ms range); hash run-level p50s had median 298.9 ms (295.8–311.2 ms range). Both plans produced equivalent 17-pair results. This is host-local evidence for the measured shape only.
- Validation: five repetitions of the confined acceptance passed in a delegated `cpu memory pids` cgroup; optimizer unit/process regressions and maintained TinyCC SDK tests passed. `cargo fmt --all -- --check`, locked workspace build, warning-denied all-target Clippy, maintained TinyCC tests, sandbox fixture, release-profile validation, and the full serial delegated-cgroup workspace suite passed. The suite reported 406 library tests, 206 commit tests, 3 metadata-process tests, and 83 transaction tests, with all other targets passing and no ignored tests.
- Remaining: generalized cost comparison, broader fanout/distribution profiles, populations above 1,024, global-statistics consumption, and target-host evidence remain open.

## ST-2 / OP-4 follow-up: DECIMAL predicate literals — implemented slice

- Contract: optimizer request and plan predicate tag `0x08` carries a signed big-endian i128 coefficient. The compared DECIMAL column supplies precision and scale; invalid precision overflow and non-DECIMAL comparisons are rejected. The trusted estimator compares authorized coefficients under that same schema context.
- Coverage: bounded request decode, binding and plan precision/type checks, planner IPC round-trip, exact real-redb estimate, and TinyCC request encoding/filter decoding cover this type.
- Verification: focused DECIMAL Rust tests, formatting, locked workspace build, warning-denied all-target Clippy, maintained TinyCC SDK tests, sandbox query fixture build, and the full serial delegated-cgroup workspace suite passed. The suite reported 404 library tests, 206 commit tests, 3 metadata-process tests, and 83 transaction tests, with all other targets passing and no ignored tests.
- Remaining: cross-type coercions, broader candidate comparison, populations above 1,024, global-statistics consumption, and target-host workload evidence remain open.

## ST-2 / OP-4 follow-up: BYTEA predicate literals — implemented slice

- Contract: optimizer request and plan predicate tag `0x07` carries a big-endian u16 length and at most 4,096 raw bytes. Binding requires a BYTEA operand; exact authorized estimation compares only readable BYTEA cells within the same per-value limit.
- Coverage: bounded request decoding, plan round-trip, binding compatibility, planner IPC round-trip, exact real-redb estimate, and TinyCC parameter encoding and filter decoding cover this type.
- Verification: focused Rust regressions, formatting, locked workspace build, warning-denied all-target Clippy, maintained TinyCC SDK tests, sandbox query fixture build, and the full delegated-cgroup suite passed. The suite reported 400 library tests, 206 commit tests, 3 metadata-process tests, and 83 transaction tests, with all other targets passing and no ignored tests.
- Remaining: cross-type coercions, broader candidate comparison, populations above 1,024, global-statistics consumption, and target-host workload evidence remain open.

## ST-2 / OP-4 follow-up: TIMESTAMPTZ predicate literals — implemented slice

- Contract: optimizer request and plan predicate tag `0x06` carries signed big-endian i64 microseconds since epoch, restricted to the supported timestamp range. Binding requires a TIMESTAMPTZ operand; authorized estimation compares the literal with readable timestamp cells.
- Coverage: request validation, binding, Rust plan codec and point-index eligibility, planner IPC round-trip, real-redb exact estimate, and TinyCC request encoding/plan decoding cover this type.
- Verification: focused Rust regressions, formatting, locked workspace build, warning-denied all-target Clippy, maintained TinyCC SDK tests, sandbox query fixture build, and the full delegated-cgroup suite passed. The suite reported 397 library tests, 206 commit tests, 3 metadata-process tests, and 83 transaction tests, with all other targets passing and no ignored tests.
- Remaining: cross-type coercions, broader candidate comparison, populations above 1,024, global-statistics consumption, and target-host workload evidence remain open.

## ST-2 / OP-4 follow-up: UUID predicate literals — implemented slice

- Contract: optimizer request and plan predicate tag `0x05` carries the 16 canonical UUID bytes. Binding requires a UUID operand; authorized estimation compares UUID literals with readable UUID cells. Eligible single-column UUID equality and range predicates may use the existing authorized index paths.
- Red/green: the request decoder regression first rejected tag `0x05` with `InvalidPredicate`; it now checks all 16 bytes. A real-redb predicate estimate returns the exact `(3 visible, 1 matching)` count for a UUID literal. Rust planner-IPC codec coverage and TinyCC request/plan fixtures cover the cross-language wire shapes.
- Verification: focused Rust regressions, formatting, locked workspace build, warning-denied all-target Clippy, maintained TinyCC SDK tests, sandbox query fixture build, and the full serial delegated-cgroup workspace suite passed.
- Remaining: cross-type coercions, broader candidate comparison, populations above 1,024, global-statistics consumption, and target-host workload evidence remain open.

## ST-2 / OP-4 follow-up: DATE predicate literals — implemented slice

- Contract: optimizer request and plan predicate tag `0x04` carries a signed big-endian i32 DATE day count, restricted to the DATE range supported by the tuple type. The binder requires a DATE operand; typed estimation compares it with authorized DATE values.
- Implementation: added request decoding, binding, canonical plan encode/decode and validation, trusted estimate evaluation, TinyCC request encoding, DATE-aware SDK predicate decoding/evaluation, and DATE unique/range hint conversion.
- Red/green: the new request decoder regression first rejected tag `0x04` with `InvalidPredicate`; it now accepts a valid signed day count and rejects out-of-range values. Real-redb estimate coverage verifies an exact `(3 visible, 1 matching)` result for a DATE literal. Rust plan-wire tests and TinyCC request/plan fixture checks cover the encoding.
- Verification: focused request/plan/transaction regressions passed; the maintained TinyCC SDK suite passed; formatting, locked workspace build, warning-denied all-target Clippy, and sandbox query fixture passed. The full delegated-cgroup serial workspace suite passed with 391 library tests, 206 commit tests, 3 metadata-process tests, 83 transaction tests, all other targets, and zero ignored tests.
- Remaining: DECIMAL, BYTEA, and TIMESTAMPTZ literals, cross-type coercions, broader candidate comparison, populations above 1,024, global-statistics consumption, and target-host workload evidence remain open.

## ST-2 / OP-7 follow-up: profile the 16-row low-fanout boundary — measured

- Harness: added `PRDB_TEST_INDEXED_JOIN_FANOUT16_BENCHMARK_ONLY` to the maintained confined SQL acceptance. It seeds 16 outer rows and a 128-row indexed input with one match per outer row, checks 16 equivalent pairs and the indexed plan, and runs the same operational read/commit load as the 9-row fanout profiles.
- Evidence: five fresh-database delegated-cgroup runs each completed 192 operational reads and 192 commits. Indexed median-of-run p50 was 243.5 ms (230.9–259.1 ms range); hash was 305.7 ms (289.4–313.7 ms range). This extends the local low-fanout observation to 16 outer rows; it does not establish deployment performance or a general cost model.
- Verification: the five focused confined acceptance runs passed. The maintained TinyCC fixture was rebuilt with the SDK sandbox-query-fixture target; Rust formatting, workspace build, warning-denied Clippy, and `git diff --check` passed.

## ST-2 / OP-4 follow-up: extend bounded join estimates to 1,024 rows — implemented

- Contract: authorized join population probes and exact INNER JOIN pair estimates now accept a requested maximum of 1,024 rows per input. Candidate row reads remain bounded to 1,025 per population; pair estimation retains the 8 MiB combined decoded-row cap, 4 KiB per encoded key, value-independent visibility requirement, readable-key requirement, and shared estimate allowance. Results above the requested bound remain `Unknown` without partial counts.
- Red/green: the real-redb transaction regression first returned `Unknown` for a 1,024×1,024 duplicated-key join. It now verifies 1,043,463 exact pairs at the boundary, `Unknown` for a requested 512-row bound, and `Unknown` when the actual input population exceeds 1,024. The planner-stream regression stages 1,024 rows and verifies an exact 1,048,576-pair estimate using the 1,024-row IPC request bound.
- Scope: this raises the bounded estimator ceiling; it does not change optimizer strategy thresholds or claim that index nested-loop is cheaper outside the measured shapes. Populations above 1,024, broader cost comparison, global-statistics consumption, and target-host profiles remain open.
- Verification: focused transaction and real planner-stream regressions passed. Rust formatting, locked workspace build, warning-denied Clippy, maintained TinyCC SDK tests, and the sandbox query fixture build passed. The writable delegated-cgroup preflight passed, and the full serial delegated-cgroup workspace suite passed: 389 library tests, 206 commit integration tests, 3 metadata-process tests, 83 transaction tests, and all other targets, with no ignored tests.

## ST-2 / OP-4 follow-up: exact same-typed scalar column estimates — implementation

- Contract: the bounded estimator compares authorized columns when both operands have the identical scalar type: BOOLEAN, SMALLINT, INTEGER, BIGINT, DECIMAL, TEXT, VARCHAR, BYTEA, UUID, DATE, or TIMESTAMPTZ. DECIMAL precision/scale must match; TEXT, VARCHAR, and BYTEA values are capped at 4 KiB each. Boolean, integer, DATE, UUID, TIMESTAMPTZ, BYTEA, and text literals are supported; cross-type coercions remain unsupported.
- Red/green: the real-redb regression first failed with `Unknown` on the BYTEA pair. It now checks exact equality and inequality counts for all eleven scalar types, with duplicate and SQL NULL cases, and verifies the 4 KiB variable-value fallback.
- Implementation: the trusted evaluator now mirrors same-type column binding and existing scalar comparison order for these types while retaining authorized projections, candidate/row-byte bounds, policy restrictions, cache invalidation, and the shared estimate allowance.
- Verification: the all-type transaction regression covers equality, inequality, and ordering; a real `serve_planner` stream regression confirms a DECIMAL column predicate returns exact counts over planner IPC. Both focused tests passed. Format, warning-denied workspace Clippy, locked build, and the complete delegated-cgroup suite passed for the evaluator change; the suite ran before the additional planner-stream regression was added.
- Remaining: additional literal encodings/coercions, broader candidate comparison, and target-host workload evidence remain open.

## ST-2 / OP-4 / SQL-10 follow-up: composite index-prefix nested-loop joins — implemented

- Contract: when a composite inner index has a contiguous leading prefix of two to eight columns and every prefix column participates in a join equality, the optimizer selects the longest matching prefix and emits plan kind 18. The SDK validates each key against the parsed join, skips probes containing NULL, sends all prefix values in index order through executor `ScanIndexPrefix`, and checks remaining equalities as residual predicates. Storage revalidates authorization and index shape, reconciles the snapshot with private writes, and returns bounded sanitized rows.
- Implementation: added the planner kind-18 representation and validation, executor IPC operation `0x1116`, storage prefix scanning, SDK decoding/execution, and Rust/C regressions. The executor response reuses the bounded index-range row response shape.
- Verification: the longest-prefix optimizer unit test, real-redb snapshot/overlay prefix regression, IPC operation/allowlist regression, maintained TinyCC SDK unit suite, sandbox fixture build, workspace build, format check, warning-denied Clippy, and release-profile validation passed. The complete serial delegated-cgroup workspace suite passed with 389 engine library tests, 205 commit tests, 3 metadata-process tests, 81 transaction tests, and all remaining targets. The confined `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed with the kind-18 plan assertion and exact result count, including the eight-worker operational saturation interval. Release-profile validation covered all 10 engine binaries and the query fixture. The acceptance measured 2,949 operational reads/commits during the 30-second interval.
- Remaining: the optimizer does not consume the collected global row-population records; adding that consumer needs a reviewed trust and disclosure contract. Statistics collection currently stores row population only. Join population probes now cover up to 512 rows per relation. General cost comparison and target-host/mixed-workload evidence remain open.

## ST-2 / OP-4 follow-up: exact join estimates extended to 512 rows — implemented slice

- Contract: exact authorized population probes and exact join-pair estimates may cover at most 512 rows per relation. The pair estimator still requires value-independent row visibility and statically readable join keys, includes private overlay rows, and returns `Unknown` without partial output when candidates exceed the requested bound, a key exceeds 4 KiB, decoded input exceeds 8 MiB, or the shared estimate allowance is exhausted.
- Red/green: the real-redb transaction regression first failed because a 512-row estimate returned `Unknown` at the former 128-row cap. It now verifies 131 rows on each side, duplicate-key SQL multiplicity of 16,513 pairs, and `Unknown` when the requested bound is 513.
- Implementation: raised the trusted estimator and planner population request bounds to 512; the wire retains the existing `u16` encoding and role-restricted planner operation. The optimizer's measured join orientation thresholds are unchanged.
- Verification: the real-redb regression passes for an exact 16,513-pair result at 131 rows per side and for `Unknown` at 513 rows; planner IPC codec and real `serve_planner` request/response regressions pass at a 512-row bound. The locked release harness measured the storage join-estimate call at 1.222 ms median and 1.658 ms p95 over 11 runs for two 512-row INTEGER relations (4,096 exact pairs). The measurement excludes planner IPC, SQL planning, and execution. Full workspace validation remains required.
- Remaining: the optimizer does not consume collected global row-population records; collection stores row population only. Cost comparison, populations above 512, and target-host/mixed-workload evidence remain open.

## ST-2 / OP-4 follow-up: bounded authorized join fanout estimate — implementation

- Contract: for a join whose authorized populations are each at most 128, storage may return exact matching pair count for one-to-eight key columns only when row visibility is value-independent and key cells are unconditionally readable. NULL-containing keys do not match. Decoded rows are capped at 8 MiB and each encoded composite key at 4 KiB; unsupported, redacted, oversized, incomplete, or exhausted cases return `Unknown` without partial results.
- Implementation: the planner-only `EstimateScan` protocol carries the qualified relation names, paired key-column IDs, and row bound. Storage re-resolves both relations against the pinned invocation context, includes staged private writes, and uses a planner-session cache invalidated by every private write. Each request consumes the shared estimate allowance. `Unknown` results are not cached.
- Optimizer: when one side has 9–16 rows and the other is at least eight times larger, an exact fanout of at most four pairs per outer row permits an eligible indexed nested-loop plan; higher fanout keeps hash join. Existing small-input and measured eight-row/16:1 rules retain precedence. Tests cover storage duplicate fanout, NULL non-matches, overlay writes, unreadable keys, population cap, bounded IPC codec, planner process request/response, and optimizer decisions.
- Validation: focused storage, codec, optimizer-service, and planner-process tests cover the estimator and decision. The real confined acceptance exposed that relation-scoped projections have distinct scopes within the same planner invocation; estimator validation now checks the shared schema version, principal and roles without incorrectly requiring those relation scopes to be identical. The real-storage assertion confirms exact pair counts for fanout one, four, and five.
- Mixed-load evidence: five fresh-database runs at 9×128 and one matching row per outer row measured indexed median-of-run p50 at 180.1 ms (173.1–193.5 ms range) and hash at 295.6 ms (289.7–308.5 ms range). At four matches per outer row, indexed was 183.7 ms (174.7–187.8 ms) versus hash at 307.4 ms (277.1–318.0 ms). At five matches per outer row, kind 4 hash was selected in all five runs. Each benchmark run completed 192 operational reads and 192 commits. These measurements support the local four-pairs-per-outer-row heuristic; they are not deployment targets.
- Verification: locked workspace build, `cargo fmt --all -- --check`, warning-denied workspace Clippy, TinyCC `sandbox-query-fixture`, focused fanout 4/5 acceptance, the normal 41.9-second confined SQL acceptance, and the complete serial delegated-cgroup workspace suite passed (387 library tests, 205 commit tests, 3 metadata-process tests, 80 transaction tests, and all remaining targets; no ignored tests).
- Remaining: global-statistics consumption, populations above 128, target-host profiles, and general cost-based selection remain open.

## ST-2 contract synchronization across storage and planner IPC specs — verified

- Correction: updated [storage manager](../../specs/storage-manager.md) and [IPC protocol](../../specs/ipc-protocol.md) to reflect that filtered ordinary unordered SELECT uses only exact zero-match predicate estimates for the no-scan hint, and that planner relation descriptions may carry both approved unique and non-unique range paths.
- Verification: checked the wire markers and response shape against `planner_ipc.rs`; `git diff --check` passed. Documentation-only change; no runtime checks were needed.

## ST-3 follow-up: hidden values and invisible candidates preserve estimate outputs — verified

- Contract: changing a cell that remains redacted to the estimator must not change exact counts. A value-dependent row policy or additional invisible candidate rows must yield the same `Unknown` estimate when the authorized reference view is unchanged.
- Regression: `authorized_scan_estimate_counts_sanitized_cells_and_private_writes` replaces only a redacted text cell while retaining its readable predicate value, then confirms `(visible_rows, matching_rows)` and the sanitized summary remain unchanged. It also compares predicate-estimate outcomes under the same value-dependent row policy with one versus eight staged invisible candidates; both return `Unknown`, while independent authorized scans remain equal.
- Verification: the focused real-redb transaction regression passed after both the hidden-cell and invisible-candidate assertions (`1 passed, 0 failed`).
- Scope: this checks content invariance, not timing/resource noninterference.

## ST-2 optimizer use of exact predicate estimates — implemented slice

- Contract: filtered ordinary unordered single-table SELECT sends its bound predicate through the planner-only EstimateScan operation. The optimizer adds the existing empty-view hint only when storage returns an exact zero-match count; nonzero and `Unknown` responses preserve the normal predicate plan.
- Implementation: the optimizer uses the plan's already-bound predicate and the original qualified relation names, consumes one shared planner estimate allowance, validates the response operation/request ID, and falls back on `Unknown`. Ordered SELECT, DML, joins, and aggregates do not issue this request.
- Red/green: the new process protocol regression first failed because the plan lacked `empty_view_hint`; it passes after dispatch was added. Separate process checks confirm exact nonzero matches and `Unknown` preserve the range plan and do not emit the no-scan hint.
- Specification: updated [statistics](../../specs/statistics-service.md), [planner/compiler](../../specs/planner-compiler.md), and this implementation plan to scope the use to exact zero-match results for filtered ordinary SELECT.
- Verification: all 15 optimizer process tests passed. Workspace build, formatting, warning-denied Clippy, maintained TinyCC SDK tests and sandbox fixture, and release-profile validation (10 binaries and 1 query fixture) passed. The delegated cgroup probe reported a writable parent with `cpu memory pids`; the full serial workspace suite passed with 221 library tests, 150 commit tests, 2 metadata-process tests, and 75 transaction tests, plus all other targets, with zero ignored tests. The first full-suite attempt exposed a missing `libgcc_s.so.1` in the supplied runtime allowlist; after adding the pinned Nix path, all sandbox/process tests passed. `git diff --check` passed.
- Remaining: broader selectivity/cost choices, larger populations, non-SELECT estimates, cache validity, planner task scheduling, and maintenance collection remain open.

## ST-2 follow-up: bounded authorized predicate estimator and planner request — implemented

- Contract: the trusted transaction API estimates exact matches only for an exact authorized population of at most four rows. It validates bound column identities and types, evaluates bounded comparison/Boolean predicates over sanitized cells, applies SQL three-valued NULL semantics, and returns `(visible_rows, matching_rows)` without row values. Planner IPC exposes the request through the planner-only EstimateScan operation's qualified-relation predicate extension.
- Fallbacks: redacted predicate cells, unsupported scalar types or predicates, value-dependent row policies, invalid projections, oversized populations/overlays, and incomplete row sets return `Unknown` without partial counts. Private inserts, replacements, and deletes participate in the estimate. Predicate values are never read from redacted cells.
- Regression: the real-redb transaction fixture verifies equality selectivity over visible text, NULL non-matching behavior, redacted-column fallback, the four-row work bound, and private overlay changes.
- Regression: the focused transaction test verifies authorized selectivity, NULL, redacted-cell fallback, bounds, and private writes. The planner stream integration verifies predicate request encoding/dispatch, a versioned exact response, and shared allowance accounting. Its first nonempty case returned `Unknown` because the fixture omitted the SELECT cell policy; adding that policy produced the intended exact count. Codec tests cover malformed requests and exact/unknown response forms.
- Verification: focused codec and real planner-stream regressions passed. Workspace build, formatting, warning-denied Clippy, and the full serial delegated-cgroup suite passed with 198 library tests, 149 commit tests, and 75 transaction tests; all sandbox and process tests ran with zero ignored.
- Remaining: optimizer request scheduling and using predicate selectivity to alter a plan remain open; broader predicate support, cache validity, task draining, and maintenance collection also remain open.

## ST-2 / OP-4: bounded population probe contract synchronized — documented

- Contract: ordinary unordered single-table SELECT may use a separate estimate request to resolve only populations of 0–4 rows. The probe reads at most five row keys, does not decode row values, and returns `Unknown` for larger populations, data-dependent policies, oversized overlays, unsupported metadata, or exhausted allowance. It shares the cumulative planner estimate allowance with the scoped scan summary.
- Planner use: exact zero can drive the validated empty-view hint; counts 1–4 can choose a sequential scan over a point/range path under the locally measured heuristic. The planner estimate does not grant execution authority; SDK and storage checks remain definitive.
- Changed contracts: [statistics service](../../specs/statistics-service.md), [storage manager](../../specs/storage-manager.md), and [statistics plan](README.md) now describe the implemented probe alongside the original bounded scan summary. [Planner/compiler](../planner-compiler/README.md) and its specification already record the implementation details and host-local timing evidence.
- Verification evidence: the population-probe implementation had passed the serial delegated-cgroup workspace suite, formatting, warning-denied Clippy, and focused hidden-cell invariance regression in the preceding increments. This documentation-only synchronization was checked with `git diff --check` and link/wording review; runtime tests were not rerun.
- Remaining: optimizer scheduling/use of predicate estimates, broader policy-filtered estimates, task draining, cache validity, and maintenance collection remain open.

## ST-2 / WI-7: repeatable authorized-estimate cost harness — measured

- Artifact: [`statistics_estimate.rs`](../../engine/examples/statistics_estimate.rs) seeds a temporary READY redb database with a deterministic 8,192-row INTEGER relation, constructs an explicitly authorized projection, runs 11 estimates over fresh invocation snapshots, verifies each exact summary, and reports median/p95 call latency. Bootstrap and seed writes are outside the timed interval.
- Measurement: on Rust 1.95.0, redb 4.4.0, x86_64 Linux, release build: median 8.655 ms and p95 12.073 ms. The run measured the storage estimate API only; planner IPC, SQL planning, compilation, and query execution were excluded. This is a single host-local sample, not a performance target.
- Verification: `cargo run --release -p prdb-engine --example statistics_estimate --locked --offline` completed and verified the row/cell counts. Locked workspace build, format check, warning-denied Clippy, serial workspace tests (129 passed, 3 cgroup-dependent ignored), and maintained TinyCC SDK suite passed.
- Remaining: broader estimate-cost variation, plan-quality evidence, mixed-workload benchmarks, cache validity, and maintenance collection remain open.

## ST-2 / WI-7: estimate-cost scaling sample — measured

- Harness: [`statistics_estimate.rs`](../../engine/examples/statistics_estimate.rs) now seeds three authorized INTEGER relations with 1,024, 8,192, and 16,383 rows, then runs 11 exact authorized estimates per relation. Setup and seeding remain outside each timed call; all returned counts are checked.
- Measurement: locked Rust 1.95.0 / redb 4.4.0 on x86_64 Linux. Medians were 0.891 ms, 7.152 ms, and 14.653 ms respectively; p95 values were 1.401 ms, 8.126 ms, and 16.754 ms. The largest relation is one row below the 16,384 candidate budget.
- Verification: `cargo run --release -p prdb-engine --example statistics_estimate --locked --offline`, workspace formatting check, and warning-denied Clippy across all targets passed in the locked Nix shell.
- Scope: one host-local sample per size; OS cache state is uncontrolled. This is scaling evidence for the storage estimate call, not planner, query, service-level, or target-hardware performance.

## ST-4: candidate-budget boundary publishes no partial estimate — verified

- Contract: authorized scan estimates may return an exact summary below the 16,384-candidate budget, but must return `Unknown` once the budget is reached, without exposing partial counts.
- Regression: a real-redb fixture commits 16,383 readable rows and verifies their exact VALUE population; it then commits the 16,384th row and verifies a fresh snapshot receives `Unknown`.
- Verification: the focused boundary regression passed. Locked workspace build, formatting, warning-denied Clippy, serial workspace tests (129 passed, 3 cgroup-dependent ignored), and maintained TinyCC SDK suite passed in the configured Nix shell.
- Remaining: asynchronous planner-task draining, cache validity, bounded maintenance collection, and mixed-workload evidence remain open.

## ST-4: cumulative planner estimate allowance — implemented

- Behavior: each planner metadata stream may request at most 16 storage estimates. The counter survives detach/reattach of planner metadata session state. Once exhausted, a valid estimate request returns `Unknown` without aborting the invocation, allowing fixed optimizer heuristics. The separate per-estimate scan bound remains 16,384 candidate rows; cancellation and deadline errors remain terminal.
- Changed code and contract: [planner metadata session](../../engine/src/transaction.rs), [statistics service](../../specs/statistics-service.md), and [planner budget](../../specs/planner-compiler.md).
- Verification: the budget exhaustion unit test confirms the allowance rejects the next estimate without advancing the counter. The planner IPC integration test builds a typed table through storage DDL, requests 16 exact summaries over `serve_planner`, receives `Unknown` on estimate 17, then successfully describes the relation again and confirms the invocation remains active. Locked workspace build, format check, warning-denied Clippy, and serial full workspace tests passed; commit integration reported 129 passed and 3 cgroup-gated ignored tests. A parallel full-test run reproduced the existing `InvalidAudit` flake in `storage_manager_creates_and_drops_check_constraint`; the serial rerun passed.
- Remaining: overall planner search budgets, asynchronous task draining, cache validity, and bounded maintenance collection remain open. Supervisor provisioning of the planner stream is also not implemented.

## ST-3: hidden redacted cell values do not change estimate output — verified

- Regression: the authorized-estimate fixture now creates two concurrent invocation overlays with identical policy-visible data and row IDs, but different contents in a column redacted from the caller. A row-value-dependent visibility policy forces both results to `Unknown`; the test asserts the normalized outputs are identical.
- Verification: `nix develop --offline --command cargo test --test transaction authorized_scan_estimate_counts_sanitized_cells_and_private_writes --locked` passed. This checks returned estimate contents only; timing/cache noninterference is not established.
- Remaining: test hidden values and invisible rows through paired authorized reference views, then address planner task draining, cache validity, and bounded maintenance collection.

## ST-3 follow-up: invisible candidates preserve the authorized reference view — verified

- Contract: adding rows hidden by a value-dependent row policy must not change the planner-visible estimate when the authorized view is unchanged; unsupported policy shapes return `Unknown` without partial counts.
- Regression: `authorized_scan_estimate_counts_sanitized_cells_and_private_writes` compares two invocation snapshots with different hidden candidate counts. Both estimates are `Unknown`, and independent paged authorized scans return the same single visible row with its hidden cell represented only as `REDACTED`.
- Verification: the focused real-redb transaction test passed (`1 passed, 0 failed`).
- Scope: checks normalized estimate results and reference rows, not timing or resource-use noninterference. Planner task draining, cache validity, and bounded maintenance collection remain open.

## ST-2 / ST-3 follow-up: resolve statically empty policy populations — implemented

- Contract: exact estimates are safe when row inclusion is independent of stored values. Literal Boolean policies can establish either the full row population or an empty one; unsupported policy expressions remain `Unknown`.
- Red evidence: a real-redb estimate for an applicable literal-FALSE table ALLOW returned `Unknown`, despite storage being able to prove the authorized population was empty. After adding constant-policy recognition, a false RESTRICT followed by a value-dependent policy incorrectly returned an exact empty estimate; the implementation now checks every applicable policy and falls back to `Unknown` if any policy is unsupported.
- Implementation: estimates return zero visible rows and zero cell populations without scanning when no applicable literal-TRUE ALLOW exists or a literal-FALSE RESTRICT applies. The full scan path remains limited to the all-rows case.
- Regression: the shared estimate fixture compares the exact empty summary with two independent authorized reference scans over different hidden candidate counts. It also confirms a constant denial cannot mask a later value-dependent policy.
- Verification: focused real-redb transaction test passed (`1 passed, 0 failed`).
- Scope: all other row-value-dependent or unsupported policies still return `Unknown`; timing/resource noninterference, planner task draining, cache validity, and bounded maintenance collection remain open.

## ST-2 follow-up: serialize exact predicate estimates over the planner stream — verified

- Regression: `planner_stream_estimate_allowance_falls_back_without_aborting_invocation` stages a row in the invocation overlay, applies literal-TRUE row and cell SELECT policies, and requests estimates over the real planner IPC stream. The decoded predicate response reports one visible and one matching row; the shared estimate allowance still returns `Unknown` when exhausted without aborting the invocation. Constant-deny behavior remains covered by transaction-level regressions.
- Verification: the focused real-redb planner-stream integration passed (`1 passed, 0 failed`). Workspace build, formatting, warning-denied Clippy, and the full serial delegated-cgroup workspace suite then passed; the commit, metadata-process, and transaction targets reported 149, 2, and 75 passing tests respectively, with no ignored tests.
- Scope: verifies planner request/response encoding and the shared estimate allowance. Isolated optimizer consumption and heuristic use of estimates remain open.

## ST-1 / ST-2 dependency: planner metadata and estimate dispatch

- Behavior: planner-only connection handles resolve and revalidate authorized relation descriptions, and estimate requests return bounded visible/readable/null/redacted summaries or UNKNOWN. Planner projections derive read grants and policies without loading write constraints or indexes; executor handles cannot be decoded as planner handles.
- Changed code and contracts: [planner IPC](../../engine/src/planner_ipc.rs), [planner connection state](../../engine/src/transaction.rs), [planner handle](../../engine/src/metadata_discovery.rs), [IPC protocol](../../specs/ipc-protocol.md), and [statistics service](../../specs/storage-manager.md).
- Verification: workspace build, format check, warning-denied Clippy, and `git diff --check` pass. Tests were not run in this increment.
- Remaining: optimizer scheduling/use of planner predicate estimates, planner task lifecycle/budget integration, and broader access-path estimates remain open.

## ST-1 / ST-2 dependency: planner stream lifecycle

- Behavior: `serve_planner` loops over one provisioned planner stream, enforces unique request IDs and a 256-request cap, and preserves kind-9 handle state across requests. Clean EOF closes only the planner session so execution can continue; malformed frames and protocol violations durably abort the invocation.
- Changed code and contract: [planner stream and dispatcher](../../engine/src/planner_ipc.rs) and [planner IPC contract](../../specs/ipc-protocol.md).
- Verification: format, workspace build, and warning-denied Clippy pass. Tests were not run in this increment.
- Remaining: supervisor provisioning, shared planner/executor invocation scheduling, and cancellation-aware planner task draining remain open.

## ST-1 / WI-7 dependency: estimate cost across candidate populations — measured

- Harness: [`statistics_estimate.rs`](../../engine/examples/statistics_estimate.rs) seeds 1,024, 8,192, and 16,383 deterministic authorized INTEGER rows and times 11 exact estimate calls per dataset, excluding bootstrap, seeding, compilation, and planner IPC.
- Profile and result: Rust/Cargo 1.95.0, redb 4.4.0, TinyCC 0.9.27-unstable-2025-01-06, Linux x86_64. Median/p95 call latency was 0.901/0.975 ms at 1,024 rows, 7.847/11.258 ms at 8,192 rows, and 14.202/16.845 ms at 16,383 rows, just below the 16,384 candidate ceiling.
- Scope: one local run on one host. These figures show scaling of the storage estimate call for this simple authorized shape; they are not planner, service-level, mixed-workload, or target-hardware performance claims. Repeated host/filesystem samples and broader policy/workload shapes remain open.
- Verification: `nix-shell -p rustc cargo clippy rustfmt tinycc --run 'cargo run -p prdb-engine --release --example statistics_estimate --locked'` completed successfully.

## ST-1 / WI-7 follow-up: estimate-cost run-to-run sample — measured

- Method: ran the already-built release harness three times sequentially; each run built and removed a fresh temporary database and took 11 estimate samples at each configured population size.
- Run-level median latency across the three repetitions: 0.921 ms at 1,024 rows (range 0.899–0.930), 7.460 ms at 8,192 rows (7.124–7.587), and 15.943 ms at 16,383 rows (14.979–23.795). Median of run-level p95 values: 1.406, 9.170, and 16.738 ms respectively; the 16,383-row p95 range was 15.835–27.299 ms.
- Scope: repetitions share one host and filesystem; cache state and competing system load are uncontrolled. The wide largest-dataset range confirms this is local comparative evidence, not a stable service target or target-hardware profile.
- Verification: all three executions of `target/release/examples/statistics_estimate` exited successfully.

## ST-2 / OP-4 follow-up: bounded population estimates guide join orientation — implemented slice

- Contract: [statistics service](../../specs/statistics-service.md) and [planner/compiler](../../specs/planner-compiler.md). The optimizer opts into one exact authorized small-population probe per join input using the existing estimate request allowance. The smaller known nonempty input, when at most 128 rows, may be chosen as the outer side for an eligible index on the other input; an exact empty input or two known inputs of at most four rows use hash join; unknown counts preserve the established fallback.
- Regression evidence: the optimizer service checks estimate-guided orientation and hash fallback for two small inputs. The optimizer process protocol regression verifies both join relation requests carry the estimate opt-in and that returned metadata affects the physical plan.
- Verification: focused optimizer service and process regressions passed in the locked Nix shell. Workspace build, format check, warning-denied Clippy, TinyCC SDK tests, sandbox fixture build, release-profile validation, cgroup preflight, and the complete serial delegated-cgroup workspace suite all passed. The full suite included the confined SQL join acceptance and process tests.
- Remaining: join estimates are bounded to 128 rows and single-table access estimates remain bounded to four rows. General predicate selectivity, join fanout, broader population estimates, safe cache validity/reuse, asynchronous estimate task draining, maintenance collection, and mixed-workload/target-host evidence remain open.

## ST-2 follow-up: join population estimates extend to 128 rows — implemented slice

- Contract: join relation metadata requests may opt into a population estimate with marker `52 07` and a big-endian `u16` maximum from 5 through 128. Storage reuses the policy-scoped, value-free row-key probe and the planner's shared 16-estimate allowance. Counts above the maximum or unsupported visibility policies return `Unknown` without partial counts.
- Regression evidence: planner request codecs accept a 128-row cap and reject 129. The real planner stream returns an exact estimate for the larger join request. A real-redb fixture returns 128 at the exact boundary and `Unknown` at 129. The optimizer service and process tests use 64/96 estimates to choose the smaller outer relation and eligible inner index.
- Verification: focused planner codec, planner-stream estimate-budget, real-redb 128/129 boundary, optimizer service, and optimizer process tests passed. `cargo build --workspace --locked --offline`, `cargo fmt --all -- --check`, warning-denied workspace Clippy, and the complete serial delegated-cgroup workspace suite passed. The full suite covered confined SQL, optimizer process, storage, and recovery tests. `git diff --check` passed.
- Remaining: this estimate supports join orientation up to 128 only; it does not estimate predicate selectivity, join fanout, or complete plan costs. Safe cache validity/reuse, asynchronous task draining, maintenance collection, and workload/target-host evidence remain open.

## ST-2 / OP-4 follow-up: measured medium joins retain hash strategy — implemented slice

- Contract: use the existing exact, authorization-scoped 0–128 row estimates to select hash join for two known inputs above the four-row small-input region. Preserve indexed nested-loop for one known 1–4-row outer input when the opposite relation has an eligible index. Keep the current deterministic fallback when estimates are unknown.
- Evidence: the existing confined 64×64, eight-duplicates-per-key benchmark measured indexed nested-loop at 539–787 ms and hash join at 232–387 ms during concurrent operational reads and commits. This is local comparative evidence, not a target-hardware cost model.
- Red/green: changed the service regression to require hash for known 64/96-row inputs; it failed before the strategy change. The service tests now cover medium-population hash selection and 4/96 indexed orientation. The process regression verifies the same strategy decision across planner IPC.
- Verification: `medium_join_cardinality_estimates_keep_hash_join`, `one_known_small_join_input_orients_to_the_opposite_index`, and the optimizer process regression passed. `cargo fmt --all -- --check`, workspace build, warning-denied Clippy, and the complete serial delegated-cgroup workspace suite passed. `git diff --check` passed.
- Remaining: this chooses a conservative measured rule, not generalized cost-based optimization. Better fanout/selectivity estimates, asymmetric workload profiles, cache validity/reuse, asynchronous task draining, maintenance collection, and target-host evidence remain open.

## ST-2 / OP-4 follow-up: exact predicate estimates cover 128 authorized rows — implemented slice

- Contract: planner predicate estimates may return exact `(visible_rows, matching_rows)` for up to 128 visible rows, only when row visibility is data-independent and every referenced cell is readable. Larger populations, redacted predicate cells, unsupported expressions, incomplete reads, or value-dependent row policies return `Unknown` with no partial count.
- Plan use: exact zero matches retain the validated empty-view hint. For nonzero estimates, the access crossover uses `visible_rows`; matching cardinality does not make a full scan cheaper. The existing four-row cutoff remains.
- Red/green: the transaction regression first returned `Unknown` for an authorized five-row population. The optimizer regression first chose a full scan from `matching_rows=2` despite `visible_rows=8`. Both now pass. A real planner stream fixture stages five authorized rows and verifies an exact `(5,1)` response while the separate four-row population probe falls back to `Unknown`.
- Profile: three sequential fresh-database runs of the release `statistics_estimate` harness, with 11 samples per run, measured the 128-row predicate estimate at a 0.131 ms median-of-run medians (0.128–0.142 ms) and 0.137 ms median-of-run p95s (0.135–0.220 ms). The row-count-only probe measured 0.115 ms median-of-run medians (0.115–0.119 ms) and 0.123 ms median-of-run p95s (0.120–0.128 ms). These are local API-call measurements and exclude planner IPC and SQL execution.
- Verification: focused transaction, optimizer-process, and planner-stream regressions passed. The release profile, `cargo fmt --all -- --check`, workspace build, warning-denied Clippy, and complete serial delegated-cgroup workspace suite passed. The full suite covered sandboxed SQL, process channels, transaction/storage, and recovery tests. `git diff --check` passed.
- Remaining: this is a bounded exact estimator, not a general selectivity model. Estimates above 128, broader supported predicates, planner-IPC overhead, and target-host profiles remain open.

## ST-2 / OP-4 follow-up: page exact predicate estimates to the 16,384-row budget — implemented slice

- Contract: exact predicate estimates now share the existing 16,384-candidate per-estimate bound. Snapshot row IDs are collected in pages no larger than the 1,024-row storage scan page; the estimator returns `Unknown` above the bound, on incomplete populations, or when a referenced cell is not fully readable.
- Red/green: the real-redb transaction regression first failed at a 129-row invocation overlay. It now verifies a 129-row exact `(visible=129, matching=1)` estimate, retains `Unknown` when the caller's maximum is below the population, and rejects a requested maximum above the candidate cap. The release harness verified an exact estimate at 16,384 snapshot rows, crossing the storage page boundary repeatedly.
- Profile: three sequential fresh-database runs, each with 11 samples, measured the 16,384-row predicate estimate at 17.235 ms median-of-run medians (17.011–18.343 ms) and 20.293 ms median-of-run p95s (17.231–20.356 ms). The row-count-only probe at the same population measured 1.299 ms median-of-run medians (1.274–1.328 ms) and 1.526 ms median-of-run p95s (1.413–1.553 ms). These are local API-call measurements and exclude planner IPC and SQL execution.
- Verification: the focused real-redb transaction test, release profile, workspace build, format check, warning-denied Clippy, TinyCC SDK tests and sandbox fixture passed. The cgroup probe reported a writable delegated parent with `cpu memory pids`; the complete serial delegated-cgroup workspace suite passed, including 368 library tests, 205 commit integration tests, 3 metadata-process tests, 79 transaction integration tests, and all remaining workspace targets.
- Remaining: the estimator supports only the documented Boolean/integer/text predicate subset and still returns `Unknown` for populations above 16,384 or value-dependent row policies. The added estimate latency is local and excludes planner IPC; cost-based path choice, cache validity, async task lifecycle, maintenance collection, and target-host workload evidence remain open.
## ST-2 follow-up: feed bounded predicate selectivity into range choice — implementation

- The optimizer consumes exact authorized `(visible_rows, matching_rows)` results to remove a range hint at 50% or higher selectivity for populations above four. The Filter remains, and unique point probes are unaffected. Unknown results preserve the ordinary access path. Predicate estimation remains capped at 16,384 rows and the shared planner request/candidate budgets.
- Local direct-API TEXT profiles used five fresh databases per selectivity with eleven samples per run. At 130 visible rows, range lookup led at 43%, the paths were near crossover at 49%, and scan plus filter led at 62%. At 514 rows, median-of-run p50s were 4.966 ms range versus 5.746 ms scan at 43%, 5.769 versus 5.565 ms at 49%, and 7.626 versus 6.037 ms at 62%. At 2,050 rows and 43% matches, five runs measured median-of-run p50s of 21.579 ms range and 25.323 ms scan plus filter. These access-path comparisons are host-local and exclude estimate acquisition from the selected path's runtime. At 2,050 rows, the exact predicate estimate itself measured 26.103 ms median-of-run p50s, more than the measured access-path difference; accounting for this per-query estimate cost in end-to-end planning latency remains open. The half cutoff is a bounded heuristic, not a general cost model or an operational mixed-load/target-host result.
- Regression coverage checks optimizer selection on either side of the cutoff and exact result equivalence in the benchmark harness. Workspace build, formatting, warning-denied Clippy, maintained TinyCC tests, sandbox fixture, cgroup preflight, and the complete serial delegated-cgroup workspace suite passed.
- Remaining: estimate overhead and end-to-end cost-aware path selection, generalized cost/cardinality statistics, mixed-load and target-host profiles, background collection, and broader operational estimate service work.

## ST-2 follow-up: count predicate matches in one candidate pass — verified slice

- The exact predicate estimator no longer calls the population-count probe before reading candidate row IDs. It checks policy eligibility and authority dependencies, then applies the existing snapshot/overlay limits while computing visible and matching counts in the same bounded pass. This removes a duplicate snapshot key traversal without raising the 16,384 candidate cap, broadening the predicate types, or changing redaction behavior.
- The `authorized_scan_estimate_counts_sanitized_cells_and_private_writes` regression and `authorized_scan_estimate_returns_unknown_at_candidate_budget` passed, covering exact counts, private writes, redaction/unknown behavior, and the candidate boundary. The direct API range/selectivity workload also passed with exact result equivalence; its 130-row predicate-estimate median was 1.450 ms. A cap-sized timing run could not complete because the benchmark transaction exceeded its total scan-work budget, so cap latency remains unmeasured in that harness.
- Workspace build, format check, warning-denied Clippy, maintained TinyCC tests, sandbox fixture build, delegated cgroup preflight, and the complete serial delegated-cgroup workspace suite passed (370 engine library tests and all integration targets).
- Verification: workspace build, format check, warning-denied Clippy, maintained TinyCC tests and sandbox fixture passed. The delegated cgroup probe found writable `cpu memory pids`; the complete serial delegated-cgroup workspace suite passed (371 engine library tests and all integration targets).
- Remaining: broader supported predicate types and policies, broader cache validity for summaries/cross-session reuse, background collection, and target-host/mixed-load profile work. Estimate task draining is covered for the current synchronous request design; asynchronous scheduling would require a new lifecycle contract.
## ST-5 follow-up: reuse exact predicate estimates within an invocation — verified slice

- The planner session caches exact predicate estimates by relation/schema identity, qualified name, encoded predicate, and candidate bound. Session lifetime pins the invocation principal, authorization state, and MVCC snapshot. Every cache hit still consumes the cumulative planner estimate allowance. Unknown results are not cached and entries do not cross planner sessions or invocations.
- `Overlay` now exposes an internal checked write generation that advances on each successful put, delete, replacement, or removal. The planner cache drops entries from older generations before lookup, conservatively invalidating on every private row mutation so own-write visibility cannot return stale estimates.
- Red/green: the shared planner-channel regression repeats an identical predicate after a synchronized private insert. Before generation validation it returned stale `(5,1)`; afterward it returns `(6,2)`. Overlay generation, exact predicate/private-write estimate, and candidate-cap tests pass.
- Verification: workspace build, formatting, warning-denied Clippy, maintained TinyCC tests, sandbox fixture, delegated-cgroup preflight, and the full serial delegated-cgroup workspace acceptance passed. The suite included the overlay-generation and exact predicate/private-write estimate regressions.
## ST-1 follow-up: enforce planner/executor roles through provisioned streams — verified

- Regression: the real planner and executor IPC servers now receive cross-role requests over Unix streams. An executor `EstimateScan` and planner `OpenScan` each produce no response, return `OperationNotAllowed` for their provisioned role, and record durable ABORTED outcomes.
- Verification: `planner_stream_estimate_allowance_falls_back_without_aborting_invocation` passed with both new channel-level denial cases. Workspace build, formatting, warning-denied Clippy, maintained TinyCC tests, sandbox fixture build, delegated-cgroup preflight, and the full serial delegated-cgroup workspace suite passed.

## ST-4 follow-up: synchronous estimate cancellation and channel draining — verified

- Architecture: each estimate runs inline on its provisioned planner request stream; it creates no detached planner task. Estimate-specific budget/candidate exhaustion returns `Unknown` without partial counts, while cancellation and deadline expiry remain terminal.
- Acceptance evidence: transaction and planner-stream tests cover cancellation/deadline during estimation. `cancellation_during_optimizer_predicate_estimate_kills_process_tree_without_fallback` and `deadline_during_optimizer_predicate_estimate_kills_process_tree_without_fallback` verify the optimizer process tree is terminated, the invocation durably aborts, and the worker cgroup is drained. `supervisor_drains_both_worker_channels_before_durable_finalize` verifies both provisioned channels drain before finalization.
- Verification: focused planner stream tests and the complete serial delegated-cgroup workspace suite passed. The ST-1 follow-up validation also passed workspace build, formatting, warning-denied Clippy, maintained TinyCC tests, sandbox fixture build, and cgroup preflight.
- Scope: this closes task-draining requirements for the current synchronous design. If asynchronous estimate scheduling is introduced later, it needs its own task ownership, snapshot-pin, cancellation, and drain acceptance.

## ST-6 follow-up: bounded committed population collection — first slice

- Contract: a trusted maintenance call reads only row keys from one committed redb snapshot, with an exact-count ceiling of 16,384 and one extra sentinel key. It stores either an exact count or `Unknown` under a versioned statistics-only key in protected `stats_v1`; the record includes the source commit sequence and is rebuilt on overflow rather than publishing partial counts.
- Admission and disclosure: one atomic maintenance slot is shared by all managers for the database and returns `Busy` rather than queueing another collection. The scan holds no commit/catalog lock, releases its read snapshot before writing the small durable record, and the public maintenance result contains only `Exact` or `Unknown`. There is no planner/executor IPC for collection or raw global counts.
- Regression evidence: `committed_population_collection_is_bounded_and_snapshot_tagged` checks exact and over-cap records, sequence invalidation after a later commit, and no partial count. `statistics_collection_admission_is_shared_across_manager_clones` checks single shared admission. `operational_row_commit_completes_while_population_collection_is_admitted` runs a row commit during a maximum-sized collection.
- Verification: collection and record-codec regressions pass. `cargo fmt --all -- --check`, locked workspace build, warning-denied Clippy, maintained TinyCC SDK tests, and sandbox fixture build pass. The delegated-cgroup preflight reports writable `cpu memory pids`; the complete serial delegated-cgroup workspace suite passes, including 376 library tests, 205 commit integration tests, 3 metadata-process tests, 79 transaction integration tests, and all remaining workspace targets.
- Remaining after the first slice: no internal statistics consumer reads these global counts yet, and richer physical statistics and mixed-load/target-host evidence remain open.

## ST-6 follow-up: scheduled round-robin table refresh — implementation

- The gateway maintenance loop now schedules one table population refresh per configured interval, with a 15-minute production default. It reads a single table catalog entry per tick, advances a wrapping in-process cursor, and uses the same shared collection permit and bounded collector. No planner or executor IPC exposes the global records.
- A storage regression drives the gateway's scheduled maintenance tick, checks the persisted exact record and cursor wraparound, and confirms this path does not materialize the table list. The focused regression passes.
- Verification: format check, locked workspace build, warning-denied Clippy, and the complete serial delegated-cgroup workspace suite pass. The suite includes 377 library tests, 205 commit integration tests, 3 metadata-process tests, 79 transaction tests, and all remaining workspace targets.
- Remaining: no trusted consumer reads the global records yet; collection currently covers only row population. Richer physical statistics, consumer freshness validation, and mixed-load/target-host evidence remain open.

## ST-6 follow-up: validate freshness before reuse — implementation

- Added a trusted reader that loads the protected population record and engine commit sequence from one redb snapshot. It returns a record only when the source sequence matches that snapshot; missing/stale records trigger refresh, while malformed records fail closed.
- The scheduled round-robin tick now skips redundant collection when a fresh exact or `Unknown` record exists. Counts remain in trusted storage and do not enter planner/executor IPC or public maintenance outcomes.
- Regressions verify exact record reuse at sequence 1, staleness after the next committed sequence, fresh `Unknown` reuse without inventing a partial count, and that a scheduled second tick reports `FreshRecord` instead of collecting again.
- Verification: the collection and scheduler-focused tests passed. Workspace build, format check, warning-denied Clippy, delegated-cgroup preflight, and the complete serial delegated-cgroup workspace suite passed (380 engine library tests, 205 commit integration tests, 3 metadata-process tests, 79 transaction tests, and all other workspace targets).

## ST-5 follow-up: reuse exact population probes within a planner session — verified slice

- Population estimates are cached by relation ID, schema ID/version, and requested row bound inside `PlannerMetadataSessionState`. The cache remains bound to the planner's invocation snapshot and authorization context; every request, including a cache hit, consumes the 16-request allowance. A changed private-write generation invalidates cached population and predicate estimates. Unknown results are not cached.
- `planner_stream_estimate_allowance_falls_back_without_aborting_invocation` now repeats an exact population estimate over the real planner stream, verifies a private insert changes the next estimate from five to six, and confirms cache hits do not bypass the cumulative allowance.
- Verification: the focused planner-stream integration regression and two cache unit tests pass. Format check, locked workspace build, warning-denied Clippy, and full serial delegated-cgroup workspace suite pass. The suite includes 379 library tests, 205 commit integration tests, 3 metadata-process tests, 79 transaction tests, and all remaining workspace targets.

## ST-5 follow-up: cache exact column summaries within a planner session — verified slice

- `PlannerMetadataSessionState` now caches exact authorized column summaries by relation ID, schema ID/version, and ordered requested column IDs. Entries stay scoped to the pinned invocation principal, authorization context, and snapshot; every private overlay mutation invalidates them by generation. Each lookup consumes the shared estimate allowance, and `Unknown` is never cached.
- The planner-stream allowance regression repeats an exact summary, stages a private row before the next identical request, and verifies the visible/readable count changes from six to seven. Cache unit coverage verifies requested-column order, exact reuse, and generation invalidation.
- Verification: the focused cache and planner-stream regressions passed. Workspace build, format check, warning-denied Clippy, maintained TinyCC SDK tests, sandbox fixture, writable delegated-cgroup preflight, and full serial delegated-cgroup workspace suite passed (380 engine library tests, 205 commit integration tests, 3 metadata-process tests, 79 transaction tests, and all remaining targets).

## ST-2 / OP-7 follow-up: validate high-fanout indexed join orientation — measured

- Contract: exact authorized input populations may orient the bounded indexed nested-loop path; they do not estimate join output cardinality or prove that indexed probing is generally cheaper.
- Evidence: the confined 4×128 high-fanout profile returned 512 equivalent result pairs for both kind-17 indexed nested-loop and hash join. Across five fresh-database runs with 192 operational reads and 192 durable commits per run, median run-level p50 was 140.3 ms indexed and 294.4 ms hash. The benchmark verifies plan kind 17 before recording indexed timings.
- Limitation: this adds evidence for one asymmetric shape only. Join planning remains capped at 128 rows per relation, and trusted global population records remain unavailable to the optimizer. Join fanout, general result-cardinality estimates, broader cost comparison, and target-host repeats remain open.
- Verification: TinyCC SDK tests, sandbox fixture build, locked workspace build, format check, warning-denied Clippy, five focused serial delegated-cgroup timing runs, and a final strict-result-check run passed. The complete serial delegated-cgroup workspace suite passed (380 library tests, 205 commit tests, 3 metadata-process tests, 79 transaction tests, and all remaining targets; no ignored tests).

## ST-2 / OP-7 follow-up: use bounded population skew to orient indexed joins — implemented slice

- Contract: join planning still requests exact authorized populations capped at 128 rows per relation. For two nonempty inputs above four rows, the smaller input may drive an eligible opposite-side index only when it has at most eight rows and the other input is at least 16 times larger; other known medium joins retain hash. Unknown estimates preserve the documented fallback.
- Regression: the optimizer service's 8×128 test first failed with hash join, then passed after the skew rule. Reverse orientation and the 8×127 ratio boundary are covered; the existing 64×96 hash regression verifies the medium fallback.
- Evidence: five confined 8×128 runs with 1,024 equivalent result rows measured median run-level p50 of 194.4 ms indexed and 287.6 ms hash while each run completed 192 operational reads and durable commits. The 4×128 high-fanout profile also favors indexed nested-loop. These are local measurements, not general cost proof.
- Verification: optimizer-service tests and five focused delegated-cgroup timing runs passed. Locked workspace build, formatting, warning-denied Clippy, TinyCC SDK tests, sandbox fixture build, and the full serial delegated-cgroup workspace suite passed (383 library tests, 205 commit tests, 3 metadata-process tests, 79 transaction tests, all other targets; no ignored tests).
- Limitation: this uses input population skew, not distinct-key, fanout, or output-cardinality statistics. It does not consume the protected committed population table and does not expand the 128-row estimate cap.

## ST-6 follow-up: remove population records when dropping tables — verified

- Contract: a committed `DROP TABLE` removes the table's protected committed-population record atomically with its catalog deletion; failed or aborted drops preserve the record.
- Red/green evidence: the real-redb commit integration test first failed because the record remained after a successful table drop. It now checks the exact record exists before the drop, remains after an over-limit aborted drop, and is absent after the successful drop.
- Implementation: the storage commit transaction detects deleted table catalog records and removes their statistics key before committing the same redb transaction.
- Verification: the focused `storage_manager_drop_table_removes_owned_catalog_and_rows` regression passed. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied all-target Clippy, TinyCC `sandbox-query-fixture`, the cgroup preflight, and the full serial delegated-cgroup workspace suite passed; the suite ran 409 library tests, 206 commit tests, 3 metadata-process tests, 85 transaction tests, and all remaining targets with no ignored tests.

## ST-2 / OP-4 follow-up: measure 18-row low-fanout outer boundary — verified slice

- The exact pair-estimate rule now admits a smaller outer population through 18 rows when the indexed input is at least seven times larger; the four-pairs-per-outer cutoff is unchanged. The optimizer test first failed at 18×126 and passed after the bound expansion. Process eligibility tests reject 18×125 and 19×133.
- Five confined 18×126 runs selected indexed nested-loop and verified hash-equivalent output while 192 operational reads and commits overlapped. Median run-level p50 was 273.9 ms indexed (246.6–281.1 ms range) versus 305.3 ms hash (279.6–336.5 ms range).
- Scope: one additional uniform fanout-one shape on this host. This does not establish the crossover for higher fanout, larger outers, or deployment hardware.
- Verification: optimizer-service and process boundary tests, five confined profile runs, maintained TinyCC SDK tests and fixture build, locked workspace build, format check, warning-denied all-target Clippy, and full serial delegated-cgroup workspace suite passed (409 library, 206 commit, 3 metadata-process, 85 transaction tests; all remaining targets passed with no ignored tests).

## ST-2 / OP-4 follow-up: verify the 18-row exact-cardinality cutoff — measured

- Five fresh-database confined 18×126 runs at the inclusive 72-pair limit selected indexed nested-loop and matched hash output; median run-level p50 was 259.8 ms indexed (247.1–309.9 ms range) versus 307.9 ms hash (299.0–338.0 ms range).
- Five runs at 73 pairs selected hash, returned all 73 pairs, and had a median run-level p50 of 348.5 ms (327.1–361.7 ms range). Both profiles overlapped 192 operational reads and commits in every run.
- The exact boundary ran through optimizer, planner/storage estimates, confined SDK execution, and the gateway relay. Maintained TinyCC tests, fixture rebuild, locked workspace build, format check, warning-denied Clippy, and the full serial delegated-cgroup suite passed (409 library tests, 206 commit tests, 3 metadata-process tests, 85 transaction tests, and all remaining targets; no ignored tests). These measurements are host-local and do not establish general cost-based selection or target-host performance.
