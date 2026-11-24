# Optimizer Progress

## OP-4 / ST-2 follow-up: typed DATE predicate literal — implemented slice

- Request and plan tag `0x04` carries a signed big-endian i32 DATE count since 1970-01-01, restricted to the supported DATE range. Rust binding checks DATE compatibility; exact authorized predicate estimates and TinyCC filter evaluation use the established DATE comparison semantics. DATE literals can use unique point and leading-key range hints.
- Evidence: Rust request decode/invalid-range and plan round-trip tests, real-redb exact DATE estimate regression, and TinyCC parameter request encoding and DATE plan decoding fixtures passed. The full delegated-cgroup serial workspace suite passed.
- Remaining: DECIMAL, BYTEA, UUID, and TIMESTAMPTZ literals, cross-type coercions, broader candidate comparison, and target-host workload evidence remain open.

## Filtered composite aggregate planning — verified

- Contract: statement 15 and plan kind 15 preserve statement 13's wire format while adding a required bounded Boolean filter before composite grouping. Authorized binding includes filter columns; the plan scan is the sorted union of filter columns, grouping keys, and aggregate input.
- Implementation: Rust decodes, binds, plans, encodes, and dispatches statement 15 through `OptimizeGroupCount`. The C SDK encodes parameterized WHERE trees, decodes and validates kind 15, matches the filter predicate to the parsed AST and parameters, re-resolves executor identity, and invokes the v13 grouping runner with the same parameters.
- Regression: Rust tests cover request decoding, authorized binding, filter-only scan columns, plan round trips, and optimizer process dispatch. TinyCC fixtures check canonical request bytes, malformed plan rejection, predicate mismatch rejection, and filtered SUM execution with the expected single group.
- Verification: ABI generation check, full TinyCC SDK suite plus `sandbox-query-fixture`, formatting check, locked workspace build, warning-denied all-target Clippy, and the full serial workspace suite passed in the delegated user cgroup.
- Remaining: filtered composite aggregates still need confined SQL artifact acceptance and broader analytic workload, secure spill, target-hardware, and mixed-workload evidence.

## OP-5 follow-up: expose the authorized unique point probe over executor IPC — implemented

- Contract: the executor accepts a resolved relation handle, logical index ID, non-NULL key values, and selected columns. Storage revalidates the pinned index and authorization; responses distinguish `unavailable`, `no_match`, and a sanitized row with a connection-local opaque handle.
- Implementation: executor operation `0x1114` and its v1 bounded codecs now route to `probe_catalog_unique_index_for_principal`. Raw row IDs remain in the trusted connection's handle map. Hidden or conditionally readable key cells retain the `unavailable` fallback semantics.
- Regression: request/response codec round trips cover all three outcomes, sanitized values, malformed truncation, and opaque row handles. The token-authenticated two-user commit fixture now creates a backfilled unique index and exercises executor IPC: the authorized principal gets `Match` and `NoMatch`, while the restricted principal gets identical `Unavailable` responses for matching and absent keys.
- Verification: focused catalog-backed executor IPC regression passed. Locked workspace build, rustfmt check, warning-denied Clippy, maintained TinyCC SDK tests, sandbox-query fixture build, and the full serial delegated-cgroup workspace suite passed. The suite reported 189 library tests, 148 commit tests, 74 transaction tests, and zero ignored tests across targets.
- Remaining: connect optimizer plan matching and selection to this operation; optimized-versus-reference result/error equivalence remains open.

## OP-5 follow-up: disclose only statically authorized unique paths — implemented

- Contract: planning metadata contains only logical index IDs and ordered key-column IDs for unique paths whose every key cell is statically readable and unconditionally allowed under the pinned authorization projection.
- Implementation: `AuthorizedPlanRelation` and planner/executor `describe_plan_relation` responses now carry bounded unique-access-path metadata. Storage filters paths using the same authorization predicate as the point probe; information-schema relations return no paths. The decoder validates path IDs, column membership, duplicates, and bounds.
- Regression: the token-authenticated two-user catalog fixture confirms the authorized reader receives the unique path and the restricted principal receives none; the path list survives a planner IPC response round trip.
- Verification: focused real-redb planner metadata and executor IPC regression passed. Full delegated-cgroup tests, including executor cancellation, passed with 189 library tests, 148 commit tests, and 74 transaction tests; workspace formatting and warning-denied Clippy passed. The first full run caught the planner-only path suffix leaking into the C executor identity response; `resolve_plan_relation` now retains its prior identity-only bytes, while `describe_plan_relation` carries the optional path suffix.
- Remaining: the optimizer does not yet select a path, match it to an exact equality filter, or execute the resulting plan through the C SDK.

## OP-5 follow-up: select and execute an authorized unique path — implemented; verification pending

- Contract: [optimizer specification](../../specs/planner-compiler.md). A point hint is allowed only for an exact root equality on one statically authorized single-column unique path; it conveys no storage authority and must remain a valid SELECT plan when ignored.
- Implementation: bound SELECTs retain authorized unique paths. `plan_select_v1` emits optional suffix `0xa1` with the stable index ID, complete key-column record, and literal value only for same-typed Boolean, in-range INTEGER, or TEXT equality. Rust validation ties it to the scan shape and root filter. TinyCC independently verifies the hint's key/value against that exact equality; the existing AST/parameter matcher then ties it to the parsed query. The SDK calls `ProbeUniqueIndex` only after executor relation validation, consumes a sanitized match through the normal filter/project/limit path, returns zero rows for `no_match`, and falls back to an authorized scan for `unavailable` or a too-small probe response.
- Regression: Rust plan tests cover an exact approved equality, canonical round-trip, no-path fallback, and composite-path rejection. The TinyCC fixture verifies the bounded request fields, full requested row shape, match/no-match/unavailable behavior, scan fallback, cancellation, and rejection of mismatched hint values/types and zero index IDs.
- Verification: the TinyCC fixture and full maintained SDK suite passed, including ABI generator checks; `sandbox-query-fixture` rebuilt. Locked workspace build, format check, warning-denied Clippy, and the full serial delegated-cgroup workspace suite passed (190 library tests, 148 commit tests, 74 transaction tests; zero ignored). The confined `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed with optimizer-backed `SELECT id FROM records WHERE id = $1 LIMIT 1` routed through the real unique-index probe, then result relay and durable commit.
- Remaining: benchmark point-probe compilation/setup/result latency against the scan fallback under the workload profile; broaden supported key types/coercions only with corresponding SQL semantics and acceptance coverage.

## OP-5 follow-up: select and execute an authorized leading-key range — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). A range hint is valid only for the exact root comparison and an authorized leading key. It conveys no storage authority; the executor rechecks visibility and availability for every request.
- Implementation: Rust SELECT planning emits suffix `0xa2` for one-sided `<`, `<=`, `>`, or `>=` filters on the first key of currently disclosed authorized paths. Rust and TinyCC validate the index/key identity, scalar literal, inclusivity, and exact root-filter match. The TinyCC runner submits `ScanIndexRange`, privately adapts sanitized handle-free rows to the existing filter/project/limit consumer, and uses full-scan fallback for unavailable or oversized range results. Malformed responses fail closed and definitive results are cancellation-checked.
- Regression: Rust covers approved leading-key selection, canonical round-trip, missing-path fallback, leading-position mismatch, and filter mismatch. The TinyCC fixture exercises returned rows, empty ranges, malformed bound flags, mismatched operators, unavailable fallback, and existing point-probe behavior. The confined SQL acceptance sends `id >= $1` through the range hint and confirms the projected result.
- Verification: the focused Rust planner test, locked workspace build, rustfmt check, warning-denied Clippy, complete maintained TinyCC SDK suite, sandbox-query fixture build, and pinned release-profile validation passed. The full serial workspace suite passed inside the delegated user service: 194 library tests, 148 commit tests, 74 transaction tests, and all other targets, with zero ignored tests. The TinyCC fixture checks exact request fields, returned and empty ranges, malformed bound flags, operator mismatch, unavailable fallback, and point-probe regressions.
- Scope: at this point the range selector used only unique paths. The following metadata follow-up adds authorized non-unique paths; the two-sided range follow-up below adds conjunctive bounds. Workload benchmarks remain open.

## OP-5 follow-up: disclose safe non-unique range paths — verified

- Contract: [metadata discovery](../../specs/metadata-discovery.md) and [optimizer](../../specs/planner-compiler.md). Planner metadata may expose only logical path IDs and ordered key columns whose complete key is statically readable and unconditionally allowed for the invocation. Non-unique paths can guide range scans but never equality probes.
- Implementation: `AuthorizedPlanRelation` now carries separate unique and non-unique path lists. Planner IPC appends an optional `0x5201` range-path section; executor identity responses continue to omit both lists. Storage classifies paths from the pinned catalog with the same full-key readability check. SELECT binding carries the range list, and range hint selection can use a matching leading key from either path class while point selection remains unique-only.
- Regression: the planner test selects a range from a composite non-unique leading key, rejects a later composite key, and verifies a non-unique path cannot produce a point probe. The authenticated two-principal storage test verifies the readable principal receives the non-unique path, the restricted principal receives none, and both path lists survive planner IPC round-trip.
- Verification: the focused confined acceptance passed inside the delegated `systemd --user` service (`1 passed, 0 ignored`), exercising TinyCC plan decode, non-unique range execution, and the authorized row result. `make -f engine/sdk/Makefile test` passed, including the eight ABI generator tests and maintained TinyCC SDK executables. Workspace build, format check, warning-denied Clippy, and full delegated serial workspace tests passed: 194 library tests, 148 commit tests, 74 transaction tests, 5 tuple tests, and zero ignored across test targets.
- Scope: the metadata section is backward compatible for identity-only responses and planner responses with no optional suffix. Decoder checks reject malformed markers/counts and duplicate/cross-class path IDs.

## OP-5 follow-up: plan and validate two-sided leading-key ranges — verified

- Contract: [optimizer specification](../../specs/planner-compiler.md). A two-sided hint requires exactly two AND terms: one lower and one upper comparison on the same authorized leading key. Term order and literal side do not affect the range. Other conjunction shapes keep the reference scan.
- Red/green: the Rust regression initially failed because the planner produced no hint for `key >= 10 AND key < 20`. Rust now combines the bounds only when the terms share a key and provide opposite directions. TinyCC independently validates both hint bounds against both predicate terms, rejecting altered bounds.
- Confined regression: the maintained SQL artifact executes `SELECT left_key FROM grouped_records WHERE left_key >= $1 AND left_key < $2 LIMIT 1` through the optimizer and non-unique range path, with typed parameters 1 and 2; it returns the expected row.
- Verification: focused Rust planner test, full maintained SDK tests, `sandbox-query-fixture` rebuild, and focused delegated-cgroup acceptance passed. Workspace build, format check, warning-denied Clippy, and release-profile validation passed. The full serial delegated-cgroup workspace suite passed with 195 library tests, 148 commit tests, 74 transaction tests, 5 tuple tests, and zero ignored across test targets.

## OP-5 follow-up: plan equality scans on non-unique leading keys — verified

- Contract: an equality predicate on an authorized non-unique leading key can use a bounded range request with identical inclusive lower and upper values. Single-column unique equality retains the unique point operation. Both plans remain tied to the exact root predicate and fall back to the authorized scan when a range is unavailable or oversized.
- Red/green: the Rust planner regression first failed because equality produced no range hint. Rust now lowers equality to paired inclusive bounds. TinyCC independently matches those bounds to the SQL filter, encodes both executor request bounds, and consumes sanitized result rows through the normal predicate/projection path.
- Confined regression: the maintained SQL artifact now executes `SELECT left_key FROM grouped_records WHERE left_key = $1 LIMIT 1` through the real non-unique leading-key path. The sandboxed SQL acceptance verifies the expected relayed result and durable commit.
- Verification: focused planner test passed; `make -f engine/sdk/Makefile test sandbox-query-fixture` passed. The cgroup preflight reported writable v2 delegation with `cpu memory pids` available and enabled. The full serial workspace suite passed inside the delegated user service, including `sandboxed_v3_sql_dml_sequence_is_visible_and_commits`; all test targets reported zero ignored tests. Workspace build, format check, warning-denied Clippy, and `git diff --check` passed.
- Scope: the change adds an access path only when metadata already authorizes the indexed cells; storage continues to validate the path and return authorized sanitized rows. Broader hidden-data/error equivalence and end-to-end SQL timing remain open.

## OP-5 follow-up: compare index results with the authorized reference scan — verified

- Regression: the real-redb catalog-index fixture now obtains a full authorized scan and independently filters its sanitized values, then checks that both the unique point probe and bounded range scan return the same row identity and sanitized cells. Existing cases in the same fixture continue to require `Unavailable` for hidden/conditionally readable keys and verify staged updates/deletes shadow pinned index entries.
- Verification: the focused `unique_constraint_uses_its_backing_index_for_invocation_writes` regression passed. Locked workspace build, format check, warning-denied Clippy, and the full serial delegated-cgroup workspace suite passed; the suite included 195 library tests, 148 commit tests, 74 transaction tests, 5 tuple tests, and zero ignored tests across all targets. `git diff --check` passed.
- Scope: this proves storage result equivalence for the tested exact key and same authorization snapshot; end-to-end timing against forced SDK scan fallback and wider hidden-data/error equivalence cases remain open.

## OP-5 follow-up: cover explicit range response-capacity fallback — verified

- Regression: the maintained TinyCC optimizer fixture now makes the range executor callback return `PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL` and verifies the runner falls back to the authorized scan, returns the same projected row, and completes with the expected executor call counts. This directly covers the oversized-response branch alongside the existing `Unavailable` fallback case.
- Verification: `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed, including `sdk-optimizer-plan-v1` and the full maintained SDK executable suite. `git diff --check` passed.
- Scope: the fallback behavior was already implemented; this increment adds regression coverage. Broader hidden-data/error equivalence and end-to-end SQL setup/result timing remain open.

## OP-5 follow-up: repeat point/range storage latency against reference scan — measured

- The same catalog-backed regression has an opt-in `PRDB_BENCH_INDEX_ACCESS` profile. It seeds 4,096 additional indexed BYTEA rows plus one exact target, then measures 11 point probes, bounded exact-key ranges, and full authorized scan-plus-filter operations in one pinned invocation. Every measured result is checked against the same expected row.
- Five sequential fresh-database runs on Linux x86_64, Rust 1.95.0, and redb 4.4.0 produced run-level median point-probe latency of 0.160–0.173 ms (median 0.171 ms), range latency of 0.123–0.142 ms (median 0.134 ms), and authorized scan-plus-filter latency of 54.993–61.966 ms (median 55.192 ms). The median of run-level p95 values was 0.272 ms, 0.207 ms, and 63.876 ms respectively.
- Scope: this measures storage API result latency with catalog/schema/index metadata and query argument construction already outside the timed region; it excludes query compilation, SQL parsing/planning, executor startup, gateway relay, and durable commit. The OS cache and host load were uncontrolled. The maintained benchmark separately measures artifact compilation/linking; end-to-end SQL setup and result latency remain open.

## OP-5 follow-up: measure optimizer SQL compilation and linking — measured

- The maintained [`benchmark_sdk_build.py`](../../engine/tools/benchmark_sdk_build.py) now defaults to compiling the real optimizer-backed SQL artifact and retains the caught-error fixture as an explicit alternate profile. It reports the selected fixture alongside SDK archive build, TinyCC compile, and GNU shared-link timings.
- Five local Linux x86_64 runs with the pinned TinyCC 0.9.27-unstable-2025-01-06 and GCC 15.3.0 measured optimizer SQL compile at 5.782 ms median after the first sample (7.278 ms); shared linking measured 43.232 ms median (44.972 ms first). SDK archive clean builds measured 135.891 ms median, with a 4.113 ms no-op rebuild. The filesystem cache state was uncontrolled.
- Scope: this isolates compilation and linking only. It does not measure optimizer planning, executor setup, point/range result latency, or scan-fallback comparison; the end-to-end indexed-workload benchmark remains open.

## OP-7 follow-up: backpressure per-invocation planner submissions — verified

- Contract: [optimizer specification](../../specs/planner-compiler.md). The isolated optimizer handles one submission at a time and waits for the matching invocation-scoped metadata response before planning the next request; request and estimate counts are bounded per connection.
- Regression: `process_protocol_backpressures_submissions_while_metadata_is_pending` pipelines two SELECT submissions, holds the first metadata response, and verifies the optimizer emits no second planner request until the first plan completes. It then verifies both responses and request IDs.
- Verification: `cargo test -p prdb-engine --lib optimizer_process::tests::process_protocol_backpressures_submissions_while_metadata_is_pending --locked --offline -- --exact --nocapture` passed (`1 passed, 0 ignored`).
- Scope: process-per-invocation plus START caps bounds active optimizer concurrency without a shared user-space planner queue. Higher-concurrency saturation at the configured limits and target-hardware measurements remain open.

## OP-7 follow-up: authorized filtered aliased-join plan backend — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). Statement 12 preserves the frozen statement-4 join request and adds one side-qualified parameter comparison or NULL test; kind 12 preserves kind-4 join metadata and appends the authorized filter column and expression.
- Implementation: the bounded decoder rejects invalid sides/operators, zero or over-limit parameter indexes, malformed identifiers, and trailing bytes. Binding resolves the filter against the selected authorized relation; plan lowering adds it to that side's ordinal-sorted scan shape. Rust kind-12 decoding validates the filter column is scanned on its declared side and preserves kind-4 behavior unchanged.
- Verification: the filtered request/bounds, side-specific metadata binding, kind-12 round-trip/scan membership, authorized service plan, and optimizer-process dispatch regressions passed. Generated ABI checks, workspace build, format check, warning-denied Clippy, maintained TinyCC tests, sandbox fixture build, and the full serial delegated-cgroup workspace suite passed. The suite reported 179 library tests, 142 commit tests, and 74 transaction tests, with zero ignored tests across all targets. The TinyCC suite does not yet exercise kind 12.
- Remaining: TinyCC statement encoding and AST matching, executor identity validation for the filter column, optimizer-backed v20 runner integration, and confined v20 acceptance remain open.

## OP-7 follow-up: independently decode filtered aliased-join plans in TinyCC — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). The SDK independently validates optimizer kind 12 before it can trust a side-qualified filter column or acquire executor capabilities.
- Implementation: `prdb_sdk_optimizer_decode_filtered_join_plan_v1` accepts only kind 12, decodes the existing kind-4 join body, then validates the filter side, full column record, membership in that side's authorized scan shape, expression kind, comparison operator, and one-based parameter index in the 1–256 range. The existing kind-4 decoder remains strict and rejects kind 12.
- Regression: the TinyCC fixture accepts a parameterized right-side filter and rejects an invalid side, a filter column absent from the scan, parameter index zero, trailing bytes, and kind 12 through the kind-4 entry point.
- Verification: maintained `make -f engine/sdk/Makefile test`, sandbox query fixture rebuild, and `git diff --check` passed. The full serial workspace suite passed in the delegated `systemd --user` service: 179 library tests, 142 commit tests, 74 transaction tests, and all other targets passed with zero ignored tests. Both cgroup-backed commit acceptance tests and both metadata process tests ran.
- Remaining: encode statement 12, match the decoded filter against the v20 AST and invocation parameters, re-resolve the filtered column through executor metadata, and execute the optimizer-backed v20 join through confined acceptance.

## OP-7 follow-up: encode filtered aliased-join requests in TinyCC — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). The v20 WHERE predicate is serialized as statement 12 with a side-qualified column, comparison and bounded parameter index, or a NULL test.
- Implementation: `prdb_sdk_optimizer_encode_filtered_join_select_v1` reuses the frozen statement-4 join prefix and LIMIT encoding, switches to statement 12, resolves the parsed WHERE qualifier to the left/right alias, and appends the normalized filter record. It rejects unsupported qualifiers, operators, parameter indexes, and parameter metadata on NULL tests. Submission uses the existing ABI v5 `optimize_join_select` operation and now accepts statement 12.
- Regression: the TinyCC fixture parses parameterized right-side and IS NOT NULL left-side v20 joins, compares both full requests to hand-checked bytes, submits the parameterized request on the join operation, checks undersized-buffer preservation, and rejects a zero parameter index.
- Verification: forced rebuild and full maintained `make -B -f engine/sdk/Makefile test` passed with TinyCC `-Wall -Wextra -Werror`; ABI generator consistency and all eight generator tests passed. `git diff --check` passed.
- Remaining: match the decoded filter against the v20 AST and invocation parameter types/values, re-resolve the filtered column through executor metadata, and execute the optimizer-backed v20 join through confined acceptance.

## OP-7 follow-up: match filtered join plans to v20 AST — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). Before executor access, kind 12 must preserve the v20 join AST and its authorized WHERE binding and parameter type.
- Implementation: `prdb_sdk_optimizer_match_filtered_join_plan_v20_v1` decodes kind 12, verifies the common join identity/projection/key/LIMIT records, then compares filter side and full bound column identity, normalized comparison, and parameter index. Parameter comparisons are type-checked against the bound column; NULL tests require no parameter metadata. The base join matcher remains unchanged for kind 4.
- Regression: the maintained TinyCC fixture accepts the bound parameterized v20 plan and rejects mismatched comparison, parameter index, and parameter type. The earlier decoder fixture covers malformed side/column/index and trailing data.
- Verification: `make -f engine/sdk/Makefile test` passed with the maintained TinyCC suite, all eight ABI generator tests, and generated ABI consistency. `git diff --check` passed.
- Remaining: re-resolve both filtered join relation identities and scan shapes through executor metadata, then add the optimizer-backed v20 execution path and delegated-cgroup acceptance.

## OP-7 follow-up: validate filtered join executor identity — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). Before scanning, kind 12's two relation IDs, epochs, and complete authorized column shapes must still match executor storage.
- Implementation: `prdb_sdk_optimizer_validate_executor_filtered_join_relations_v1` independently decodes kind 12 and reuses the existing stable-identity and complete scan-shape checks for both relation sides. Decoder validation requires the filter column to be present in the selected side's scan shape, so it is included in the executor comparison.
- Regression: the TinyCC fixture accepts both executor resolutions and rejects an injected relation identity mismatch; the existing kind-4 validator remains covered.
- Verification: full maintained `make -f engine/sdk/Makefile test` passed with TinyCC `-Wall -Wextra -Werror`, all eight ABI generator tests, and generated ABI consistency. `git diff --check` passed.
- Remaining: connect statement-12 submission, AST matching, and executor identity checks to the v20 optimized runner, then exercise it through delegated-cgroup confined acceptance.

## OP-7 follow-up: optimizer-backed filtered join runner — verified

- Contract: submit statement 12, match the returned kind-12 plan against the parsed v20 SQL and typed parameters, re-resolve both relations and complete scan shapes through executor storage, then execute the bounded filtered join.
- Implementation: `prdb_sdk_sql_execute_inner_join_optimizer_v20` now performs those gates before invoking the v20 join kernel. The optimized path accounts for its four extra executor requests by limiting each scan to 117 pages (the direct v20 runner remains at 119).
- Regression: the maintained TinyCC executor fixture returns a kind-12 optimizer plan for a parameterized right-side filter, validates the matching join, and checks one exact matching row plus the planner, identity-resolution, executor-request, and failure-latch counts.
- Confined acceptance: the ABI v5 `sql_v3_write_read.c` artifact now runs the parameterized statement-12/kind-12 join through optimizer submission, AST matching, executor identity re-resolution, bounded execution, and SQLR relay. The acceptance verifies both direct and optimizer-backed v20 result frames as `(1, 42)`.
- Verification: forced TinyCC rebuild of the targeted executor fixture, full maintained SDK suite, ABI generator consistency and all eight generator tests passed. The focused delegated-cgroup commit acceptance passed (`1 passed, 0 ignored`). The complete serial delegated-cgroup workspace suite passed, including 179 library tests, 142 commit tests, 74 transaction tests, and all remaining targets with zero ignored tests. `cargo fmt --all -- --check`, warning-denied workspace Clippy, and `git diff --check` passed.
- Measurement: five sequential fresh-database delegated-cgroup acceptance runs exercised the direct and optimizer-backed filtered joins plus grouped/sorted SQL alongside operational reads/commits. Median run-level point-read p50/p95/p99 was 1.407/2.180/3.054 ms at baseline and 1.762/2.836/4.150 ms during analytics; durable-commit medians were 11.583/14.767/18.034 ms at baseline and 11.535/15.890/17.239 ms with analytics. The repeated sample is recorded in [workload progress](../workload-profile/PROGRESS.md); it is not a saturation target.
- Remaining: broader multi-client analytical saturation beyond the repeated two-client capped acceptance, higher-concurrency optimizer saturation, larger SQL memory-pressure tests and allocator-level copy accounting, and target-hardware measurements remain open. The slow-gateway bounded-output acceptance is recorded in [workload progress](../workload-profile/PROGRESS.md).

## OP-7 follow-up: ordered single-key grouped aggregates — verified

- Contract: request statement 11 / plan kind 11 extends the statement-10 single-key COUNT/MIN/MAX/SUM/AVG shape with up to two distinct ORDER BY expressions drawn from the group key or selected aggregate, preserving direction, NULL placement, and optional LIMIT. WHERE, HAVING, extra projections, and unsupported argument types remain rejected.
- Implementation: Rust request decoding, authorized metadata binding, numeric checks, canonical kind-11 plan codec, optimizer process dispatch, and IPC response validation are implemented. TinyCC encodes statement 11, decodes kind 11, validates the exact ordinal-sorted scan shape, matches v12 AST sort fields and LIMIT, re-resolves executor relation identity, and invokes the bounded grouped sorter.
- Confined acceptance: the v12 SQL fixture runs an AVG grouped query ordered by AVG descending NULLS LAST and the group key ascending NULLS FIRST, with LIMIT 2. The acceptance verifies both exact SQLR records in returned order through the delegated executor path.
- Coverage matrix: Rust request decoding, authorized binding, and canonical plan round-trip now exercise COUNT, MIN, MAX, SUM, and AVG. The maintained TinyCC plan fixture independently decodes each kind-11 plan and matches it to corresponding v12 SQL with the same two ORDER BY expressions. The v12 executor fixture runs all five aggregate kinds over deterministic rows and checks the sorted keys and aggregate values.
- Verification: focused Rust request/binding/plan/process tests passed; the complete TinyCC SDK suite and sandbox fixture build passed; the focused delegated SQL DML sequence passed (1 passed, 0 ignored). Full locked workspace build, format, warning-denied Clippy, ABI generation, and serial workspace tests passed. Commit integration reported 142 passed and zero ignored; both metadata process tests passed; every target reported zero ignored cases. `git diff --check` passed.

## OP-7 follow-up: single-key grouped aggregate plans — verified

- Contract: request statement 10 / plan kind 10 plans one group key with COUNT(*), MIN, MAX, SUM, or AVG and optional LIMIT. It rejects WHERE, HAVING, ORDER BY, extra projections, invalid arguments, and trailing bytes; SUM/AVG require numeric arguments.
- Implementation: Rust request decoding, metadata binding, type checks, exact ordinal-sorted union scan planning, canonical plan validation, process dispatch, and IPC validation are implemented. TinyCC encodes statement 10, decodes kind 10, validates scan and aggregate shape, matches the v7 AST/binding, re-resolves executor relation identity, then calls the bounded v7 runner.
- Confined acceptance: the ABI v5 SQL fixture routes `SELECT id, AVG(id) FROM records GROUP BY id` through statement 10 and kind 10. It verifies the established AVG SQLR result through result relay and durable commit.
- Verification: focused Rust aggregate binding/plan tests passed; the TinyCC SDK suite and fixture build passed; the focused confined SQL DML sequence passed. Locked workspace build, formatting, warning-denied Clippy, and the complete workspace suite passed. The suite ran inside the delegated `systemd --user` cgroup; all four cgroup-backed commit tests and both metadata process tests ran, with every test target reporting zero ignored cases. `git diff --check` passed.

## OP-7 follow-up: composite grouped COUNT sort plan — verified

- Contract: request statement 9 / plan kind 9 handles two to eight authorized group keys, up to nine distinct key/COUNT(*) sort items, and LIMIT. WHERE, HAVING, and non-COUNT aggregates remain unsupported.
- Implementation: Rust decoding, authorized binding, ordinal-sorted scan planning, canonical kind-9 codec, optimizer process dispatch, and IPC plan validation are implemented. TinyCC encodes statement 9, decodes kind 9, matches sort sources/directions/NULL placement against v15 SQL, and offers an optimizer-gated v15 execution entry point that re-resolves relation identity before execution.
- Confined acceptance: the maintained ABI v5 SQL fixture routes `SELECT left_key, right_key, COUNT(*) ... ORDER BY ... LIMIT 2` through statement 9, matches the v15 AST to kind 9, re-resolves executor relation identity, runs the bounded v15 sorter, and commits the expected SQLR output through result relay.
- Verification: Rust kind-9 plan round-trip and invalid-sort-source regression passed; TinyCC exact request bytes, kind-9 decode, AST match, and mismatch rejection passed. `make -f engine/sdk/Makefile test sandbox-query-fixture` passed. The focused confined SQL DML sequence passed. Full locked workspace tests passed inside a delegated user service: 142 commit integration tests and all other test targets passed, every target reported zero ignored, including all cgroup-backed commit and metadata process tests. Build, format, warning-denied Clippy, ABI generation checks, and `git diff --check` passed.

## OP-7: bounded composite join request and authorized binding — in progress

- Contract: [optimizer](../../specs/planner-compiler.md). Decode a two-relation SELECT with stable left/right projections, two to eight equality keys, and an optional limit; bind both relations through the invocation's authorized planner metadata before planning.
- Red/green evidence: the request test initially failed to compile because the statement decoder and typed request records were absent. The maintained tests now cover a composite request, invalid projection side, key-count bounds, trailing bytes, left/right column-ID binding, incompatible key types, cross-schema relation shapes, and unavailable relations.
- Implementation: `decode_join_select_request` accepts statement 4 under the existing request version and size/identifier bounds. `bind_join_select_request_via_metadata` independently resolves each relation and maps projections/keys to stable metadata. It requires a common schema identity/version; equality keys require matching scalar types, with Decimal precision allowed to vary when scale matches. This binding conveys no executor capability.
- Verification: both focused request decoder and binding tests passed under the locked workspace toolchain. ABI constants are generated from `engine/abi/query_abi_constants.json` and include the new statement tag.
- Plan/process implementation: `JoinSelectPlanV1` is a distinct kind-4 encoding, preserving kinds 1–3. Both scans carry unique ordinal-sorted shapes; projections retain left/right side and source order; equality keys retain bound column metadata; and LIMIT is optional. Rust decoding validates common schema identity, relation identity, scan ordering/uniqueness, projection/key membership, compatible key types, bounds, and trailing bytes. `OptimizeJoinSelect` (`0x1207`) resolves both authorized relations through the planner process and returns only a validated plan. Optimizer response framing accepts all four plan kinds.
- Verification: plan codec round-trip/malformed-reference regression and the two-relation optimizer process regression pass. The process test verifies both metadata requests, stable relation names, scan shapes, two keys, and LIMIT. Generated ABI consistency, workspace build, format, warning-denied Clippy, maintained TinyCC tests/fixture builds, and the full serial workspace suite pass. The suite ran in a delegated service; commit integration reported 142 passed, zero ignored, and all cgroup-backed tests executed. `git diff --check` passed.
- SDK boundary: `prdb_sdk_optimizer_decode_join_plan_v1` independently decodes kind 4 and validates both scan shapes, ordered projections, key references/type compatibility, LIMIT, bounds, and canonical framing. `prdb_sdk_optimizer_validate_executor_join_relations_v1` re-resolves both plan references through normal executor storage IPC and matches schema/table identities, epochs, relation names, and scanned column definitions before any execution code receives the plan.
- Request adapter and execution gate: the SDK encodes the parsed alias-aware v19 INNER JOIN AST into request statement 4 and submits it through ABI v5's operation-selected `OptimizeJoinSelect` callback. It preserves projection sides/order, composite keys, and LIMIT; unsupported or malformed spans fail before callback dispatch. `prdb_sdk_optimizer_match_join_plan_v19_v1` checks returned relation names/schema, projection order/sides/types, composite key order/types, and LIMIT against the parsed and bound AST. `prdb_sdk_sql_execute_inner_join_optimizer_v1` submits the plan, binds against authorized planner shapes, matches the AST, re-resolves both relations through executor storage, and only then runs the matching bounded SDK join pipeline.
- Verification: TinyCC compiles and runs the maintained `sdk-optimizer-plan-v1` fixture; it checks a hand-built kind-4 plan, both-side metadata resolution, AST matching and a mismatched LIMIT, invalid projection side, mismatched key types, and trailing bytes. The optimizer fixture checks canonical statement-4 bytes, bounded output behavior, operation selection, and response decoding. `sdk-sql-execute-v19` exercises optimizer submission, plan matching, both executor identity lookups, and result equivalence on a composite join. The confined `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance now executes the two-key aliased v19 join through ABI v5 optimizer submission, validates the plan against SQL, re-resolves both relations, and commits the relayed result.

## OP-7: confined optimizer join acceptance — verified

- Red evidence: the first delegated acceptance failed with `Planner(DuplicateRequestId)` after earlier direct planner calls. Optimizer metadata lookups had reused optimizer-channel IDs on the shared planner stream; a two-relation lookup also needed distinct IDs for each side.
- Fix: optimizer planner requests now use a disjoint range above the direct planner request cap and allocate a unique ID per relation. The optimizer process regression asserts those IDs for SELECT, DELETE, UPDATE, and both join relations.
- Verification: optimizer process tests passed (7/7); the maintained TinyCC SDK suite and sandbox query fixture build passed. The focused confined DML sequence passed with the optimizer-backed join. Locked workspace build, format check, warning-denied Clippy, and the full serial workspace test suite passed in the delegated `systemd --user` unit; commit integration reported 142 passed, zero ignored, including all cgroup tests, and metadata process integration reported 2 passed. `git diff --check` passed.
- Remaining: broaden grouped optimizer planning beyond COUNT and gather analytical-load and mixed-workload measurements. The optimizer-gated single-key COUNT plus v12 ORDER BY composition is verified below; its sort remains in SDK execution.

## OP-7 dependency: bounded single-key COUNT(*) plan — Rust service implemented

- Contract: the first grouped optimizer plan accepts `SELECT key, COUNT(*) FROM relation GROUP BY key [LIMIT n]`; it carries only authorized stable relation identity and the one scanned key column. It has no WHERE, HAVING, or ORDER BY.
- Implementation: statement 5 (`OptimizeGroupCount`, `0x1208`) is bounded and decoded, binds the key against invocation-authorized metadata, and lowers to validated plan kind 5. The scan shape must equal the group key exactly; the optional LIMIT applies after grouping.
- Verification: request bounds/trailing bytes, authorized key binding, kind-5 round-trip/scan validation, and optimizer process dispatch are covered by four focused Rust regressions; they passed. ABI generation was refreshed and checked.
- TinyCC boundary: `prdb_sdk_optimizer_decode_group_count_plan_v1` independently validates kind 5, the exact one-column scan/group key, stable relation references, optional LIMIT, and canonical framing. The SDK encodes the v4 COUNT(*) AST as statement 5 and submits it through ABI v5's `OptimizeGroupCount` operation.
- SQL validation and execution gate: the SDK matches kind 5 against the parsed v4 COUNT(*) statement, its authorized grouping-key binding, schema/relation names, and optional LIMIT. It re-resolves the relation through executor storage and checks the stable relation identity and exact scanned column definition before running the bounded COUNT grouping scan.
- Confined acceptance: the maintained ABI v5 SQL artifact executes `SELECT id, COUNT(*) FROM records GROUP BY id` through optimizer operation `0x1208`, validates current executor identity, relays the exact SQLR result, and verifies it through durable commit in the existing cgroup-backed SQL sequence.
- Verification: TinyCC regressions cover valid kind-5 decode, scan/key mismatch, AST/LIMIT matching, canonical request bytes, operation selection, response decoding, and filtered-query rejection. `make -f engine/sdk/Makefile test sandbox-query-fixture` and the focused confined acceptance passed. Locked workspace build, format check, warning-denied Clippy, and the full serial workspace suite also passed in the delegated user service with cgroup tests enabled and zero ignored tests. Composite grouping, aggregates beyond COUNT(*), WHERE/HAVING, and ordering remain unsupported by this optimizer request subset.

## OP-7 dependency: bounded composite COUNT(*) request and plan — verified

- Contract: statement 6 extends the existing grouped COUNT optimizer operation to two through eight keys in source order, plus optional post-group LIMIT. It carries no filters, HAVING, or ordering.
- Implementation: the bounded Rust decoder, invocation-authorized binder, kind-6 plan codec, canonical ordinal-sorted scan shape, optimizer service, process dispatch, and response validation are implemented. TinyCC independently encodes the v13 AST, decodes and matches kind 6, and checks executor identity before the existing bounded composite grouping runner executes.
- Confined acceptance: the ABI v5 SQL artifact submits `SELECT left_key, right_key, COUNT(*) FROM grouped_records GROUP BY left_key, right_key LIMIT 2` through statement 6 / operation `0x1208`. The existing exact SQLR assertions pass through result relay and durable commit.
- Verification: focused Rust request bounds/trailing-byte, binding duplicate/order, plan codec/canonical scan, and optimizer process-channel tests passed (4 tests). TinyCC tests cover exact statement-6 request bytes, kind-6 scan/key validation, AST matching and LIMIT mismatch. `make -f engine/sdk/Makefile test sandbox-query-fixture` and the focused confined acceptance passed. Composite-group ordering and analytical workload measurements remain open.

## OP-7 dependency: bounded ordered SELECT plan — implemented

- Contract: statement 7 sends explicit projections, one source-column ORDER BY key, direction, and optional LIMIT through the existing `OptimizeSelect` operation. The order key may be hidden from the output projection.
- Implementation: Rust decodes and binds the request through invocation-authorized planner metadata and emits kind 7 with a canonical union scan shape, ordered projections, order-key metadata, direction, and LIMIT. TinyCC independently validates and matches the plan to the v3 SQL AST, re-resolves the relation through executor storage, then stable-sorts at most 128 authorized rows within 1 MiB before applying LIMIT.
- Regression: Rust request, binding, plan codec, and optimizer process tests cover statement 7. TinyCC covers exact request bytes, hidden-key plan decoding/matching, zero LIMIT without a scan, materialization exhaustion without output, and cancellation after 32 published rows before the next callback. The confined SQL artifact verifies descending hidden-key output through durable result relay.
- Verification: the full maintained SDK suite passed after the executor-bound regressions. The focused Rust optimizer regressions and delegated confined acceptance passed in the preceding ordered SELECT increment.
- Remaining: composite-group optimizer sorting and analytical/mixed-workload measurements remain open.

## OP-7 follow-up: optimizer grouped COUNT sort plan — verified

- Contract: a one-key `COUNT(*)` query with v12's one- or two-output ORDER BY validates its sort sources, direction, NULL placement, LIMIT, and authorized group-key identity before emitting SQLR results. This subset rejects WHERE and HAVING.
- Implementation: request statement 8 and kind-8 plans encode a unique ordered list of group-key/COUNT(*) sort items. Rust bounds, binds, dispatches, and canonically validates the plan. The C SDK independently encodes the v12 AST, decodes kind 8, matches sort items and LIMIT, and re-resolves executor identity before invoking the bounded grouped runner.
- Confined acceptance: the maintained ABI v5 query artifact routes `SELECT id, COUNT(*) ... ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS LAST LIMIT 1` through the kind-8 optimizer operation. The test checks the exact relayed SQLR result and durable commit. The TinyCC request regression separately covers `NULLS FIRST`.
- Verification: four focused Rust request/plan/service/process tests passed; the full TinyCC SDK suite passed exact statement-8 bytes, kind-8 decoding, AST matching, and mismatch rejection. The fixture build and delegated `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed. Workspace build, format check, warning-denied Clippy, and the full serial workspace suite passed in the delegated user service; every target reported zero ignored tests, including all four cgroup-backed commit tests and both metadata process tests. `git diff --check` passed.
- Remaining: grouped sort plans for composite groups and broader analytical/mixed-workload measurements remain open.

## OP-2 dependency: generated unknown estimate response — implemented

- Contract: when an authorized estimate is unavailable or exceeds its allowance, planner IPC returns the exact versioned unknown response and keeps the invocation usable.
- Implementation: the shared ABI schema now generates the unknown response encoder/decoder, and [`planner_ipc`](../../engine/src/planner_ipc.rs) uses them instead of duplicating version/status bytes. Exact estimate tails remain bounded and schema-driven.
- Regression: `planner_stream_estimate_allowance_falls_back_without_aborting_invocation` verifies the unknown estimate is decoded and the invocation continues.
- Verification: ABI drift/schema tests, focused fallback regression, locked workspace build, format check, warning-denied Clippy, serial workspace tests (129 passed, 3 cgroup-dependent ignored), and maintained TinyCC SDK suite passed in the configured Nix shell.
- Remaining: optimizer binding/planning integration and execution remain open as described below.

## OP-3 ABI v4 SQL callback fixture — implemented; confined acceptance verified

- Behavior: the maintained SQL write/read fixture now exports both v3 and v4. The v4 entry submits a parameterized `id = 42` SELECT through the executor's fixed optimizer callback, validates the optimizer Filter against the AST and parameter, independently resolves the relation through executor storage, and checks the projected row.
- Verification: TinyCC SDK tests and sandbox-fixture compilation pass. The actual fixture packages successfully as ABI v4 without loading; the package test checks exact descriptor/entry symbols. Workspace build, format, warning-denied Clippy, and tests pass (129 commit tests; 3 cgroup tests ignored).
- Remaining: bounded mutation plans remain open.

## OP-1 dependency: bounded structured SELECT request decoder

- Contract: [optimizer specification](../../specs/planner-compiler.md). The first request payload is a versioned, single-relation SELECT with explicit projections, optional bounded predicate tree, and optional LIMIT.
- Implementation: [`optimizer_request`](../../engine/src/optimizer_request.rs) decodes identifiers, record lengths, predicate tags and values into owned Rust data. It caps requests at 64 KiB, predicates at 16 KiB, projections at 128, text literals at 4,096 bytes, predicate nodes at 256, depth at 32, and boolean fan-in at 16. Unknown versions/statements/nodes, invalid booleans/operators/identifiers, truncation, and trailing bytes fail closed.
- Boundary: the decoder performs structural validation only. It has no storage access and does not bind names or types, authorize relations, choose plans, or grant execution authority. The C parser and worker channels do not call it yet.
- Regression: four unit tests cover a compound typed request, malformed outer records/identifiers, excessive collection bounds and unsupported nodes, and predicate byte/node/depth exhaustion.
- Verification: locked workspace build and format passed; warning-denied Clippy passed; serial locked workspace tests passed (129 commit integration tests and all other active suites passed; 3 cgroup-dependent commit tests and 1 disconnected-submission process test were ignored). `git diff --check` passed.
- Remaining: define and validate type semantics against authorized metadata, return only non-disclosing bind failures, produce and validate a logical/physical plan, integrate a bounded isolated optimizer process and its supervisor-bound submission channel, and execute plans through SDK operations.

## OP-2 dependency: bind SELECT against authorized relation shape

- Implementation: [`optimizer_binding`](../../engine/src/optimizer_binding.rs) decodes the v1 request, resolves its exact qualified name through the invocation-bound `PlannerMetadataConnection`, and binds schema ID/version, table ID/write epoch, and projection/predicate column IDs, ordinals, types, and nullability. It validates relation shape and expression type compatibility. Missing descriptions and relation-name mismatches both return `RelationUnavailable`.
- Boundary: the helper receives no principal, grant, raw catalog, or storage handle and creates no executor permission. Its only metadata source is the planner connection opened on the admitted transaction.
- Regression: three unit tests cover stable reference binding, non-disclosing relation absence/name mismatch, and unknown-column/type-incompatible rejection. A real-redb integration regression binds an authorized user table, checks schema ID/version, table ID/epoch, column ID/shape, and confirms a missing relation maps to `RelationUnavailable`.
- Verification: focused binder tests, the focused planner stream/metadata integration regression, locked workspace build, formatting, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (132 commit integration tests passed; 3 cgroup-dependent commit tests and 1 process test ignored).
- Remaining: define additional literal coercions/SQL NULL semantics. Isolated optimizer process, versioned plan output, and supervisor-bound submission channel remain open.

## OP-3 dependency: lower a bound SELECT to a validated plan pipeline

- Implementation: [`optimizer_plan`](../../engine/src/optimizer_plan.rs) creates and canonically encodes a linear scan/filter/project/limit plan with schema ID/version, relation ID/write epoch, and stable column references. The scan reads the unique ordinal-sorted union of projection and predicate columns; output projection order and duplicates are preserved. The plan contains no planner handle, grant, or commit operation.
- Validation: `validate_plan_v1` and the byte decoder check the pipeline shape, relation identity, unique sorted scan columns, projection membership, filter references/types, and predicate depth/node/fan-in/text bounds. Column references in predicates resolve only through the decoded scan shape. The SDK and executor must repeat plan validation after channel transfer.
- Regression: eight unit tests verify pipeline construction/order, scan-column union, malformed order/duplicate limits, invalid filters/references/depth/types, canonical byte round-trip, and malformed version/type/truncated/trailing payload rejection.
- Verification: focused optimizer plan tests passed. Locked workspace build, format, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (129 commit integration tests passed; 3 cgroup-dependent commit tests and 1 process test ignored).
- Remaining: assign/transfer the plan over the supervisor-bound channel, independently re-resolve the relation through executor storage, interpret the plan through the SDK, and implement mutation plans and optimizer process isolation.

## OP-3 dependency: compose invocation-bound optimizer service

- Behavior: one library entry point decodes, resolves and binds a structured request through the invocation's planner metadata connection, lowers it, and returns canonical plan bytes without transferring planner handles or authority.
- Implementation: [`optimizer_service`](../../engine/src/optimizer_service.rs) composes the existing binder and plan validator/encoder. The real-redb planner regression now invokes it, decodes the returned plan, and checks schema/table IDs and pinned epochs; unavailable relations map to a non-disclosing bind result.
- Verification: locked workspace build, format, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (129 commit integration tests passed; 3 cgroup-dependent tests ignored).
- Remaining: bounded submission IPC, isolated optimizer process and supervisor channel, independent executor-side relation resolution, SDK interpretation, and mutation plans.

## OP-3 dependency: define optimizer submission payload and role

- Behavior: reserve one role-specific operation for SELECT submissions and accept a successful response only when it contains a complete independently validated plan; errors use a small non-disclosing status set.
- Implementation: operation `0x1203`, generated Rust/C constants, `OptimizerSubmission` role allowlisting, and canonical response payload encoder/decoder are defined in `optimizer_ipc`. Request structure remains the canonical SELECT v1 payload and its 64 KiB decoder. This only defines and validates payloads; no socket handler or executor callback is connected yet.
- Verification: generated ABI files matched the schema. Locked workspace build, format, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (129 commit integration tests passed; 3 cgroup-dependent tests ignored; 81 Rust unit tests passed).
- Remaining: connect the submission socket to `optimize_select_v1`, launch the optimizer in its own process, independently re-resolve plan references through the executor channel, and interpret plans in the SDK.

## OP-3 dependency: serve optimizer submissions over a bounded socket

- Behavior: a provisioned optimizer-submission stream validates request IDs and role, returns canonical plan payloads, and gives the same unavailable status for hidden/missing relations. Malformed framing, duplicate IDs, limit exhaustion, transport failures, and storage transaction errors abort the invocation.
- Implementation: [`prdb-optimizer`](../../engine/src/bin/prdb-optimizer.rs) serves submissions in a separate process using only inherited submission and planner sockets. [`optimizer_process`](../../engine/src/optimizer_process.rs) validates each request, resolves stable authorized relation metadata through the planner socket, builds a plan, and validates the response before writing it. The previous in-process stream handler was removed. A real-redb subprocess integration regression checks successful and unavailable lookups.
- Boundary: the isolated process now has production supervisor launch, OS confinement, aggregate invocation resource limits, and a distinct planner metadata channel. The executor callback/channel is provisioned but not yet exposed through the query ABI.
- Verification: focused real-redb optimizer subprocess and protocol-loop tests passed. Generated ABI consistency, locked workspace build, format, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (132 commit integration tests passed; 3 cgroup-dependent tests and 1 disconnected-submission process test ignored; 82 Rust unit tests passed).
- Remaining: expose optimizer submission through the executor query ABI, independently re-resolve plans in storage, and execute plans in the SDK.

## OP-3 dependency: C SDK optimizer response envelope — implemented

- Behavior: the C SDK can submit an already encoded bounded SELECT request through the ABI v4 optimizer callback and decode the canonical response version/status/length envelope. Successful responses expose borrowed plan bytes; errors expose no plan. Malformed versions, statuses, lengths, trailing bytes, oversized plans, and overlapping output/payload records are rejected.
- Implementation: [`prdb_sdk_optimizer_v1.h`](../../engine/include/prdb_sdk_optimizer_v1.h) and [`optimizer_v1.c`](../../engine/sdk/optimizer_v1.c) provide the status decoder and callback wrapper. The result record borrows plan bytes from the caller's response buffer.
- Regression: [`sdk_optimizer_v1.c`](../../engine/tests/fixtures/sdk_optimizer_v1.c) checks successful plans, unavailable status, callback I/O errors, response capacity behavior, and malformed payload rejection under TinyCC.
- Limitation: this step does not serialize a parsed SQL AST, validate plan structure, resolve relation IDs through executor storage, or execute plan operators.
- Verification: the full maintained TinyCC SDK suite passed, including this fixture. Workspace build, formatting, warning-denied Clippy, serial Rust tests, generated ABI check, and `git diff --check` also passed (129 commit integration tests; three cgroup-dependent tests ignored).
- Remaining: serialize the supported SQL AST into canonical optimizer requests, independently decode/validate plans, re-resolve the relation, and route physical operators through SDK executor calls.

## OP-3 dependency: encode simple SQL ASTs as optimizer requests — implemented

- Behavior: the C SDK serializes an explicit-column, single-relation SQL v2 SELECT AST into canonical optimizer request v1, with a caller-supplied schema and optional LIMIT.
- Boundary: `SELECT *` and every WHERE tree return `UNSUPPORTED`; the encoder does not silently discard either projection or predicate semantics. It checks ABI/version/AST shape, identifier and span bounds, request size, and output overlap; undersized buffers receive only the required length.
- Regression: the TinyCC optimizer fixture parses `SELECT name FROM items LIMIT 7;`, compares the encoded bytes to the canonical request, checks undersized-buffer behavior, and verifies star and parameterized predicates are rejected.
- Verification: the targeted fixture and full maintained TinyCC SDK suite passed. Rust 1.95 locked workspace build, format check, warning-denied Clippy, and serial workspace tests passed (82 library tests and 129 commit tests passed; 3 cgroup-dependent commit tests and 1 cgroup-dependent process test were ignored). Generated ABI consistency and `git diff --check` passed.
- Remaining: independently decode and validate plans in the executor, re-resolve plan relation identities through executor storage, and execute supported plan operators.

## OP-3 dependency: independently decode basic plans in the C SDK — implemented

- Behavior: the C SDK independently decodes canonical optimizer plan v1 for `Scan -> Project -> optional Limit`.
- Boundary: it validates framing, nonzero stable relation/column IDs and epochs, ASCII identifiers, scalar type encodings, scan-column ordering/uniqueness, projection membership, node order, and trailing bytes. Relation names borrow the input payload; column records are copied into the result. Filter nodes return `UNSUPPORTED` until their bounded prefix trees receive independent structural and type validation.
- Regression: the TinyCC fixture checks a hand-constructed plan, borrowed names and copied column metadata, projection-reference rejection, filter rejection, trailing-byte rejection, and undersized output records.
- Verification: targeted fixture and full maintained TinyCC SDK suite passed. Rust 1.95 locked workspace build, format check, warning-denied Clippy, and serial workspace tests passed (82 library tests and 129 commit tests passed; 3 cgroup-dependent commit tests and 1 cgroup-dependent process test were ignored). Generated ABI consistency and `git diff --check` passed.
- Remaining: decode/type-check filter predicates, independently resolve plan IDs through executor storage, and interpret validated nodes through SDK calls.

## OP-3 dependency: validate bounded filter plans in the C SDK — implemented

- Behavior: `prdb_sdk_optimizer_decode_plan_with_filter_v1` accepts the full canonical plan v1 pipeline and returns a borrowed raw prefix predicate view alongside the decoded scan/project records.
- Boundary: the decoder validates predicate tags and operators, UTF-8/text limits, column IDs against the scan shape, comparison operand compatibility, Boolean fan-in and NOT operands, depth 32/node 256 bounds, and a Boolean root. The existing basic decoder keeps its stable output record and returns `UNSUPPORTED` for filtered plans; callers use the new explicit filter-view entry point.
- Regression: TinyCC fixtures cover a valid nested AND/NOT comparison, invalid root type, incompatible comparison operands, invalid UTF-8, filter absence, and output capacity errors.
- Verification: targeted fixture and full maintained TinyCC SDK suite passed. Rust 1.95 locked workspace build, format check, warning-denied Clippy, and serial workspace tests passed (82 library tests and 129 commit tests passed; 3 cgroup-dependent commit tests and 1 cgroup-dependent process test were ignored). Generated ABI consistency and `git diff --check` passed.
- Remaining: independently resolve plan IDs through executor storage, interpret validated nodes through SDK calls, and connect the plan decoder to optimizer submission results.

## OP-3 dependency: expose stable authorized plan relation metadata

- Behavior: the optimizer can resolve exact qualified names to a pinned schema/table identity and declared column shape without receiving a storage handle or executor capability.
- Implementation: planner operation `describe_plan_relation` (`0x1204`) calls the invocation-bound metadata resolver directly and returns schema ID/version, table ID/write epoch and authorized columns. It does not allocate a planner handle. The existing planner channel regression checks returned identities against the real-redb catalog; role tests reject the operation on the optimizer submission role.
- Verification: focused planner channel regression passed. Generated ABI consistency, locked workspace build, format, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (129 commit integration tests passed; 3 cgroup-dependent tests ignored; 81 Rust unit tests passed).
- Remaining: production process launch now gives the optimizer separate submission and planner metadata sockets; executor callback integration, executor-side re-resolution, and SDK interpretation remain.

## OP-3 dependency: re-resolve stable plan identity through executor storage

- Behavior: the executor independently checks a decoded plan's qualified relation against the same invocation-pinned authorized catalog and returns stable schema/table/column identities without creating a storage handle.
- Implementation: executor operation `resolve_plan_relation` (`0x1105`) reuses the versioned qualified-name request and the planner `describe_plan_relation` response encoding. Shared identity assembly validates catalog schema metadata for both roles. Executor-role allowlisting permits the operation while planner-role use is rejected; a real-storage IPC regression verifies the pinned identity and that hidden/missing relations are indistinguishable. C helper `prdb_sdk_optimizer_validate_executor_relation_v1` re-decodes the full plan, requests this metadata, and checks all stable identities plus each scanned column's ID, ordinal, type parameters, and nullability.
- Regression: TinyCC fixture checks successful identity matching, stale epoch rejection, scanned column ID mismatch, unavailable relation, and undersized response handling.
- Verification: Rust workspace and TinyCC suite passed for the preceding IPC increment. The current C helper's focused fixture passed; full workspace/TinyCC validation and diff checks are pending for this increment.
- Remaining: resolve the executor scan capability only after identity match, then interpret the validated operators.

## OP-3 dependency: acquire executor relation capability after plan identity validation

- Behavior: a plan receives an ordinary executor relation handle only after its stable identity and scanned shape match metadata resolved from the invocation's authorized snapshot.
- Implementation: `prdb_sdk_optimizer_acquire_plan_relation_v1` invokes the independent metadata validator first, then calls executor `resolve_relation` (`0x1101`) and checks that the returned relation ID/name still match the plan before reporting success. It returns the normal connection-local executor handle and full declared column shapes for subsequent scan setup.
- Regression: TinyCC fixture verifies both IPC calls on success, the returned kind-6 handle and relation shape, and rejection if the capability response returns a different relation ID.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: open a scan using the validated capability and interpret bounded plan operators.

## OP-3 dependency: open a scan only after plan validation

- Behavior: the SDK validates a physical plan through executor metadata, acquires the ordinary executor handle, and opens its full scan. A zero LIMIT produces an empty result without creating a cursor.
- Implementation: `prdb_sdk_optimizer_open_plan_scan_v1` composes plan decoding, stable-identity comparison, executor relation resolution, and full-scan open. It returns the checked relation metadata and connection-local cursor token in caller-owned buffers.
- Regression: TinyCC fixture verifies the metadata and capability requests precede `open_scan`, the returned cursor has kind 7, and LIMIT 0 performs no cursor request.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: consume bounded batches, evaluate Filter, project rows, honor nonzero LIMIT, and emit SQL results.

## OP-3 dependency: consume projected rows from bounded scan batches

- Behavior: a bounded C executor runs an unfiltered plan over immutable storage batches, emits projection-ordered values, polls cancellation, stops at LIMIT, and closes early cursors. Filter plans fail as unsupported before scan creation.
- Implementation: `prdb_sdk_optimizer_execute_unfiltered_plan_v1` composes validated metadata/capability/scan setup with a 126-page budget so two IPC calls per page plus setup and an early close stay within the 256-request executor stream limit. The SQL optimizer bridge additionally spends one request checking the returned plan against the submitted AST, so its effective page ceiling is 125. It polls cancellation, applies the stateful projection/LIMIT batch consumer, and closes the cursor on cancellation or early stop. Values borrow the current batch only during the result callback.
- Regression: TinyCC fixture checks source-ordinal projection, callback value/type, limit completion, request order, cursor close after early LIMIT, cancellation abort/cleanup, and rejection of filtered plans.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining at this increment: evaluate Filter predicates, encode and emit SQL results, and add a real executor channel integration covering the plan-driven batch path. Filter evaluation is implemented in the following increment.

## OP-3 dependency: evaluate bounded Filter plans over scanned rows

- Behavior: the SDK evaluates validated plan predicates against each immutable scanned row before projection. SQL three-valued logic is preserved, including short-circuit behavior for AND/OR; only TRUE rows are emitted. Predicate errors such as REDACTED inputs fail plan execution and trigger normal cursor cleanup.
- Implementation: `prdb_sdk_optimizer_consume_plan_batch_v1` decodes the bounded prefix tree, resolves scanned columns by stable ID and ordinal, evaluates scalar comparisons through the shared SQL evaluator, and integrates with the bounded executor scan loop. False and UNKNOWN rows do not count toward LIMIT.
- Regression: TinyCC fixture checks that a matching row emits, a nonmatching row is skipped, and the bounded executor completes a Filter scan with early LIMIT and cursor close.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: submit WHERE predicates through the SQL optimizer bridge and add a real process/channel execution fixture. SQLR row emission is implemented in the following increment.

## OP-3 dependency: connect optimizer submission to unfiltered SQL execution

- Behavior: a parsed explicit-projection SELECT is encoded and submitted through ABI v4, its successful plan is checked against the submitted relation/projection order/LIMIT and executor-authorized column names, and the unfiltered plan is run through bounded storage pages. Unsupported predicates return to the existing SQL path without latching statement failure; optimizer or execution failures latch failure.
- Implementation: `prdb_sdk_optimizer_execute_select_ast_v1` composes canonical request encoding, the dedicated optimizer callback, response decoding, query-to-plan semantic checks, and the bounded executor plan runner. The extra metadata request is included in the 125-page budget.
- Regression: TinyCC fixture submits parsed SELECTs, checks optimizer/executor callback order and one projected result, accepts case-folded relation/projection spellings, rejects a changed projection before scanning, verifies WHERE fallback makes no optimizer call, and checks that optimizer error status latches statement failure.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: submit WHERE predicates through the SQL optimizer bridge, encode/emit SQL results, and exercise the path with a real optimizer/executor IPC invocation. Returned unfiltered plans are compared with the submitted SELECT and emitted as SQLR rows; the bridge leaves WHERE queries on the established SQL path.

## OP-3 dependency: launch and confine the optimizer with each invocation

- Behavior: production START launches optimizer and executor as separate confined processes in one invocation cgroup. The optimizer receives only its submission and planner sockets; storage serves its planner socket alongside executor planner and storage channels. Either launch failure cleans up the other process and invocation resources.
- Implementation: `ExecutorLaunchConfig` and `prdb-supervisor --optimizer` select the optimizer executable. The launcher stages independent command policies, starts and reaps both children, and retains the optimizer endpoint in the executor. Storage drains three worker channels concurrently and aborts all peers on handler failure. The optimizer installs `no_new_privs` and the worker seccomp filter before serving.
- Verification: the serial workspace suite passed (82 library unit tests; 129 commit integration tests; three cgroup-dependent commit tests and one cgroup-dependent process test ignored). After the final child-reaping change, workspace build, formatting, warning-denied Clippy, and all four launcher unit tests passed. Generated ABI consistency and `git diff --check` passed. End-to-end confined launch remains unverified because the host has no delegated cgroup test fixture.
- Remaining: route an optimizer request from the executor ABI, validate returned plans again against executor storage, and interpret SELECT plans through SDK operations.


## OP-3 dependency: emit projected optimizer rows as SQLR records

- Behavior: the supported optimizer SELECT path writes each projected row as a provisional SQLR v1 record using executor-authorized column names and declared types. Result callback failures fail the statement so the invocation cannot commit partial output.
- Implementation: `prdb_sdk_sql_write_projected_result_v1` encodes and writes a projected single-relation row in host-sized chunks. `prdb_sdk_optimizer_execute_select_ast_to_result_v1` adapts bounded plan execution to that writer.
- Regression: TinyCC fixture executes a submitted SELECT through ABI v4, captures the SQLR record, checks its projection name/type/state/bytes, and verifies a failed result callback latches statement failure.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: submit WHERE predicates through the optimizer bridge and add a real process/channel execution fixture.


## OP-3 dependency: encode bounded SQL predicates in optimizer requests

- Behavior: callers can serialize parsed single-relation SELECT predicates and supplied positional values into the existing optimizer request v1 format without changing the ABI wire version. Unsupported forms remain eligible for the existing SQL path.
- Implementation: `prdb_sdk_optimizer_encode_select_with_parameters_v1` serializes comparisons, Boolean AND/OR/NOT trees, and Boolean/INTEGER/TEXT/NULL values into the bounded prefix representation expected by the Rust decoder. The compatibility encoder continues to reject WHERE trees.
- Regression: TinyCC fixture checks an exact comparison request payload and rejects missing parameters; unfiltered request encoding remains byte-for-byte stable.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: route the validated parameterized bridge through a real optimizer/executor process and channel integration.


## OP-3 dependency: submit and execute validated parameterized WHERE plans

- Behavior: supported parameterized SELECT predicates are submitted to the optimizer, and the SDK rejects a returned Filter unless its operator tree, scanned column names, comparison operators, and values match the submitted AST and parameters. Matching plans execute through bounded filtered scans and can emit SQLR rows.
- Implementation: `prdb_sdk_optimizer_execute_select_ast_with_parameters_v1` composes parameterized request encoding, ABI v4 optimizer submission, executor metadata validation, AST-to-Filter comparison, filtered batch execution, and sticky statement failure. `prdb_sdk_optimizer_execute_select_ast_with_parameters_to_result_v1` emits provisional SQLR records for matching rows.
- Regression: TinyCC fixture checks successful filtered submission/execution/result emission and rejects a plan whose literal differs from the supplied parameter. Unsupported predicates remain on the existing SQL path.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: exercise the path through the confined optimizer/executor processes and channels; add bounded mutation plans.

## OP-3 dependency: verify filtered optimizer process output against executor storage

- Behavior: a parameterized equality SELECT crosses the real optimizer subprocess submission and planner channels; the returned Filter retains its bound column identity and literal, and executor storage independently resolves the plan's qualified relation and scanned column shape over its own IPC channel. The fixture then obtains a normal relation handle, opens a scan using the plan's scanned column IDs, and consumes a finished bounded batch.
- Regression: `planner_stream_estimate_allowance_falls_back_without_aborting_invocation` submits both an unfiltered request and `value = 7` with LIMIT, checks the decoded Filter node, compares schema/table identity plus every scanned column's ID, ordinal, type, and nullability with executor metadata, and confirms the empty fixture table returns no rows and no continuation. The existing unavailable-relation check remains on the optimizer process stream.
- Verification: focused Rust process/channel integration and format check passed. Workspace build, formatting, and warning-denied Clippy passed; the full workspace test run hit `InvalidAudit` in `storage_manager_creates_and_drops_check_constraint`, which passed alone, and the serial commit integration suite passed (129 passed, 3 cgroup-dependent tests ignored). The new scan assertion is being verified separately.
- Remaining: run the C SDK callback, Filter evaluation, and result path against a real confined optimizer/executor invocation; add bounded mutation plans.

## OP-1 / OP-3 dependency: malformed optimizer requests fail without stopping the process

- Behavior: a structurally malformed request receives `InvalidRequest` without reaching planner metadata, and the same optimizer subprocess remains able to serve a later valid request.
- Regression: `planner_stream_estimate_allowance_falls_back_without_aborting_invocation` sends an empty SELECT payload to the real optimizer child, verifies the status and request ID, then proceeds through valid unfiltered and filtered submissions plus the unavailable-relation case. `optimizer_process::tests` sends a malformed IPC frame, a duplicate request ID after an `InvalidRequest` response, and 256 invalid requests followed by request 257, checking frame rejection, duplicate-ID rejection, and the connection request ceiling.
- Verification: the focused process-loop tests and real-process integration passed. Locked workspace build, formatting, warning-denied Clippy, and full tests passed; 3 cgroup-dependent commit tests and 1 cgroup-dependent process test were ignored.
- Remaining: run the SDK callback, Filter evaluation, and result path in a confined invocation; add bounded mutation plans.

## OP-3 dependency: execute the filtered SQL callback through confined optimizer and executor processes — verified

- Contract: a parameterized `SELECT id FROM records WHERE id = $1 LIMIT 1` must pass through the real optimizer submission process, be independently checked against authorized executor metadata, scan through the executor channel, and return the matching SQL value from the confined artifact.
- Regression: `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` runs the maintained ABI-v4 `sql_v3_write_read.c` fixture. Its v4 entry submits the equality predicate with parameter 42, confirms the returned row after prior insert/update operations in the same invocation, and reports the result through the normal callback path.
- Verification: the ignored acceptance passed under a transient `systemd --user` unit with delegated `cpu memory pids`, using the built TinyCC fixture and configured glibc/libgcc runtime paths. The full acceptance also checked its SQLR result frames and durable COMMITTED outcome after terminal delivery failed. `sandbox-query-fixture` was already current; the test ran exactly as `cargo test --locked --test commit sandboxed_v3_sql_dml_sequence_is_visible_and_commits -- --ignored --exact --nocapture` inside the delegated unit.
- Remaining: bounded mutation plans and broader optimizer planning remain open.

## OP-3 dependency: define and validate the canonical bounded DELETE plan — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md), mutation-plan format. A DELETE plan carries an authorized logical relation, a Boolean WHERE predicate, the minimal sorted scan shape, and a Delete sink; it never carries an executor handle or write authority.
- Implementation: `DeletePlanV1`, `plan_delete_v1`, `encode_delete_plan_v1`, `decode_delete_plan_v1`, and `validate_delete_plan_v1` implement the canonical v1 `Scan -> Filter -> Delete` layout. Constant predicates encode zero scan columns to request the full declared row shape. Predicate column collection now enforces the existing node/depth bounds.
- Red/green evidence: the round-trip test first failed to compile because the DELETE planner/codec did not exist; it now verifies a two-column sorted scan and canonical round-trip, rejects a corrupted Delete sink, exercises constant Boolean predicates, and rejects non-Boolean or over-depth predicates.
- Verification: all 19 `optimizer_plan::tests` passed in the locked Nix shell. The hand-checked-byte round-trip test was rerun exactly after its expected payload was added. Workspace build, format check, warning-denied Clippy, and full locked tests passed (130 library unit tests; 4 cgroup-dependent tests ignored by default; active integration and doc tests passed). `git diff --check` passed.
- Remaining: bind a versioned mutation request through the invocation metadata connection, serve it through the isolated optimizer process, decode it independently in the SDK, and execute it through the normal authorized mutation channel. UPDATE plans and bounded mutation-planning integration remain open.

## OP-3 dependency: decode and bind bounded DELETE optimizer requests — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md), structured request v1. DELETE requests must carry one bounded Boolean WHERE tree, resolve only within the invocation's authorized metadata view, and create no executor capability.
- Wire format: generated statement tag `PRDB_OPTIMIZER_DELETE_STATEMENT=2`; the v1 header is followed by schema/relation identifiers and a nonempty length-prefixed predicate. It has no projection or LIMIT fields and reuses the established predicate node, text, depth, and request-size bounds.
- Implementation: `decode_delete_request`, `bind_delete_request`/`bind_delete_request_via_metadata`, and `optimize_delete_v1`/`optimize_delete_v1_with_relation` decode, resolve and type-check the predicate, then return the canonical DELETE plan bytes from the previous increment. The shared generated Rust/C ABI constant is derived from `engine/abi/query_abi_constants.json`.
- Red/green evidence: decoder and binder tests initially failed to compile because their entry points were absent. The regressions now verify a parameter-free integer equality predicate, stable authorized column IDs, missing/mismatched relation non-disclosure, Boolean-root enforcement, mandatory WHERE, and rejection of trailing request bytes. The service regression decodes the output and compares its relation, scan column, and bound predicate.
- Verification: the focused request, binder, and service tests passed; `engine/tools/generate_query_abi.py --check` passed. The full workspace build, format check, warning-denied Clippy, and locked tests also passed; four cgroup-dependent tests were ignored by default. Host Nix 2.34.9 rejects `nix develop --locked`, so checks used `nix develop -c` and the repository's `flake.lock`.
- Remaining: add a dedicated optimizer IPC operation and response validation for kind-2 plans, then add C SDK request/decoder and execution integration. UPDATE request and plan support remains open.

## OP-3 dependency: serve DELETE plans through optimizer process IPC — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). DELETE uses a distinct optimizer submission operation and the existing invocation-bound planner metadata channel. The process returns only a canonical validated plan; storage remains the mutation and authorization boundary.
- Wire operation: generated `PRDB_IPC_OP_OPTIMIZE_DELETE=0x1205`, separately allowlisted for the `OptimizerSubmission` role. The process decodes the request before metadata lookup, routes to the DELETE service helper, and keeps operation/request IDs on the response.
- Response validation: the existing bounded optimizer response envelope now independently accepts either a canonical SELECT plan or canonical DELETE plan, rejecting malformed payloads as before.
- Evidence: the process regression covers metadata lookup, DELETE dispatch, response IDs, and kind-2 plan decoding; response round-trip covers both plan kinds; IPC tests assert the operation code and role authorization.
- Verification: focused optimizer-process and IPC tests passed. Full workspace build, format check, warning-denied Clippy, and locked tests passed; four cgroup-dependent cases were ignored by default. ABI generator `--check` and `git diff --check` passed. As recorded above, host Nix 2.34.9 requires `nix develop -c` rather than the unsupported `nix develop --locked` form; the repository flake lock remains in use.
- Remaining: C SDK DELETE execution integration, then UPDATE request/plan support.

## OP-3 dependency: independently decode DELETE plans in the C SDK — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). The SDK must independently validate plan kind 2 and the exact `Scan -> Filter -> Delete` pipeline before any execution integration.
- Implementation: `prdb_sdk_optimizer_decode_delete_plan_v1` validates identities, zero-to-256 sorted scan columns, the required bounded Boolean filter tree, Delete sink, node count, and complete payload. Constant predicates correctly allow an empty scan shape. Existing SELECT decoder entry points remain SELECT-specific.
- Evidence: the TinyCC fixture accepts a constant-Boolean DELETE plan and checks output kind/shape and borrowed predicate bytes; it rejects kind mismatches, incorrect terminal nodes, and malformed Boolean literals.
- Verification: `nix develop -c make -f engine/sdk/Makefile test` passed, including ABI generator checks, TinyCC SDK builds, and all maintained SDK executables. Workspace Rust checks passed in the immediately preceding process-IPC increment; this increment changes only C SDK and plan documentation.
- Remaining: integrate DELETE execution with independently resolved executor handles and ordinary authorized scan/mutation IPC; then continue UPDATE request and plan support.

## OP-3 dependency: add operation-aware optimizer callback ABI v5 — implemented

- Contract: [query worker ABI](../../specs/query-worker.md), [client SDK ABI](../../specs/client-sdk.md), and [planner specification](../../specs/planner-compiler.md). ABI v4 remains fixed to SELECT; DELETE submission requires a new ABI record so callers cannot reinterpret the old callback.
- Implementation: ABI v5 preserves the complete v4 host table prefix and adds an operation parameter. The confined executor validates the v5 descriptor before dispatch, supplies both callbacks, and applies the existing `OptimizerSubmission` role allowlist. PMF1 encoders/validators accept v5 with its exact symbol pair.
- Evidence: Rust ABI layout and descriptor validation, operation forwarding/denial, ELF manifest validation, and TinyCC C layout/callback fixtures pass. The v4 fixed-operation fixture still passes.
- Verification: workspace build, formatting, warning-denied Clippy, full locked tests, ABI generator check, and TinyCC SDK tests passed.
- Remaining: execute matching row handles through authorized scan/stage-delete IPC.

## OP-3 dependency: encode and submit bounded C DELETE requests — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). Parsed DELETE statements require a bounded Boolean WHERE tree; the SDK callback grants only the `OptimizeDelete` submission operation.
- Implementation: `prdb_sdk_optimizer_encode_delete_v1` writes canonical statement tag 2 requests with schema/relation identifiers and a mandatory prefix predicate. `prdb_sdk_optimizer_submit_delete_v1` uses ABI v5's operation-selected callback and validates the bounded optimizer response envelope.
- Coverage: the maintained TinyCC fixture checks exact wire bytes, missing parameter rejection, unsupported `IS NULL`, no partial output on a short buffer, null output-buffer validation, callback operation selection, and successful response decoding.
- Verification: the pinned Nix shell's maintained SDK suite and ABI generator checks passed. Workspace build, format check, warning-denied Clippy, and full locked tests passed (four cgroup-dependent tests ignored by default). The initial test attempt inherited a root-only Nix temporary directory and failed unrelated permission-sensitive tests; rerunning with `TMPDIR=/tmp` as the repository user passed.
- Remaining: execute matched row handles through authorized scan/stage-delete IPC. UPDATE request and plan support remains open.

## OP-3 dependency: verify DELETE plans against the SQL AST — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). A returned plan must match the submitted relation, predicate columns, operators, and bound parameter values before its relation capability is acquired.
- Implementation: `prdb_sdk_optimizer_validate_delete_plan_with_parameters_v1` independently decodes the kind-2 plan, compares its filter tree to the parsed DELETE AST using authorized planner metadata, and checks stable relation identity. The shared relation validator and relation-acquisition path now decode either SELECT or DELETE plans.
- Coverage: the TinyCC fixture accepts a matching integer predicate, rejects a changed parameter and a changed column, and acquires the independently resolved relation handle for the DELETE plan.
- Verification: focused plan fixture, maintained TinyCC SDK suite, and ABI generator checks passed in the pinned Nix shell.
- Remaining: scan authorized rows, evaluate the verified filter, stage matching row handles through executor mutation IPC, and add confined process acceptance. UPDATE request and plan support remains open.

## OP-3 dependency: execute verified DELETE plans through SDK mutation IPC — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). Only after plan-to-AST and stable relation checks may the executor scan authorized rows; matching opaque handles are staged through its ordinary mutation operation.
- Implementation: `prdb_sdk_optimizer_execute_delete_ast_v1` submits with ABI v5, validates the returned plan, opens the independently resolved full scan, binds the DELETE predicate to executor relation metadata, polls cancellation between bounded pages, collects matching row handles, closes incomplete scans, and stages deletes. It enforces the executor request budget before staging.
- Coverage: the maintained TinyCC fixture drives a parsed parameterized DELETE through operation-selected optimizer submission, relation validation and acquisition, one executor batch, predicate evaluation, and one successful stage-delete acknowledgement.
- Verification: focused optimizer plan fixture and maintained TinyCC SDK suite passed in the pinned Nix shell; ABI generator checks passed.
- Remaining: exercise DELETE through the confined optimizer/executor process acceptance harness and continue UPDATE request/plan support.

## OP-3 dependency: decode bounded UPDATE optimizer requests — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). UPDATE requests identify a bounded assignment set and require a Boolean WHERE tree; names and values remain untrusted until invocation-bound binding.
- Wire format: generated statement tag `OPTIMIZER_UPDATE_STATEMENT=3`; after schema/relation identifiers come 1–64 assignments, each containing a column identifier and either a nonzero parameter index or NULL, followed by a nonempty bounded predicate.
- Implementation: `decode_update_request` validates the request envelope, identifiers, assignment count/kinds, parameter indices, predicate size/tree, and trailing bytes. It does not resolve names, infer assignment types, or grant mutation authority.
- Coverage: Rust tests decode mixed parameter/NULL assignments and reject missing WHERE, invalid assignment kinds, zero parameter indices, and trailing bytes. ABI generation and format checks passed.

## OP-3 dependency: bind bounded UPDATE requests against invocation metadata — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). Binding resolves the target relation and assignment/predicate columns only through authorized planner metadata and produces no executor handle or write grant.
- Implementation: `bind_update_request` and `bind_update_request_via_metadata` bind parameter/NULL assignments to stable column identities, preserve assignment order, reject unknown and duplicate targets, reject NULL for non-nullable columns, and require a Boolean predicate root.
- Parameter values remain untyped in this optimizer request and must be checked against the parsed SQL AST and the storage mutation contract before staging.
- Coverage: tests verify stable identity binding, source-order assignments, predicate binding, duplicate case-insensitive targets, hidden columns, nullability, missing relation, and non-Boolean WHERE.
- Remaining: lower UPDATE values through the SDK and validate their runtime types before staging.

## OP-3 dependency: lower bound UPDATEs to canonical plans — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). UPDATE plans carry stable relation/column identities and the bounded predicate/assignment data but no handles or write authority.
- Wire format: generated kind-3 plan with `Scan -> Filter -> Update` nodes. The scan includes the ordinal-sorted union of assignment targets and predicate references. The generated Update node header carries 1–64 source-order assignments; every assignment contains a full bound column record and a fixed value record for either a nonzero parameter index or NULL with a zero index.
- Implementation: `plan_update_v1`, `encode_update_plan_v1`, `decode_update_plan_v1`, and `validate_update_plan_v1` check relation identity, complete scan shape, Boolean predicate, target uniqueness, scanned target metadata, parameter bounds, NULLability, and trailing bytes.
- Coverage: focused tests round-trip a parameterized plan and reject NULL to a non-nullable target, zero parameter index, duplicate targets, missing scan references, invalid assignment encoding, and trailing bytes. The ABI generator check passed.
- Remaining: independently decode and validate the kind-3 plan in the C SDK.

## OP-3 dependency: compose authorized UPDATE planning service — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). The service composes request decoding, invocation-authorized relation binding, canonical lowering, and plan encoding; it does not create an executor capability.
- Implementation: `optimize_update_v1` uses invocation-bound planner metadata, and `optimize_update_v1_with_relation` composes the authorized description path for a relation resolved over planner IPC.
- Coverage: service tests decode the produced kind-3 plan, verify stable relation identity and scan shape, preserve the parameter assignment, and check non-disclosing missing-relation behavior.
- Remaining: route the request through the isolated optimizer process and independently verify it in C.

## OP-3 dependency: serve UPDATE plans through optimizer process IPC — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). UPDATE uses a distinct optimizer submission operation and the same authorized planner metadata channel as SELECT/DELETE.
- Implementation: generated IPC op `OptimizeUpdate=0x1206`, role allowlist, request name decoding, relation metadata resolution, service dispatch, and response plan validation now include kind-3 UPDATE plans.
- Coverage: IPC code/role tests, optimizer response round-trip test, and a two-stream process protocol test verify relation lookup and validated UPDATE plan output.
- Remaining: submit UPDATE ASTs, verify plan-to-AST values, and execute through authorized scan/stage-update IPC.

## OP-3 dependency: independently validate UPDATE plans in the C SDK — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). The C SDK independently checks optimizer output before trusting target columns or obtaining an executor relation capability.
- Implementation: `prdb_sdk_optimizer_decode_update_plan_v1` validates kind 3 and the `Scan -> Filter -> Update` pipeline, resolves predicate references against scanned IDs, enforces the exact predicate/assignment scan union, and validates unique targets, parameter indices, fixed NULL records, and target nullability. Generic relation validation now accepts UPDATE plans only through this independent decoder.
- Coverage: TinyCC fixture tests a valid parameterized UPDATE plan, malformed/truncated assignment values, target nullability, output/payload overlap, and executor relation re-resolution.
- Verification: maintained TinyCC SDK suite, all eight ABI generator tests, and ABI generated-file check passed in the pinned Nix shell.
- Remaining: encode and submit SQL UPDATE ASTs, match decoded assignments against AST and parameter types, then execute via authorized scan/stage-update IPC.

## OP-3 dependency: encode and submit UPDATE ASTs from the C SDK — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). Serialize a parsed UPDATE with required WHERE, ordered assignments, bounded parameters, schema and relation names; submit only on ABI v5's `OptimizeUpdate` callback.
- Implementation: `prdb_sdk_optimizer_encode_update_v1` checks the UPDATE AST shape and assignment spans, rejects duplicate targets and invalid parameter references/values, then writes the canonical statement-3 request with assignment records and prefix predicate. `prdb_sdk_optimizer_submit_update_v1` uses the operation-selected callback and the shared bounded response decoder.
- Regression: the maintained TinyCC fixture parses a mixed parameter/NULL UPDATE, compares the complete request against hand-checked bytes, verifies undersized-buffer and invalid-parameter behavior, and checks callback operation and successful response decoding.
- Observed failure: the first TinyCC run found the in-progress validation change had accidentally put an UPDATE-only field check in the DELETE encoder and left the old projection shape in UPDATE. The checks were moved to the correct branches; the full maintained SDK suite then passed.
- Verification: pinned Nix shell reported Rust/Cargo/Clippy 1.95.0, rustfmt 1.9.0 and TinyCC 0.9.27-unstable-2025-01-06. `make -f engine/sdk/Makefile test`, locked workspace build, format check, warning-denied Clippy, serial locked workspace tests, and `git diff --check` passed. Rust tests: 138 passed, 4 ignored; the ignored tests include cgroup-dependent acceptance paths.
- Boundary: this is request encoding/submission evidence only; it does not yet validate the returned UPDATE plan against the AST or stage writes.
- Remaining: match decoded assignments against the parsed AST and parameter types, then execute via authorized scan/stage-update IPC and confined-process acceptance.

## OP-3 dependency: validate UPDATE plans against the SQL AST — implemented

- Contract: [optimizer specification](../../specs/planner-compiler.md). Before obtaining an executor capability, independently verify that the optimizer's predicate and source-order assignments match the parsed SQL and its typed parameters, then re-resolve the relation through executor storage.
- Implementation: `prdb_sdk_optimizer_validate_update_plan_with_parameters_v1` checks UPDATE AST shape, plan schema/relation and assignment count, predicate structure/operator/columns/parameter values, assignment names/order/kinds/indices, exact parameter scalar types and nullability. It calls `resolve_plan_relation` and validates stable identities and scanned column definitions before returning success.
- Regression: the maintained TinyCC optimizer-plan fixture accepts a matching plan and rejects assignment-index and assignment-type mismatches; the matching path verifies executor-side relation resolution.
- Verification: targeted optimizer-plan fixture and full maintained TinyCC SDK suite passed in the pinned Nix shell; ABI generator's eight tests and generated-file check passed.
- Limitation: this validates plans but does not scan or stage UPDATE writes. Parameter assignment compatibility currently requires an exact scalar type code; coercions are unspecified and remain unsupported by this path.
- Remaining: execute matched UPDATE plans over an authorized full scan, stage typed patches through executor mutation IPC, and verify the path through confined optimizer/executor processes.

## OP-3 dependency: execute matched UPDATE plans through executor IPC — implemented; confined acceptance passed

- Contract: [optimizer specification](../../specs/planner-compiler.md). Run the SQL predicate only over the independently resolved relation's authorized full-shape batches, collect matching opaque row handles, close the scan, then stage the exact verified patch set through ordinary executor UPDATE IPC.
- Implementation: `prdb_sdk_optimizer_execute_update_ast_v1` submits and validates the plan, acquires a normal relation handle, binds/evaluates the SQL AST predicate over sanitized rows, polls cancellation, bounds pages and target handles, closes the cursor before mutation, and stages each patch through `stage_update`. Any execution/staging failure latches statement failure; the output count is published only after all stages succeed.
- Regression: maintained TinyCC optimizer-plan fixture executes a matching parameterized UPDATE through mocked optimizer/executor callbacks, checks the operation and typed patch bytes, and confirms one row was staged with no failure latch.
- Verification: targeted optimizer-plan fixture and full maintained TinyCC SDK suite passed in the pinned Nix shell; ABI generation check and all eight generator tests passed.
- Observed acceptance correction: the first confined run staged the UPDATE successfully but the fixture expected three rows; an earlier DELETE had removed the original row, leaving the two later inserts. The fixture now expects two updated rows, verifies their value, then confirms optimizer-backed DELETE removes them. The delegated-cgroup acceptance passes.
- Confined acceptance: the maintained `sql_v3_write_read.c` artifact now exports ABI v5, runs `UPDATE records SET id = $2 WHERE id = $1` through optimizer submission, selects both updated rows, then runs optimizer-backed `DELETE FROM records WHERE id = $1` and confirms no rows remain. The ignored `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed through authenticated staging, confined executor and optimizer processes, result relay, and storage commit in a delegated `systemd --user` cgroup.
- Multi-column acceptance: the same confined fixture now updates both `left_key` and `right_key` on `grouped_records`, then runs a second optimizer UPDATE whose predicate matches both assigned values. Two affected rows confirm both assignments became visible through ordinary executor storage and mutation IPC. The acceptance was run in a delegated `systemd --user` cgroup and passed.
- Verification: `nix develop -c make -f engine/sdk/Makefile sandbox-query-fixture` rebuilt the fixture with pinned TinyCC; the confined `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` test passed with the optimizer and executor processes, authenticated staging, durable commit, and result relay. The fixture setup grants only the grouped table/columns needed for this UPDATE and installs UPDATE table/cell policies.
- NULL assignment acceptance: the confined fixture now runs `UPDATE records SET id = NULL WHERE id = $1` through optimizer submission, verifies both rows as SQL NULL through the regular SELECT path, then restores their values through the authorized SQL UPDATE path so optimizer DELETE acceptance still runs.
- Verification: the rebuilt TinyCC sandbox fixture and delegated-cgroup `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` test passed, including the NULL assignment, its readback, optimizer-backed DELETE, and durable commit.
- Remaining: cover cancellation and stage-update failure in the confined process harness, target exhaustion there, and transaction conflict/policy behavior.

## OP-3 dependency: cancel an in-progress optimizer UPDATE — verified

- Behavior: if executor cancellation arrives between opening an authorized scan and reading its next page, the SDK closes the cursor, stages no UPDATE, latches statement failure, and leaves the result count at its failure value.
- Regression: the maintained `sdk_optimizer_plan_v1.c` callback fixture now returns cancellation at that checkpoint and checks the cancellation status, cursor-close operation, zero stage-update calls, one failure latch, and zero published updated rows. The successful mock UPDATE remains covered.
- Verification: `nix develop -c make -f engine/sdk/Makefile test` passed, including all eight ABI generator tests and the full maintained TinyCC SDK suite.
- Remaining: cover cancellation in the confined process harness, optimizer response loss, target exhaustion, stage-update failure, and transaction conflict/policy behavior.

## OP-3 dependency: fail UPDATE when executor staging fails — verified

- Behavior: if the ordinary executor rejects StageUpdate after a completed scan, the SDK returns callback failure, latches statement failure, and leaves the updated-row count at zero.
- Regression: the maintained `sdk_optimizer_plan_v1.c` mock injects an executor callback error for StageUpdate. It verifies the row batch completed before staging, no success count is published, and exactly one failure is latched. The neighboring cancellation regression still verifies cursor closure before staging begins.
- Verification: `nix develop -c make -f engine/sdk/Makefile test` passed, including the full TinyCC SDK suite and all eight ABI generator tests.
- Remaining: exercise stage failure and cancellation through confined processes, test target exhaustion, and expand transaction conflict/policy cases.

## OP-3 dependency: optimizer submission callback failure aborts UPDATE — verified

- Behavior: an optimizer callback error before a plan response stops execution before executor relation resolution or mutation and latches statement failure.
- Regression: the TinyCC `sdk_optimizer_plan_v1.c` mock injects an optimizer callback error and checks the returned callback status, one failure latch, zero executor requests, zero staged updates, and zero published row count.
- Verification: `nix develop -c make -f engine/sdk/Makefile test` passed, including all ABI generator tests and maintained TinyCC SDK executables.
- Remaining: exercise target exhaustion and mutation cancellation/staging failures through confined processes, then expand transaction conflict/policy cases.

## OP-3 dependency: stop UPDATE when target capacity is exhausted — verified

- Behavior: when a bounded scan finds more matching rows than the caller's handle capacity, execution fails before staging any mutation and publishes no updated-row count.
- Regression: the TinyCC optimizer-plan fixture supplies a two-row matching batch with capacity for one target. It checks `REQUEST_LIMIT`, one sticky failure, zero StageUpdate calls, and zero updated count.
- Verification: `nix develop -c make -f engine/sdk/Makefile test` passed, including all ABI generator tests and maintained TinyCC SDK executables.
- Remaining: exercise cancellation, target exhaustion, and staging failure through confined processes, then expand transaction conflict/policy cases.

## OP-3 follow-up: UPDATE target exhaustion through confined processes — verified

- Contract: when a bounded optimizer UPDATE finds more matching rows than the caller's target-handle capacity, it must stop before staging any mutation, publish no updated-row count, latch statement failure, and abort the invocation.
- Regression: the maintained ABI v5 artifact runs `UPDATE grouped_records SET left_key = $2 WHERE left_key = $1` with two matching rows and target capacity one. The SDK returns `REQUEST_LIMIT` with zero updated rows; the C entry deliberately returns success afterward so the sticky statement-failure latch itself must force worker failure. The delegated acceptance checks durable ABORTED and byte-identical committed rows.
- Verification: forced TinyCC `sandbox-query-fixture` rebuild and the focused `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` test passed through the delegated service (`1 passed, 0 ignored`). The complete maintained SDK suite and ABI generator's eight tests passed. Format, warning-denied Clippy, and `git diff --check` passed. The final full serial delegated workspace suite passed with 180 library tests, 142 commit tests, and 74 transaction tests; every test target reported zero ignored.
- Remaining: exercise UPDATE cancellation and explicit StageUpdate callback failure in the confined process harness; broaden transaction conflict/policy acceptance.

## OP-3 dependency: reject an optimizer UPDATE without storage write authority — verified

- Behavior: a readable relation remains available to optimizer planning, but its absent UPDATE grant must reject staging and abort the invocation without committing.
- Regression: the confined SQL artifact submits a parameterized UPDATE against `linked_records`, for which the admitted user has SELECT access but no UPDATE grant. It emits a provisional ready event, then the executor/storage path rejects the mutation; the gateway reports `QueryError` and storage records an `Aborted` outcome.
- Verification: the fixture compiled with pinned TinyCC using `make -f engine/sdk/Makefile sandbox-query-fixture`; `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` passed inside a delegated `systemd --user` cgroup with real optimizer and executor processes.
- Remaining: confined cancellation and explicit staging-failure injection, plus broader transaction conflict/policy acceptance.

## OP-3 follow-up: UPDATE cancellation through confined processes — verified

- Behavior: cancellation delivered at the SDK's first poll after an authorized UPDATE scan opens closes the cursor, stages no rows, latches statement failure, and publishes no updated-row count.
- Confined fixture: the existing `'C'` invocation now runs `UPDATE grouped_records SET left_key = $2 WHERE left_key = $1`. Its executor callback announces readiness at that first cancellation check, waits for the storage cancellation response, and returns it to the SDK. The artifact confirms `CANCELLED` and zero updated rows, then emits a confirmation event; a successful artifact return after the sticky failure still leads to worker failure.
- Acceptance: the normal `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` test requests cancellation only after confirming overlap with live analytics and an operational transaction. It requires the readiness, observed, and UPDATE-confirmation frames, and verifies durable ABORTED.
- Verification: forced TinyCC sandbox fixture rebuild passed. The focused acceptance passed inside the delegated `systemd --user` service (`1 passed, 0 ignored`). `make -f engine/sdk/Makefile test`, ABI generation checks (8 passed), workspace build, format check, warning-denied Clippy, and the full serial delegated-cgroup workspace suite passed. The workspace suite reported 180 library tests, 142 commit tests, 74 transaction tests, and zero ignored tests in every target.
- Remaining: injected StageUpdate callback failure through confined processes, then broaden transaction conflict/policy acceptance.

## OP-3 follow-up: UPDATE StageUpdate callback failure through confined processes — verified

- Behavior: an executor callback error on the first StageUpdate after a successful scan returns SDK callback failure, latches statement failure, and leaves the updated-row count at zero.
- Confined fixture: invocation parameter `'G'` submits an optimizer UPDATE for the two grouped rows. The fixture callback intercepts the first StageUpdate, emits an injection marker, and returns an I/O callback error. The C entry verifies the status and zero count, then returns success so the sticky failure latch must still fail the worker.
- Acceptance: the enabled DML sequence acceptance requires the readiness and injection result frames, confirms worker failure and durable ABORTED, and compares both committed row payloads byte-for-byte.
- Verification: forced TinyCC fixture rebuild and the focused delegated-cgroup acceptance passed (`1 passed, 0 ignored`).
- Remaining: broader transaction conflict/policy acceptance.

## OP-3 follow-up: default-deny UPDATE policy with explicit grants — verified

- Behavior: an UPDATE with table and target-column privileges still fails closed when no applicable UPDATE row policy allows it. The test targets `match_key`, which is not used by the table's SELECT policy predicates, so the failure occurs at row-policy authorization rather than the immutable policy-driving-column guard.
- Regression: the confined SQL fixture reaches the real StageUpdate callback, forwards the request to storage, and confirms that callback returns an error. The gateway reports `QueryError`; storage records durable `Aborted`; the committed row payload remains byte-identical.
- Supervisor fix: worker-channel deadline polling now treats an already-aborted transaction as a worker error path and waits for the handler result. The monotonic invocation deadline still bounds a worker that does not exit after aborting.
- Verification: TinyCC sandbox fixture rebuild and complete maintained SDK suite passed. The focused confined DML acceptance passed; locked workspace build, format check, warning-denied Clippy, and the full serial workspace suite inside the delegated `systemd --user` cgroup passed.
- Remaining: broader transaction conflict and policy acceptance.

## OP-3 follow-up: concurrent authorized UPDATE conflict — verified

- Behavior: two transactions can stage policy-authorized patches from the same row snapshot, but commit validation publishes only one version. The losing transaction records a durable ABORTED outcome and cannot overwrite the winner.
- Regression: the real-redb transaction test gives both invocations table/column grants and applicable SELECT/UPDATE policies, stages different values through `stage_patch_authorized`, synchronizes before commit, and checks one COMMITTED outcome, one ABORTED conflict, the winning row version, and the final typed value.
- Verification: the focused `concurrent_authorized_updates_have_one_commit_and_one_durable_conflict` test passed.
- Remaining: broader transaction conflict and policy acceptance.

## OP-4 dependency: unknown estimate falls back to a bounded scan plan — verified

- Behavior: exhausting the estimate allowance returns `Unknown`, omits the optional empty-view proof, and keeps the planner stream usable. The optimizer then emits its bounded full-scan plan when storage provides no empty-population proof. Cancellation and invocation expiry during an estimate terminate planning and durably abort rather than becoming `Unknown`.
- Regression: the real-redb planner-stream test consumes the estimate allowance, observes `Unknown`, and asks for the opt-in empty-view proof again; storage returns the authorized relation without a proof. The optimizer-process test verifies a relation response without a proof yields an ordinary `Scan` plan with no empty-view hint. Separate admitted invocations are cancelled or allowed to expire after relation-handle creation and before `EstimateScan`; each gets no fallback response and ends durably ABORTED.
- Verification: focused `planner_stream_estimate_allowance_falls_back_without_aborting_invocation` and `optimizer_process::tests::process_protocol_uses_planner_metadata_and_returns_a_validated_plan` tests passed.
- Remaining: general cardinality estimates, candidate comparison, and broader estimate-driven choices.

## OP-4 dependency: exact visible cardinality informs a bounded access choice — implemented

- Behavior: ordinary single-table SELECT can request a bounded authorized-view row-count probe through the planner relation response. Exact zero retains the empty-view hint; exact populations of one through four suppress point or range hints in favor of a sequential scan. Larger counts and unknown estimates keep their prior choices.
- Contract: the count is advisory, consumes the existing 16-request allowance, reads at most five row keys plus at most four staged overlay entries, and never decodes row values. Populations above four, value-dependent policies, oversized overlays, and unsupported cases return `Unknown`. The executor continues to independently validate relation identity and access-path authorization.
- Regression: the real-redb planner-stream test checks exact zero and exhausted `Unknown`; transaction tests check staged inserts, replacements, and deletes, the four-row boundary, dynamic-policy fallback, a 4,097-row fallback, and identical counts when only redacted cell contents change. Point/range plan regressions check that four rows suppress either access hint while unknown and five rows preserve them. An optimizer-process IPC test sends an exact four-row estimate and approved range path, then confirms the returned filtered plan keeps its predicate and drops the range hint.
- Limitation: the 1–4 cutoff is a local initial heuristic informed by repeated API timings, not an end-to-end SQL break-even point. Predicate selectivity, broader candidate comparison, and repeat/target-host profiles remain open.
- Verification: full delegated workspace tests, workspace build, formatting, Clippy, TinyCC SDK tests, and the sandbox fixture build passed. The cgroup probe reported writable `cpu memory pids` delegation.

## OP-4 follow-up: measure small-population scan choice — measured

- Harness: `PRDB_BENCH_INDEX_ACCESS=1 PRDB_BENCH_INDEX_ROWS=<n>` runs 11 timed small-population probes, point probes, exact-key range scans, and authorized scan-plus-filter calls after seeding a fresh redb fixture with `n + 1` total rows. Every result is checked against the target row and the probe returns `Unknown` above four.
- Five fresh-database runs on Linux x86_64 with Rust 1.95.0 and redb 4.4.0 produced median-of-run p50 point/range/scan latency (ms) of 0.085/0.092/0.020 at 1 row, 0.085/0.093/0.049 at 4, 0.087/0.093/0.089 at 8, and 0.088/0.096/0.168 at 16. A one-run sweep at 2, 32, and 4,097 rows also showed scan/range p50 of 0.030/0.093, 0.346/0.097, and 56.514/0.124 ms respectively.
- A separate run measured the row-count probe itself at 0.012–0.026 ms p50 for populations 1, 4, 8, and 4,097. It excludes planner relation resolution and projection loading.
- Decision: retain a conservative cutoff of four rows. This measures direct storage API result latency, not SQL setup, optimizer estimation, confined executor startup, result relay, or commit; cache state and host load were uncontrolled. Target-host profiles remain open.

## OP-4 follow-up: sample the scan/index crossover above the cutoff — measured

- Harness: repeated `PRDB_BENCH_INDEX_ACCESS=1 PRDB_BENCH_INDEX_ROWS=<n>` on five fresh redb databases at each point. Each run took 11 samples of the bounded population estimate, unique point probe, exact-key range, and full authorized scan plus filter; every operation checked the same exact target row.
- Results: the median of the five run-level p50/p95 values (milliseconds) for total visible rows was:

  | Rows | Estimate | Point | Range | Scan + filter |
  | ---: | ---: | ---: | ---: | ---: |
  | 6 | 0.015 / 0.022 | 0.086 / 0.111 | 0.094 / 0.111 | 0.070 / 0.087 |
  | 7 | 0.015 / 0.017 | 0.085 / 0.097 | 0.094 / 0.107 | 0.080 / 0.093 |
  | 9 | 0.015 / 0.017 | 0.086 / 0.100 | 0.095 / 0.126 | 0.100 / 0.113 |
  | 11 | 0.015 / 0.023 | 0.086 / 0.101 | 0.095 / 0.101 | 0.120 / 0.130 |
  | 13 | 0.016 / 0.033 | 0.090 / 0.106 | 0.101 / 0.116 | 0.142 / 0.164 |

- Decision: keep the existing four-row sequential-scan cutoff. The direct API crossover is between seven and nine rows for this one-match workload; at seven rows the scan advantage over a point probe is only about five microseconds, and the estimate's roughly 15-microsecond median plus planner/setup work is not included. This supports avoiding a broader cutoff based on a fragile local margin.
- Scope: one Linux x86_64 host, uncontrolled cache/load, one exact matching row, and storage API calls only. The harness excludes optimizer process startup, SQL setup, gateway relay, and durable commit; no target-hardware or service-latency claim follows.
- Verification: all 25 fresh-database benchmark invocations passed and confirmed the target row; each used 11 timed samples.

## OP-4 follow-up: apply exact predicate population to scan choice — implemented

- Behavior: filtered ordinary unordered single-table SELECT now uses the exact visible population from its bounded predicate estimate with the existing 1–4 row scan heuristic. An exact nonzero small population drops a point/range hint while retaining the Filter node, so the sequential scan still evaluates the predicate. Exact zero continues to use the validated empty-view hint; unknown estimates preserve the original access path.
- Regression: optimizer-process protocol cases verify exact `(visible=4, matching=2)` selects a filtered sequential scan, `Unknown` retains the range plan and filter, and exact zero still selects the no-scan hint.
- Verification: all three focused optimizer-process tests passed; workspace build, fmt check, warning-denied Clippy, TinyCC SDK tests, and sandbox fixture build passed. The delegated cgroup probe reported writable `cpu memory pids`; the full serial delegated suite passed (223 engine unit, 151 commit integration, and all other workspace targets, with zero failures or ignored tests).

## OP-4 follow-up: cancelled predicate estimates do not produce fallback plans — verified

- Contract: `Unknown` is a valid estimate result and may retain the ordinary access path. Cancellation, expiry, or loss of the planner stream terminates planning; it must not be converted to `Unknown` or return a fallback plan.
- Regression: `process_protocol_terminates_when_predicate_estimate_stream_closes` lets the optimizer resolve an authorized relation, observes its `EstimateScan` request, closes the planner stream at that boundary, then verifies that no submission response or plan is emitted and the optimizer reports `PlannerClosed`. The real-redb planner-stream regression above separately verifies that a cancelled estimate closes the worker stream and durably records ABORTED.
- Verification: the focused optimizer-process regression passed. Rust 1.95.0, Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06 matched the pinned shell. Workspace build, fmt check, warning-denied Clippy, maintained TinyCC tests and sandbox fixture, release build/profile validation (10 engine binaries and 1 query fixture), delegated cgroup probe, and the full serial delegated-cgroup workspace suite passed. The suite reported 224 library tests and 151 commit integration tests passed, with zero ignored tests; all other targets passed. `git diff --check` passed.
- Remaining: the storage worker-channel slice below still does not route the interleaving through the external TCP gateway and full executor lifecycle; estimate-driven candidate comparison and target-host cost profiles remain open.

## OP-4 follow-up: terminate a production planner request at the estimate boundary — verified slice

- Contract: a planner estimate that has been requested but has not returned is still in-flight planning work. Owner cancellation and deadline expiry must stop the request before it can produce a successful estimate or fallback plan, and the invocation must durably abort.
- Regression: `cancellation_during_optimizer_predicate_estimate_kills_process_tree_without_fallback` and `deadline_during_optimizer_predicate_estimate_kills_process_tree_without_fallback` launch the production `prdb-optimizer` binary as a child in a real delegated cgroup and serve its planner and executor storage channels through `SupervisorInvocation::serve_worker_channels`. The optimizer resolves `information_schema.tables` through the real planner handler. A request observer pauses at the incoming `EstimateScan` boundary; the cases then accept owner cancellation or wait for the invocation deadline. The initial deadline-path regression returned a successful optimizer plan, exposing a race while a planner handler was busy. The storage channel now checks cancellation/deadline after the observer and before request dispatch. Both cases now confirm a non-success response or EOF, durable ABORTED, optimizer exit, and cgroup cleanup.
- Verification: the initial service-level deadline regression failed because the optimizer returned Success. After the request-boundary check, both focused delegated-cgroup regressions passed. Locked workspace build, format check, warning-denied Clippy, maintained TinyCC SDK tests and sandbox fixture, release-profile validation (10 engine binaries and one query fixture), and the full serial delegated-cgroup suite passed. The suite reported 224 library tests and 154 commit integration tests passed, with zero ignored; all other workspace targets passed. `git diff --check` passed.
- Scope: this exercises the production optimizer and storage worker-channel multiplexer but still does not route the blocked-estimate schedule through the external TCP gateway and full executor service lifecycle. End-to-end gateway/executor interleaving, broader candidate comparison, and target-host profiles remain open.

## OP-4 follow-up: observe predicate estimates through the confined executor lifecycle — verified slice

- Behavior: `RunningExecutor` now has a doc-hidden observation variant that passes optimizer-planner request headers through the same storage worker-channel service used by normal execution. Existing callers retain the unchanged `observe_exit_with_results` API and receive a no-op observer.
- Regression: `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` uses the optimizer-backed filtered SELECT in the TinyCC fixture and asserts that `EstimateScan` reaches the observer while `RunningExecutor` serves the real worker processes. The same run relays provisional output and confirms the established durable terminal outcome. This verifies production executor-channel and gateway-result-relay interleaving; it does not inject cancellation while that estimate is pending.
- Verification: locked workspace check and the focused acceptance passed. The acceptance ran inside the delegated `systemd --user` service with the maintained TinyCC artifact and real cgroup/Landlock executor launcher.
- Scope at this slice: the blocked-estimate cancellation/deadline schedule remained proven at the storage worker-channel boundary; the next follow-up closes the TCP START and full executor lifecycle gap. Broader candidate comparison and target-host profiles remain open.

## OP-4 follow-up: cancel a pending estimate through external TCP execution — verified slice

- Implementation: the maintained restricted-metadata fixture now exposes ABI v4 and submits a filtered `information_schema.schemata` SELECT through the SDK optimizer bridge. The observer exposes the real executor storage-dispatch boundary so the test can coordinate an authenticated control-mode CANCEL while `EstimateScan` is pending.
- Regression: `sandboxed_optimizer_metadata_submission_cancels_over_tcp` uploads and starts the ABI v4 query through the external TCP gateway. The first invocation commits `empty_roundtrip`; the second pauses at `EstimateScan` until an authenticated administrator `AdminClient` sends control-mode CANCEL through a separate external TCP gateway connection. The client receives an accepted acknowledgement, the submission receives an Aborted terminal, and storage records durable ABORTED status.
- Validation: the focused test and full serial delegated-cgroup workspace suite passed. The suite reported 224 unit tests and 154 commit integration tests, with all other targets passing and zero ignored tests. The optimizer estimate and cancel acknowledgement are synchronized at the actual request boundary; the accepted control cancellation is followed by the full executor process/cgroup cleanup and terminal relay. Workspace build, fmt check, warning-denied Clippy, TinyCC SDK tests and fixture, release-profile validation, and `git diff --check` passed.
- Scope: broader estimate-driven candidate comparison and target-host cost profiles remain open.

## OP-5 follow-up: hidden range keys do not reveal match state — verified

- Contract: executor storage rejects an index range before key lookup when any indexed key column is not unconditionally readable for the invocation. Explicit requests therefore return `Unavailable` for both matching and absent hidden keys; authorized callers receive only sanitized bounded rows.
- Regression: the token-authenticated executor test submits exact inclusive ranges through the real executor IPC endpoint. The authorized principal gets the expected sanitized matching row and an empty result for an absent value. A second principal whose row is visible but whose indexed cell is REDACTED receives the same `Unavailable` response for both bound values. The planner metadata response for that principal continues to omit both access paths.
- Verification: focused `token_authenticated_catalog_scans_redact_cells_per_principal` passed. Workspace build, fmt check, warning-denied Clippy, maintained TinyCC tests, sandbox fixture, and the full serial delegated-cgroup workspace suite passed. The cgroup probe reported writable `cpu memory pids`; test targets included 223 engine unit tests, 151 commit integration tests, and zero failures or ignored tests. `git diff --check` passed.

## OP-5 follow-up: confined SQL acceptance exercises the index range path — verified

- Regression: the five-row `grouped_records` fixture stays above the optimizer's four-row sequential-scan cutoff. The TinyCC fixture wraps the executor callback for its optimizer-backed range query, asserts exactly one `ScanIndexRange` request, and compares both result rows with the authorized reference SQL scan. Group, HAVING, and ordered-query expectations were updated for the enlarged fixture.
- Verification: the focused acceptance passed inside the delegated `systemd --user` cgroup. The full serial delegated-cgroup workspace suite passed (223 engine unit tests and 151 commit integration tests, with all other workspace targets passing and zero ignored tests); workspace build, format check, warning-denied Clippy, maintained TinyCC tests, sandbox fixture build, and `git diff --check` passed.
- Timing is recorded separately below; this path-proof increment itself makes no latency claim.

## OP-5 follow-up: confined SQL range setup and result latency — measured

- Harness: the same enabled SQL acceptance records monotonic elapsed time inside the confined TinyCC process, from before optimizer-query parsing through both returned rows, and compares it with the reference SQL scan from entry through its result callbacks. A private test telemetry payload is intercepted by the parent and omitted from gateway results. Five sequential fresh-database runs used the delegated `systemd --user` cgroup; each also ran the acceptance's concurrent workload.
- Result: optimizer-path latency was 130.9–167.0 ms (median 152.9 ms); reference SQL scan latency was 39.3–54.1 ms (median 48.3 ms). In this five-row fixture, the optimizer setup and IPC path took about 3.2 times as long as the direct reference scan.
- Verification: all five focused delegated-cgroup runs passed. The full serial delegated-cgroup workspace suite passed, including 223 engine unit tests and 151 commit integration tests, with zero failures or ignored tests. Workspace build, format check, warning-denied Clippy, TinyCC SDK tests, and the sandbox fixture build passed.
- Scope: these are local timings for one small population on one Linux x86_64 host. They include parse/setup, planner/executor IPC, and result callbacks within each SDK call; they exclude sandbox startup and gateway relay. They are comparative evidence, not targets or a reason to select the optimizer for a five-row table. Larger populations, gateway relay, and target-host profiles remain open.

## OP-4 follow-up: confined predicate estimate selects a sequential scan — verified slice

- Red evidence: the first confined acceptance attempt failed at the new small-estimate scan-choice assertion. Its five-row range table made the exact predicate estimate unavailable, and the optimizer compared `visible_rows` instead of the specified `matching_rows` when applying the filtered-query cutoff.
- Implementation: the predicate heuristic now uses `matching_rows`; the optimizer-process regression has eight visible rows and two matches to distinguish those fields. The TinyCC acceptance uses a one-row indexed `records` table, asserts one `OPEN_SCAN` and no `SCAN_INDEX_RANGE`, and compares the optimizer result with direct authorized SQL. The existing five-row indexed range case remains and still compares its duplicate-preserving result with the reference scan.
- Contract: exact predicate estimates are released only when the entire visible population is within the four-row bound, so the sequential-scan and range-path cases intentionally use separate relations/populations.
- Verification: the focused optimizer-process regression and confined SQL acceptance passed under the delegated `systemd --user` cgroup. Workspace build, formatting, warning-denied Clippy, maintained TinyCC SDK tests, release-profile validation (10 engine binaries and one query fixture), and the full serial delegated-cgroup workspace suite passed. The suite reported 224 engine unit tests and 154 commit integration tests passed, with all other targets passing and zero ignored tests. The cgroup probe reported writable `cpu memory pids` delegation.
## OP-4 follow-up: choose scans for broad range matches — implementation

- Contract: when a filtered ordinary unordered single-table SELECT has an exact authorized predicate estimate, keep the existing 1–4 visible-row scan rule. For larger populations, remove only a range access hint when at least half the visible rows match; retain the Filter node. Unique point probes are unaffected. Unknown estimates retain the prior access choice.
- Evidence: five fresh-database local TEXT profiles at 130 visible rows compared exact predicate-estimate, range lookup, and authorized scan-plus-filter timings with eleven samples per run. At 56 matches (43%), range lookup remained faster; at 64 (49%), paths were near crossover; at 80 (62%), scan-plus-filter was faster. The one-half cutoff is a conservative rounded heuristic, not a cost model. The profile measures direct transaction APIs on one Linux x86_64 host and excludes planner IPC, mixed operational load, and target hardware.
- Regression: optimizer process tests verify 80/130 selects scan and 64/130 retains range; plan tests cover threshold rounding, malformed counts, and unique-point preservation. The transaction benchmark verifies exact counts and equivalent row IDs for range and scan paths.
- Verification: focused plan and optimizer-process regressions passed; workspace build, formatting, warning-denied Clippy, maintained TinyCC tests, and sandbox fixture passed. The delegated cgroup probe confirmed a writable cgroup v2 parent with `cpu memory pids`; the complete serial delegated-cgroup workspace suite passed, including all engine, process, and confined SQL targets. `git diff --check` passed.
- Remaining: broader cost comparison, mixed-load and target-host profiling, and planner maintenance/task work remain open.
## OP-6 follow-up: invalidate session estimate reuse after private writes — verified slice

- Exact predicate estimates can be reused only inside the same planner session, for identical relation/schema identity, qualified names, predicate bytes, and candidate bound. The invocation snapshot and authorization context remain session-scoped; the cumulative planner estimate allowance applies to cache hits, Unknown is not cached, and results never cross sessions.
- A checked overlay write generation changes on successful mutations and invalidates prior entries. The shared-channel regression observes an identical estimate before and after an invocation-private insert; its initial stale-cache failure returned `(5,1)` instead of `(6,2)`, and now passes with the updated count.
- Verification: workspace build, format check, warning-denied Clippy, maintained TinyCC tests and sandbox fixture passed. The delegated cgroup probe found writable `cpu memory pids`; the complete serial delegated-cgroup workspace suite passed, including the shared planner-session invalidation regression.

## OP-6 follow-up: cache bounded population probes — implementation

- `PlannerMetadataSessionState` now preserves exact population-probe cache entries across request detach/reattach, keyed by relation ID, schema ID/version, and row bound. Cache hits still consume one planner estimate allowance; changed overlay generations invalidate population and predicate entries. Unknown probes are not cached.
- The real planner-stream allowance regression now checks repeated population estimates, a changed result after a staged private row, and allowance exhaustion after cache hits.
- Verification: the focused planner-stream integration regression and two exact/unknown cache unit tests passed. Format check, locked workspace build, warning-denied Clippy, and full serial delegated-cgroup workspace suite passed, including 379 library tests, 205 commit integration tests, 3 metadata-process tests, 79 transaction tests, and all remaining workspace targets.

## OP-6 follow-up: cache exact column summaries — verified slice

- The planner session now reuses exact scan summaries by relation/schema identity and version plus ordered requested column IDs. Its pinned authorization and snapshot context scopes reuse to one invocation; overlay generations invalidate summary, predicate, and population cache entries after private writes. Cache hits still consume the 16-estimate allowance; `Unknown` and cross-session results are not cached.
- The shared planner-channel regression repeats an exact summary and then stages a private row before the next request, proving the result updates from six visible rows to seven. Unit coverage checks that requested column order is part of the key and that a new generation recomputes the summary.
- Verification: focused cache and planner-stream regressions, workspace build, format check, warning-denied Clippy, TinyCC SDK tests, sandbox fixture, writable delegated-cgroup preflight, and full serial delegated-cgroup workspace suite passed (380 engine library tests, 205 commit integration tests, 3 metadata-process tests, 79 transaction tests, and all remaining targets).
