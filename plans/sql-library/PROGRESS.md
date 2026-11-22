# SQL Library Progress

## SQL-7 follow-up: compose WHERE with compound HAVING — verified

- Contract: row filtering runs before grouped aggregation, and the resulting groups then pass through compound HAVING before callbacks.
- Regression: the v22 TinyCC execution fixture combines `WHERE id >= $1` with `HAVING COUNT(*) >= 1 AND SUM(score) >= 5.00`. An unfiltered `(1, 3.00)` group would qualify but must be absent; only filtered `(2, 5.00)` is delivered with its exact SUM and row count.
- Verification: focused `sdk-sql-execute-v10` fixture and full maintained TinyCC SDK suite passed with the pinned toolchain; `git diff --check` passed.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-8 follow-up: redacted NULL filter aborts confined SQL — verified

- Contract: optimizer-backed filtered aggregation must fail when `IS NULL` evaluates a REDACTED cell, emit no partial SQLR output, and durably abort the invocation.
- Regression: the confined SQL acceptance installs a restrictive cell policy on the Decimal aggregate column, then submits a separate filtered composite aggregate using `decimal_value IS NULL`. The executor must fail before any result frame; the gateway confirms ABORTED. The existing redacted aggregate-input case remains separate.
- Verification: the focused delegated-cgroup acceptance passed, followed by the full serial delegated-cgroup workspace suite with zero ignored tests. The sandbox query artifact build, locked workspace build, format check, warning-denied all-target Clippy, full maintained TinyCC SDK suite, and `git diff --check` passed.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-8 follow-up: NULL predicates reject redacted cells — verified

- Contract: optimizer-backed `IS NULL` and `IS NOT NULL` evaluate VALUE and NULL cells according to SQL semantics and fail the query on REDACTED operands.
- Regression: the maintained TinyCC plan-consumer fixture exercises both predicate tags against all three cell states. It requires the expected row visibility for VALUE/NULL and `FILTER_ERROR` with no callback for REDACTED.
- Verification: focused optimizer plan fixture and full `make -f engine/sdk/Makefile test` suite passed under the pinned TinyCC toolchain.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-7 follow-up: filtered composite aggregate supports NULL predicates — verified

- Contract: statement 15 accepts bounded `IS NULL` and `IS NOT NULL` predicates over direct columns, while preserving redaction as an evaluation error.
- Implementation: request and plan codecs, invocation binding, SQL encoding, AST-to-plan matching, and predicate evaluation carry the two null-test nodes. The confined Decimal SUM query combines `decimal_value IS NOT NULL` with its parameterized group filter.
- Regression: Rust request/binder and plan round-trip coverage include null tests; the focused confined SQL acceptance combines `left_key = $1 AND (IS NULL OR IS NOT NULL)` for `(1,7)`, requires both rows to survive, and checks the sum `1.00`.
- Verification: focused statement-15 acceptance and full serial delegated-cgroup workspace suite passed; the TinyCC SDK suite, locked workspace build, format check, warning-denied all-target Clippy, release-profile validation, and `git diff --check` passed.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-7 follow-up: confined filtered composite aggregate acceptance — verified

- Contract: statement 15 filters rows before composite aggregation, counts the matching rows after filtering, and returns only groups satisfying a parameterized predicate over the authorized view.
- Regression: the confined SQL artifact runs `SUM(decimal_value)` grouped by `(left_key, right_key)` with `WHERE left_key = $1` through optimizer planning, plan-to-AST validation, executor relation re-resolution, and the bounded v13 runner. The focused `Q` invocation filters to `(2,9)`, returns its exact DECIMAL(38,2) sum `4.50`, and commits after the gateway verifies the SQLR record byte-for-byte.
- Verification: the focused delegated-cgroup acceptance and full serial delegated-cgroup workspace suite passed, including the confined SQL acceptance. The maintained TinyCC SDK suite, format check, locked workspace build, warning-denied all-target Clippy, ABI check, and `git diff --check` passed. The maintained optimizer fixture is 152,184 bytes under its adjusted 153,000-byte regression bound.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-7 follow-up: filtered composite aggregate optimizer path — implemented

- Contract: composite `COUNT(*)`, `MIN`, `MAX`, `SUM`, or `AVG` supports a bounded WHERE tree with two to eight grouping keys and optional LIMIT. Statement 15 leaves the existing statement 13 request format unchanged and scans only the sorted union of group keys, aggregate input, and filter columns.
- Implementation: the SQL SDK encodes parameterized predicates, validates the optimizer filter against the parsed/bound AST, resolves the executor relation again, and passes parameters to the v13 runner so filtering occurs before aggregation and LIMIT.
- Regression: maintained TinyCC tests cover statement bytes, decoder validation and predicate tampering, plus filtered SUM on a two-key group query; Rust covers request binding, plan validation, and process dispatch.
- Verification: the TinyCC suite and sandbox fixture build, ABI check, Rust format/build/Clippy checks, and full serial delegated-cgroup workspace suite passed.
- Remaining: broader analytic workload, secure spill, target-hardware, and mixed-workload evidence remain open.

## SQL-7 follow-up: composite runner groups NULL key tuples — verified

- Contract: the v13 composite grouping runner treats NULL components as equal, separates NULL from VALUE, and emits groups in first-seen order.
- Regression: the maintained C runner fixture executes composite `COUNT(*)` over `(1,NULL)`, `(1,9)`, `(NULL,9)`, and repeated `(NULL,NULL)` tuples. It checks the exact four-group order, NULL states, and row counts `2,1,1,2`.
- Verification: the focused `sdk-sql-execute-v10` fixture and full `make -f engine/sdk/Makefile test` suite passed under the pinned TinyCC toolchain; `git diff --check` passed.
- Remaining: broader analytical workload, secure spill, target-hardware, and mixed-workload evidence remains open.

## SQL-7 follow-up: all aggregate families at eight grouping keys — verified

- Contract: optimizer statement 13 supports eight composite grouping keys for `COUNT(*)`, `MIN`, `MAX`, `SUM`, and `AVG`; each plan scans exactly the grouping keys plus the selected aggregate input when one exists.
- Regression: the optimizer-process test now exercises all five aggregate kinds against a nine-column authorized relation at the eight-key boundary, checks each ordered key set, scan-column union, aggregate argument ordinal, and LIMIT, and retains the maximum-shape ordered SUM plan check.
- Verification: the focused locked optimizer-process test, formatting, locked workspace build, warning-denied all-target Clippy, and full serial delegated-cgroup workspace suite passed. All sandbox/process targets ran with zero ignored tests.
- Remaining: broader analytical workload, secure spill, target-hardware, and mixed-workload evidence remains open.

## SQL-7 / SQL-8 follow-up: C optimizer wrapper at eight keys and nine sort items — verified

- Contract: the maintained C SDK optimizer wrapper accepts the maximum composite key and ORDER BY shape, resolves the optimizer's nine-column relation, and executes the matched ordered plan.
- Regression: the TinyCC fixture submits eight grouping keys and nine descending sort items through statement kind 14. It validates request encoding, plan matching, executor relation re-resolution, and exact sorted SUM results for both groups.
- Verification: the focused `sdk-sql-execute-v10` fixture and full `make -f engine/sdk/Makefile test` suite passed under the pinned TinyCC toolchain; `git diff --check` passed.
- Remaining: broader analytical workload, secure spill, target-hardware, and mixed-workload evidence remains open.

## SQL-7 / SQL-8 follow-up: optimizer plan at eight keys and nine sort items — verified

- Contract: optimizer-backed ordered composite aggregation accepts eight grouping keys and up to nine selected sort outputs, with scan columns equal to the key/aggregate union.
- Regression: the optimizer process test now submits `SUM(measure)` over eight keys ordered by all eight keys and the aggregate. It verifies the ordered plan contains eight key ordinals, the measure column, all nine direction/NULL-order items, and the requested LIMIT.
- Verification: the focused locked Rust optimizer process test, formatting, locked workspace build, warning-denied all-target Clippy, and full serial delegated-cgroup workspace suite passed. The suite ran the sandbox and process acceptance targets with zero ignored tests.
- Remaining: broader analytical workload, secure spill, target-hardware, and mixed-workload evidence remains open.

## SQL-7 / SQL-8 follow-up: order aggregate groups at the nine-item limit — verified

- Contract: v15 supports up to nine distinct selected ORDER BY outputs: the eight composite group keys plus the selected aggregate, with independent direction and NULL ordering.
- Regression: the maintained TinyCC runner fixture executes eight-key `SUM(measure)` with all eight group keys and SUM in `ORDER BY ... DESC`. It validates the nine bound sort items and confirms lexicographic output order with exact widened BIGINT sums and group counts.
- Verification: the focused `sdk-sql-execute-v10` fixture and full `make -f engine/sdk/Makefile test` suite passed under the pinned TinyCC toolchain; `git diff --check` passed.
- Remaining: broader analytical workload, secure spill, target-hardware, and mixed-workload evidence remains open.

## SQL-7 / SQL-8 follow-up: execute composite SUM at eight grouping keys — verified

- Contract: the v13 SDK runner supports two through eight ordered GROUP BY keys and validates the complete key tuple before publishing each aggregate result.
- Regression: the maintained TinyCC executor fixture runs an eight-key `SUM(measure)` query over three rows. It checks every value in both output tuples, the exact widened BIGINT sums (9 and 8), group row counts (2 and 1), and the v13 binding's key and measure ordinals. The fixture mock planner/executor response buffers now fit the supported nine-column relation shape.
- Verification: the focused `sdk-sql-execute-v10` fixture and full `make -f engine/sdk/Makefile test` suite passed under the pinned TinyCC toolchain; `git diff --check` passed.
- Remaining: broader analytical workload, secure spill, target-hardware, and mixed-workload evidence remains open.

## SQL-7 / SQL-8 follow-up: eight-key composite aggregate planning — verified

- Contract: optimizer-backed composite aggregation accepts two through eight group keys and derives the scan projection from the keys plus the selected aggregate input.
- Regression: optimizer process acceptance now submits both `COUNT(*)` and `SUM(measure)` requests at the eight-key boundary. It validates the returned plan's ordered key ordinals, LIMIT, and scan-column union: COUNT scans only the eight keys, while SUM also scans `measure`.
- Verification: the focused locked Rust optimizer process test, `cargo fmt --all -- --check`, locked workspace build, warning-denied all-target Clippy, and the full serial delegated-cgroup workspace suite passed. The suite ran all sandbox and process acceptance targets with zero ignored tests.
- Remaining: optimizer-backed composite aggregates still have broader workload, secure spill, target-hardware, and mixed-workload evidence open.

## SQL-8 follow-up: COUNT(*) excludes policy-hidden rows — verified

- Contract: aggregate groups and `COUNT(*)` operate only on rows visible through the authorized, row-policy-filtered scan.
- Regression: the confined numeric relation includes a fourth `(4,13)` group denied by the principal's restrictive SELECT policy. An optimizer-backed ordered composite `COUNT(*)` query returns only `(2,9)=3`, `(1,7)=2`, and `(3,11)=1`; the Rust gateway acceptance checks the exact SQLR rows and confirms the hidden group never reaches the result stream.
- Verification: the TinyCC sandbox fixture and release profile built; the focused delegated-cgroup SQL acceptance passed; `make -f engine/sdk/Makefile test`, `cargo fmt --all -- --check`, warning-denied all-target Clippy, and the full serial delegated-cgroup `cargo test --workspace --locked -- --test-threads=1` suite passed. `git diff --check` passed.
- Remaining: broader analytical workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-7 / WI-7 follow-up: seed v22 AVG and negative exponent HAVING fuzz path — verified

- Regression input: the v22 parser corpus now includes a compound predicate that compares hidden `COUNT(*)` and selected `AVG(score)` through OR, with a negative fractional exponent literal. This complements the existing hidden-count SUM seeds and exercises both BIGINT and Decimal operand binding paths under the v22 AST mask oracle.
- Regression: the maintained v22 parser/binder fixture checks the comparison mask, COUNT literal coercion to BIGINT, and exact AVG literal rescaling to `DECIMAL(38,8)` coefficient `-1250000`.
- Verification: the full maintained SDK suite and ASan/UBSan/libFuzzer target `make -f engine/sdk/Makefile fuzz-sql-v22` passed; the fuzzer completed all 10,000 configured runs with the new seed and no crash or sanitizer finding. A separate 60-second campaign over the prior two-seed corpus completed 1,762,546 runs with no finding.
- Remaining: broader analytical workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-7 / SQL-8 follow-up: optimizer-backed INTEGER aggregate NULL matrix — verified

- Contract: optimizer-backed composite grouping preserves INTEGER `MIN`/`MAX` result metadata, emits `AVG` as `DECIMAL(38,6)`, skips NULL inputs, and sorts the all-NULL group last for explicit `NULLS LAST`.
- Regression: the confined authorized numeric relation now includes a nullable INTEGER measure alongside BIGINT and SMALLINT. Optimizer plan kind 14 executes ordered `MIN`, `MAX`, and `AVG` over three composite groups, including a singleton all-NULL group. The gateway acceptance compares every relayed SQLR row byte-for-byte: INTEGER MIN/MAX values retain type code 3, AVG uses type code 5 with scale 6, and all three NULL outputs retain their declared type metadata and sort last.
- Verification: the focused delegated-cgroup `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` test passed; the maintained TinyCC SDK suite passed; `cargo fmt --all -- --check`, warning-denied all-target Clippy, and `git diff --check` passed. The full serial delegated-cgroup workspace suite passed, including all 205 commit tests.
- Remaining: broader analytical workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-8 follow-up: ordered composite aggregate NULL placement — verified

- Contract: composite aggregate ordering with `NULLS LAST` places an all-NULL aggregate result after every non-NULL group while preserving the aggregate's declared SQLR type metadata.
- Regression: confined Decimal, BIGINT, and SMALLINT fixtures each include a third `(3,11)` all-NULL aggregate group. Optimizer-backed MIN, MAX, SUM, and AVG emit it last with canonical NULL SQLR fields and declared result metadata; Decimal COUNT(*) still sorts the three groups by row count. Rust checks every relayed SQLR row and the durable COMMITTED outcome.
- Verification: TinyCC sandbox fixture build, focused delegated-cgroup SQL acceptance, SDK test suite, format check, warning-denied all-target Clippy, and full serial delegated-cgroup workspace test suite passed. The maintained optimizer fixture package is 150,520 bytes and remains under its updated 152,000-byte regression bound.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-7 follow-up: ordered composite aggregate numeric type matrix — verified

- Contract: optimizer-backed composite `GROUP BY` retains the requested aggregate-descending order with an explicit group-key tie-breaker for INTEGER, BIGINT, and SMALLINT aggregate results.
- Regression: the confined SQL fixture now executes INTEGER MIN/MAX/AVG and BIGINT/SMALLINT SUM/MIN/MAX/AVG through the ordered composite optimizer runner. Gateway acceptance checks both group order and exact integer or DECIMAL(38,6) SQLR values; the existing Decimal MIN/MAX/SUM/AVG/COUNT(*) cases remain covered.
- Verification: TinyCC sandbox fixture build, focused delegated-cgroup SQL acceptance, SDK test suite, format check, warning-denied all-target Clippy, and full serial delegated-cgroup workspace test suite passed.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-7 follow-up: ordered composite aggregate optimizer execution — verified

- Contract: add an optimizer request and validated plan for composite `GROUP BY` with COUNT, MIN, MAX, SUM, or AVG, sorted by up to nine distinct selected group-key/aggregate expressions with explicit direction, NULL order, and LIMIT.
- Implementation: optimizer statement 14 now has bounded decoding, authorized relation binding, scan-column derivation, canonical plan encoding/decoding, and optimizer-process dispatch. The C SDK encodes requests, decodes and matches plan kind 14 to the v15 parsed/bound statement, validates the executor relation, and invokes the ordered composite runner. The confined Decimal aggregate matrix orders MIN, MAX, SUM, AVG, and COUNT(*) by aggregate DESC then the first group key ASC, with exact SQLR rows relayed for every family.
- Regression: Rust tests cover request decoding, authorized binding, plan round trips/rejections, optimizer process dispatch, and response validation. Maintained TinyCC fixtures verify request bytes and the executor path; the C plan decoder consumes both composite aggregate and ordered-sort node markers.
- Verification: `make -f engine/sdk/Makefile test`, locked workspace build, format check, warning-denied all-target Clippy, the focused delegated-cgroup `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance, and the full serial delegated-cgroup workspace test suite all passed. The suite reported 205 commit tests and all other workspace test targets passing.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-8 follow-up: redacted aggregate input aborts confined query — verified

- Contract: a required redacted aggregate operand fails the statement; the gateway must not publish partial SQLR group results and must durably abort the invocation.
- Regression: the confined SQL fixture runs Decimal `MIN` under a restrictive cell SELECT policy. It expects the SDK evaluation error before any group callback, and the Rust process acceptance checks for zero result-data frames, an aborted terminal, and a durable ABORTED outcome.
- Verification: focused delegated-cgroup acceptance and the complete serial delegated-cgroup workspace suite passed. The maintained TinyCC fixture build and SDK test suite, format check, warning-denied all-target Clippy, and `git diff --check` passed.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-7 follow-up: grouped integral SUM/AVG runner matrix — verified

- Contract: grouped `SUM` and `AVG` accept SMALLINT, INTEGER, and BIGINT inputs; SUM widens to BIGINT and AVG produces exact DECIMAL(38,6).
- Regression: the maintained v6/v7 SQL runner fixture executes both aggregates for all three integral input types across duplicate and singleton groups. It checks binding input types, row counts, widened result type, and exact average coefficients.
- Verification: focused `sdk-sql-execute-v10` TinyCC fixture and the complete `make -f engine/sdk/Makefile test` suite passed.
- Remaining: secure spill, broader workload coverage, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-7 follow-up: grouped SQL MIN/MAX scalar-type matrix — verified

- Contract: grouped `MIN` and `MAX` bind their argument to the declared scalar type and preserve that type and Decimal metadata in the aggregate result.
- Regression: `sdk_sql_execute_v10.c` now executes the maintained v5 grouped SQL runner for MIN and MAX over BOOLEAN, SMALLINT, INTEGER, BIGINT, DECIMAL, TEXT, VARCHAR, BYTEA, UUID, DATE, and TIMESTAMPTZ. It verifies binding metadata, stable groups and row counts, and exact SQLR field names, result metadata, and value bytes.
- Verification: focused `sdk-sql-execute-v10` TinyCC fixture and the complete `make -f engine/sdk/Makefile test` suite passed; `git diff --check` passed.
- Remaining: secure spill, broader workload coverage, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-7 / SQL-8 follow-up: v21 BIGINT literal HAVING and overflow — verified

- Contract: v21 coerces exact integer HAVING literals to BIGINT aggregate results, and grouped integer SUM overflow fails before callbacks while latching statement failure.
- Regression: the maintained composite runner fixture verifies `HAVING SUM(score) >= 6` filters a lower group and emits two qualifying BIGINT SUM groups with exact row counts. A separate `INT64_MAX + INT64_MAX` group applies an integer literal and asserts evaluation error, zero callbacks, and a latched failure.
- Verification: focused `sdk-sql-execute-v10` TinyCC fixture and the complete `make -f engine/sdk/Makefile test` suite passed; the successful v21 path reaches and evaluates the BIGINT comparison before emission.
- Remaining: secure spill, wider workload and host measurements, and mixed-workload optimization remain open.

## SQL-7 / SQL-8 follow-up: BIGINT SUM overflow through grouped SQL — verified

- Contract: integer `SUM` widens to BIGINT, and a sum outside the signed 64-bit range fails before any group callback; the invocation latches statement failure.
- Regression: the maintained grouped runner fixture feeds `INT64_MAX` and `1` into one BIGINT group and asserts evaluation error, zero callbacks, and a latched statement failure.
- Verification: the focused `sdk-sql-execute-v10` TinyCC fixture and complete `make -f engine/sdk/Makefile test` suite passed.
- Remaining: secure spill, wider workload and host measurements, and mixed-workload optimization remain open.

## SQL-8 follow-up: confined high-cardinality ordered grouped SUM — implemented slice

- Contract: exercise v12 grouped `ORDER BY SUM(measure) DESC NULLS LAST, group_key ASC NULLS FIRST` over the live authorized view, keeping the 128-row and 1 MiB materialization bounds and relaying exact SQLR records through durable commit.
- Implementation: the confined write/read fixture runs the ordered SUM in its separate analytical invocation before the existing composite aggregate matrix. Its 32 KiB scan batch carries multiple 8,172-byte text rows per page, staying below the v12 runner's 124-page ceiling for the 128-row input. A focused `W` fixture parameter runs only this query so the same sandboxed acceptance can isolate its result checks.
- Regression: the C callback validates both sort items, every group-key byte, the exact BIGINT sum and two-row group count. Rust compares all 64 relayed SQLR records byte-for-byte in descending aggregate order and verifies the committed terminal outcome.
- Verification: the focused delegated-cgroup `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` run passed; `make -f engine/sdk/Makefile test`, locked workspace build, format check, and warning-denied all-target Clippy passed. The normal acceptance has passed standalone once, but repeated runs and full workspace runs have also hit `ScanBudgetExceeded` in its earlier primary write/read workload before this new aggregate invocation; the ordered branch is covered separately by the focused mode.
- Remaining: full serial workspace acceptance is still sensitive to the primary fixture's cumulative scan budget under suite load. Broader analytical workload diversity, secure spill, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-8 follow-up: high-cardinality ordered grouped SUM ties — verified

- Contract: the v12 grouped runner sorts selected aggregate results and group keys lexicographically, honoring each key's direction and NULL placement while retaining the 128-row and 1 MiB materialization bounds.
- Regression: the maintained `sdk-sql-execute-v10` fixture streams 128 rows over two scan pages, forming 64 two-row groups with 8,188-byte text keys. Their widened BIGINT SUM values tie in adjacent pairs. At exactly 1 MiB copied key-plus-operand bytes, the callback checks the bound v12 sort metadata, every key byte, exact sum and row count, descending aggregate order, ascending group-key tie order, and all 64 emitted groups. It also compares each encoded grouped SQLR record byte-for-byte with an independently constructed expected record. Increasing every key by one byte fails with `MATERIALIZATION_LIMIT` and zero group callbacks.
- Verification: the focused TinyCC executor fixture passed, followed by the complete `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` suite with the pinned TinyCC toolchain.
- Scope finding: adding the sorted high-cardinality query to the existing live write/read invocation exceeded that invocation's cumulative executor scan budget at a later optimizer-backed DELETE, with both direct and optimizer-backed runners. That experiment was reverted. The confined live-pressure acceptance remains the verified grouped SUM workload; a sorted live-pressure query needs its own invocation budget.
- Remaining: broader analytical workload diversity, secure spill, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-7 follow-up: optimizer-backed composite aggregates — implemented slice

- Contract: the bounded v13 optimizer subset accepts `COUNT(*)`, `MIN`, `MAX`, `SUM`, or `AVG` with two to eight GROUP BY keys and an optional LIMIT. The optimizer request is rejected for unsupported WHERE, HAVING, or ORDER BY clauses; v13 retains its existing bounded execution semantics.
- Implementation: statement kind 13 now decodes and binds the composite group keys and aggregate argument against authorized relation metadata, builds canonical plan kind 13, and validates the plan union of grouping and aggregate scan columns. The C SDK encodes the request, decodes and matches the optimizer plan to its parsed and bound v13 statement, independently resolves executor relation identity, and invokes the bounded v13 grouping runner.
- Regression: Rust tests cover request decoding, authorized binding, plan round trips and scan-union checks, optimizer process dispatch, and generic response validation. Composite aggregate process dispatch now checks COUNT(*), MIN, MAX, SUM, and AVG against the same authorized relation, including the correct scan-column union for COUNT(*) versus argument aggregates. Maintained TinyCC fixtures verify request bytes, plan decoding, and AST/binding-to-plan matching for all five composite aggregate kinds. The confined write/read fixture runs the optimizer-gated composite SUM query over two grouping keys and checks both exact SQLR rows through the gateway relay; its legacy v13 fallback remains covered.
- Verification: locked workspace build, format check, warning-denied Clippy, and the complete maintained TinyCC SDK suite passed for the optimizer implementation. All 19 optimizer process tests passed in the delegated service, including a table-driven pass over every supported aggregate kind. The confined composite SUM query passed alone and in the latest full serial workspace run. That full run passed all 205 commit tests, including `authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes` and `sandboxed_v3_sql_dml_sequence_is_visible_and_commits`; all other workspace test targets passed as well.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-7 follow-up: composite runner aggregate-family matrix — verified

- Contract: the v13 composite grouping runner computes COUNT(*), MIN, MAX, SUM, and AVG over the same authorized group tuple, preserving each aggregate's declared result type and exact numeric representation.
- Regression: the maintained v10 executor fixture now runs all five aggregate kinds over duplicate two-key groups. It checks COUNT row totals, integer MIN/MAX, BIGINT SUM, and exact DECIMAL(38,6) AVG coefficients for INTEGER inputs.
- Verification: focused `sdk-sql-execute-v10` and the complete `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` suite passed with the pinned TinyCC toolchain; `git diff --check` passed.
- Remaining: broader optimizer/runtime pairing across types, analytical workloads, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-7 follow-up: hidden COUNT with grouped AVG — verified

- Contract: v22 permits an unselected `COUNT(*)` comparison in HAVING while the selected aggregate remains COUNT, MIN, MAX, SUM, or AVG.
- Regression: the maintained grouped executor fixture now groups duplicate composite keys, filters on `COUNT(*) >= 2 AND AVG(score) >= 3.00000000e0`, and verifies the matching row count, Decimal group key, and exact widened `DECIMAL(38,8)` average bytes. This adds the AVG path alongside existing hidden-count SUM coverage.
- Verification: the focused `sdk-sql-execute-v10` fixture and complete `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` suite passed with the pinned TinyCC toolchain; `git diff --check` passed.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-7 follow-up: hidden COUNT with grouped MIN/MAX — verified

- Contract: v22 supports hidden `COUNT(*)` HAVING comparisons with every selected aggregate family.
- Regression: the composite grouped executor fixture now checks `COUNT(*) >= 2` with selected Decimal `MIN(score)` and `MAX(score)`, validating each comparison, the shared composite key, matching row count, and exact result coefficient. Together with the SUM and AVG cases, this exercises hidden COUNT against all supported aggregate families.
- Verification: focused `sdk-sql-execute-v10` and complete `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed with the pinned TinyCC toolchain; `git diff --check` passed.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload measurements remain open.

## SQL-4 / AU-5: explicit INSERT column lists — implemented slice

- Added INSERT AST v3 for optional unique explicit column names, retaining the schema-order v2 AST/parser behavior. The v2 executor now maps those names against the resolved executor relation shape, binds values by target ordinal, and passes DEFAULT for omitted fields to trusted storage materialization.
- Maintained C fixtures verify parse spans, duplicate-name rejection, v2 parser compatibility, case-insensitive executor mapping, reordered inputs, and omitted-field DEFAULT state.
- Verification: `nix develop --offline --no-update-lock-file -c make -f engine/sdk/Makefile test` and `make -f engine/sdk/Makefile sandbox-query-fixture` passed. Locked workspace build, format check, warning-denied all-target Clippy, `git diff --check`, and the full serial delegated-cgroup workspace suite passed with sandbox/process tests enabled and zero ignored tests (294 unit, 190 commit, 76 transaction tests, plus all remaining targets). The confined write/read artifact now inserts only the generated identity column while storage materializes and persists a required second column's catalog default; the test checks the committed UUID and default text. Wider default expression forms remain open.

## SQL-4 follow-up: multi-row UPDATE through the 128-row bound — verified

- Contract: UPDATE must materialize all matching targets, close the input scan, and stage the same bound patch for each target. Regressions verify distinct opaque handles and require the cursor's terminal page before any patch is staged.
- Regression: `sdk_sql_execute_v2.c` covers a continued three-row batch and the exact 128-target cap, checks each request's candidate bound, requires scan completion before staging, and asserts the returned count and staged update count.
- Verification: the focused TinyCC fixture and complete `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` suite passed with TinyCC 0.9.27-unstable-2025-01-06.
- Scope: verifies UPDATE runner behavior through the 128-target cap; adversarial live storage coverage remains open.

## SQL-4 follow-up: multi-row DELETE through the 128-row bound — verified

- Contract: the bounded DELETE runner retains every matching row handle through the scan, closes the cursor, and only then stages mutations. Regressions use distinct handles and require the terminal scan page before staging.
- Regression: `sdk_sql_execute_v2.c` covers a continued three-row batch and the exact 128-target cap, verifies each request's candidate bound, requires scan completion before staging, and checks the returned count and staged delete count.
- Verification: the focused TinyCC fixture and complete `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` suite passed with TinyCC 0.9.27-unstable-2025-01-06.
- Scope: verifies DELETE runner behavior through the 128-target cap; larger adversarial storage fixtures and broader live permission cases remain open.

## SQL-7 follow-up: exact fractional and exponent HAVING literals — verified

- Contract: [SQL library](../../specs/sql-library.md). V21 extends compound grouped HAVING with exact signed integer, fractional, and exponent numeric literals while preserving v1-v20 layouts and parser behavior.
- Implementation: the new v21 parser stores normalized signed decimal coefficients and scale; binding requires exact conversion to BIGINT or the selected DECIMAL result scale before scans. The v21 runner carries those canonical bytes through the bounded v18 grouping, validation, ordering, and callback path.
- Regression: maintained TinyCC parser/binder coverage exercises signed and exponent forms, exact decimal rescaling, inexact BIGINT/DECIMAL coercion rejection, malformed exponents, canonical AST validation, and frozen v18 rejection. The executor fixture checks Decimal SUM filtering with `4.50e0`; the ASan/UBSan/libFuzzer harness checks parser and binding invariants. The confined write/read fixture runs v21 grouped COUNT with `2.0e0`, Decimal SUM with `4.50e0`, and Decimal AVG with `1.40e0` against separately authorized tables, then checks the count rows and exact DECIMAL(38,2) and DECIMAL(38,8) SQLR frames through the gateway relay.
- Verification: maintained TinyCC SDK suite, shared-library ABI fixture, sandbox query artifact build, and 10,000 ASan/UBSan/libFuzzer runs passed. Locked workspace build, format check, warning-denied Clippy, and the full serial locked workspace test suite passed inside the delegated service with all confined/process tests enabled. The v21 COUNT, Decimal SUM, and Decimal AVG SQLR frames matched exactly; all enabled test targets reported zero ignored cases. `git diff --check` passed.
- Remaining: broader analytical type/workload coverage, spill support, and end-to-end pressure testing remain open.

## SQL-7 follow-up: v21 Decimal literal boundary coercion — verified

- Contract: [SQL library](../../specs/sql-library.md). V21 rejects literals beyond its 38-digit/scale parse bounds and rejects exact Decimal rescaling that would exceed precision 38 before any scan.
- Regression: the maintained v21 parser/binder fixture now verifies that `1e36` parses but fails to bind against a `DECIMAL(38,2)` SUM result because rescaling requires 39 digits. It also verifies that `1e-100` exceeds the literal scale bound and returns `LIMIT_EXCEEDED` during parsing.
- Verification: focused TinyCC fixture and the complete maintained `make -f engine/sdk/Makefile test` suite passed in the pinned Nix shell using TinyCC 0.9.27-unstable-2025-01-06. `git diff --check` passed.
- Scope: this protects exact decimal coercion boundaries; broader analytical type/workload coverage, spill support, and end-to-end pressure testing remain open.

## SQL-7 follow-up: v21 Decimal MIN HAVING execution — verified

- Contract: v21's exact Decimal literal binding and composite grouped runner apply to each supported aggregate result shape, including Decimal MIN.
- Regression: the maintained grouped executor fixture runs `MIN(score) <= 3.00e0` over Decimal rows, checks the bound v21 literal and MIN metadata in the callback, and verifies only the qualifying group is emitted with the expected Decimal coefficient.
- Verification: focused `sdk-sql-execute-v10` and the full `make -f engine/sdk/Makefile test` suite passed with the pinned TinyCC toolchain. `git diff --check` passed.
- Scope: this covers v21 Decimal MIN comparison in the bounded runner; broader workload coverage and confined end-to-end result verification remain open.

## SQL-5 follow-up: confined indexed-range equivalence — verified

- Contract: [SQL library](../../specs/sql-library.md) and [planner/compiler](../../specs/planner-compiler.md). A validated two-sided leading-key range over an authorized non-unique index must return the same projected rows as the normal authorized SQL scan on the invocation snapshot.
- Regression: `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` now runs `left_key >= $1 AND left_key < $2` over the real `grouped_left_key_range` index, collects the optimizer results and reference scan results, and compares their values and multiplicity. The seeded `(1, 7), (1, 7), (2, 9)` rows make the expected range contain two duplicate key values.
- Verification: the acceptance passed inside the delegated `systemd --user` service with all seven cancellation workers and the 64-thread operational workload active. The focused run completed 3,085 operational commits during its 30-second interval; the SQL worker also passed the existing cancellation, backpressure, and durable-commit assertions. TinyCC rebuilt the confined artifact with `-Wall -Wextra -Werror`.
- Scope: this proves equivalence for the supported exact two-sided INTEGER range over one non-unique index and one invocation snapshot. It does not broaden planner range extraction or replace the separately tested fallback/malformed-result cases.

## SQL-8 follow-up: exact group-key memory bound through confined SQL — verified

- Regression: the maintained confined write/read artifact groups 128 distinct 8,192-byte text keys with `COUNT(*)` through `prdb_sdk_sql_execute_group_count_v4`. Its callback validates all key bytes and groups, and its copied group-key payload reaches exactly 1 MiB. The enabled sandbox acceptance overlaps the query with operational reads and durable commits and verifies the callback marker and complete invocation result path.
- Red/green: the acceptance failed with the pre-change artifact because `insert-visible` arrived where the new marker was expected; rebuilding the TinyCC artifact made it pass. The updated acceptance then passed five sequential fresh-database repetitions in the delegated user service (`1 passed, 0 ignored` each). The cancellation fixture's check interval was extended to 2 seconds so admission work can finish after readiness while the acceptance retains its separate two-second cancellation-observation bound.
- Scope: the five-run timing and sampled cgroup figures are local comparative measurements at the current bound. Broader client saturation and target-hardware measurements remain open.

## SQL-5 dependency: borrowed SDK views for executor index-range results — verified

- Contract: [client SDK](../../specs/client-sdk.md) and [IPC protocol](../../specs/ipc-protocol.md). Decode the bounded range response without inventing row handles; sanitized cells borrow the immutable response buffer, and `unavailable` explicitly requests the authorized reference path.
- Red/green: the maintained TinyCC batch fixture first failed to compile because the range-view API was absent. It now checks a typed row/cell read, unavailable status, malformed state, excessive row count, truncation, and trailing-byte rejection.
- Implementation: `prdb_sdk_index_range_v1` validates the entire response before publishing a range view and provides row/cell iterators with no row-handle field.
- Verification: focused TinyCC batch fixture and complete `make -f engine/sdk/Makefile test` passed, including 8 ABI generator tests and the SDK release metadata checks.
- Follow-up complete: the SDK submission path and optimizer range selection are exercised by the confined indexed-range equivalence acceptance above. That acceptance proves the supported two-sided INTEGER case over one non-unique index; other bound types and fallback/malformed-result paths remain limited to their focused fixtures.

## SQL-5 follow-up: direct executor range submission — verified

- Contract: [client SDK](../../specs/client-sdk.md) and [IPC protocol](../../specs/ipc-protocol.md). A caller can submit typed bounds and selected columns through the checked relation handle; unavailable results preserve the caller's range output and require scan fallback.
- Red/green: the TinyCC executor fixture first failed to compile because operation `0x1115` and its SDK call were missing. The fixture now checks exact request bytes, decoded empty-range results, unavailable/malformed responses, absent bounds, and undersized request buffers without unintended callbacks.
- Implementation: `prdb_sdk_executor_scan_index_range_v1` encodes typed bounds, validates nonzero unique column IDs and the request budget, calls executor IPC, and publishes only a validated borrowed range view.
- Verification: focused TinyCC executor fixture, full `make -f engine/sdk/Makefile test`, and `make -f engine/sdk/Makefile sandbox-query-fixture` passed.
- Follow-up complete: validated range hints are submitted through this operation by the optimizer-backed row runner, which matches returned rows against the parsed filter and falls back to the authorized full scan when the range is unavailable or over-bound. The confined indexed-range equivalence acceptance above verifies the supported two-sided INTEGER case on one invocation snapshot.

## SQL-8 follow-up: grouped ORDER BY limits and cancellation — verified

- Contract: grouped sort stays within the existing 128 matching-row/group and 1 MiB materialization bounds. A bound violation fails before any group callback; cancellation is checked during output so a later group is not published after cancellation.
- Regression: the maintained v12 executor fixture orders 128 distinct COUNT groups and verifies the complete ascending-key result after aggregate ties. It streams a 129th row on a second cursor page and verifies materialization failure with zero callbacks. A 40-group sort cancels after 32 callbacks and verifies failure before callback 33.
- Verification: the focused `sdk-sql-execute-v10` fixture and complete TinyCC SDK suite passed. Locked workspace build, format, warning-denied Clippy, ABI drift check, and the serial workspace suite passed inside the delegated `systemd --user` service; commit integration reported 142 passed, zero ignored, metadata process integration reported 2 passed, and every test target reported zero ignored.
- Remaining: byte-budget boundary coverage for grouped sorting, concurrent operational latency under real confined analytics, and wider workload/host measurements remain open.

## SQL-8 follow-up: grouped ORDER BY byte budget — verified

- Contract: the grouped runner retains no more than 1 MiB of copied group-key and aggregate operand bytes. Exactly 1 MiB is accepted; excess input fails before invoking the group callback.
- Regression: the maintained v12 executor fixture streams 128 distinct 8,192-byte text keys across two cursor pages and verifies all 128 ordered COUNT results at exactly 1,048,576 copied bytes. It repeats with 8,193-byte keys (1,048,704 bytes total) and verifies `MATERIALIZATION_LIMIT`, zero callbacks, and a latched statement failure.
- Verification: focused `sdk-sql-execute-v10` and the full TinyCC SDK suite passed. Locked workspace build, format, warning-denied Clippy, ABI drift check, and the serial workspace suite passed inside the delegated `systemd --user` service. Commit integration reported 142 passed/zero ignored; metadata process integration reported 2 passed; every test target reported zero ignored cases.
- Remaining: concurrent operational latency under real confined analytics and broader workload/host measurements remain open.

## SQL-8: bounded ordered SELECT optimizer path — implemented

- Contract: an explicit single-column `ORDER BY` key is resolved through the authorized relation description. Its source column may remain hidden from the SELECT projection; LIMIT is applied after stable ordering by the bounded SQL v3 runner or optimizer-backed ordered SELECT path.
- Implementation: ordered SELECT statement 7 is decoded, bound to stable column IDs, and dispatched through the optimizer submission process. The canonical kind-7 plan includes the sorted union of projected columns and the hidden order key, direction, and optional LIMIT. Rust optimizer responses and the maintained C SDK independently validate the plan. The SDK encodes SQL v3 ordered SELECT ASTs, matches plans against their parsed relation/projections/order/LIMIT, independently re-resolves executor metadata, scans with a 124-page request budget, materializes up to 128 rows/1 MiB, stable-sorts by the authorized typed key, applies LIMIT after sorting, and polls cancellation before sorting and every 32 output rows. Errors latch statement failure.
- Evidence: focused Rust request, binding, plan round-trip/rejection, and optimizer-process dispatch tests passed. The TinyCC SDK suite passed exact ordered request-byte, hidden order-key decoding, AST match, and mutation rejection checks. The maintained optimizer fixture now also executes `LIMIT 0`, verifies that planner and executor relation identities are still resolved, and asserts that no scan opens and no row callback runs. `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` passed inside the delegated user service after checking exact relayed SQLR rows for `SELECT right_key FROM grouped_records ORDER BY left_key DESC LIMIT 2` (`9`, then `7`). Cgroup tests remain enabled and the test targets report zero ignored tests.
- Remaining: the v3 row ORDER BY grammar remains single-key; broader analytical memory-budget, spill, and mixed-workload acceptance remains open.

## SQL-8 dependency: maximum-cardinality stable sort regression — verified

- Contract: the single-key sorter accepts exactly 128 authorized values in either direction, preserves input order for ties, and returns a complete permutation; 129 values fail with `LIMIT_EXCEEDED`.
- Regression: the maintained TinyCC sorter fixture now feeds 128 typed INTEGER keys with repeated values through ascending and descending sorts. An independent property check verifies monotonic order, stable tie ordering, and that each source row appears exactly once. The adjacent 129-row rejection remains covered.
- Verification: `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed, including the full maintained SDK fixture suite with TinyCC 0.9.27-unstable-2025-01-06.
- Remaining: broader analytical memory-pressure, spill, and mixed-workload evidence remain open.

## SQL-8 dependency: optimizer-backed ordered SELECT bounds — verified

- Contract: a zero LIMIT returns no rows after plan/authority validation and does not open a scan.
- Regression: the optimizer executor fixture submits a kind-7 plan for `SELECT c0 FROM items ORDER BY c3 DESC LIMIT 0`; it checks zero output, no callback or statement failure, and only planner plus executor relation resolution operations.
- Verification: the focused `sdk-optimizer-plan-v1` fixture and the complete `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` suite passed.
- Materialization regression: a valid one-row executor batch with a caller scratch capacity too small for its row returns `RESPONSE_TOO_SMALL`, latches statement failure, emits no projected callback, and confirms the scan reached `NEXT_BATCH`.
- Cancellation regression: a 33-row ordered batch with cancellation on the fourth poll publishes 32 rows, returns `CANCELLED` before the 33rd callback, and latches statement failure.
- Remaining: wider analytical memory-pressure/spill/mixed-workload evidence remains open.

## SQL-7: filtered aliased INNER JOIN runner and mandatory cgroup acceptance — implemented

- Contract: [SQL library](../../specs/sql-library.md). V20 adds one alias-qualified comparison against a positional parameter or `IS [NOT] NULL` to the v19 INNER JOIN grammar. Only TRUE candidates survive; NULL comparisons are UNKNOWN, required REDACTED values fail, and LIMIT applies after filtering.
- Implementation: [`prdb_sdk_sql_v20.h`](../../engine/include/prdb_sdk_sql_v20.h) and [`prdb_sdk_sql_exec_v20.h`](../../engine/include/prdb_sdk_sql_exec_v20.h) define the versioned AST, binding, callback, and runner. The runner validates typed parameters before scans, checks independent planner/executor shapes, adds a hidden filter projection when needed, and exposes only selected columns to callbacks. The v20 scan ceiling is 119 pages to reserve its four preflight requests within the 256-request connection budget. Earlier versions remain unchanged.
- Regression and fuzzing: maintained TinyCC fixtures cover aliased filter binding, filter-before-LIMIT output, unchanged v19 WHERE rejection, invalid parameter indexes, and REDACTED filter failure with zero output and a latched statement error. The new ASan/UBSan/libFuzzer v20 target completed 10,000 runs from five grammar seeds (1,299 coverage edges; 2,852 features).
- Cgroup coverage: removed `#[ignore]` from the four sandboxed commit tests and the disconnected-submission metadata process test. The complete workspace suite now runs these acceptance paths when invoked under the documented delegated `systemd --user` service with fixture and runtime variables; it no longer reports them as ignored. [`SANDBOX_TEST.md`](SANDBOX_TEST.md) and [`AGENTS.md`](../../AGENTS.md) document the required invocation.
- Verification: TinyCC `make -f engine/sdk/Makefile test`, `make -f engine/sdk/Makefile sandbox-query-fixture`, and the v20 10,000-run fuzzer passed. Locked workspace build, format check, warning-denied Clippy, `git diff --check`, and the full serial locked workspace test suite passed inside the delegated service. All four sandboxed commit tests passed; both metadata process tests passed; the test targets reported zero ignored cases.
- Remaining: broader analytical type/workload coverage, spill support, and end-to-end pressure testing remain open.

## SQL-7 follow-up: v20 filtered join through confined SQLR — verified

- Contract: the V20 SQL runner must preserve the filtered projection when producing SQLR output inside the normal sandboxed executor path, then carry the provisional record through result relay and durable transaction completion.
- Regression: the maintained `sql_v3_write_read.c` artifact runs a two-key aliased join with `WHERE l.id >= $1 LIMIT 1`, forces multiple join candidates, and encodes only the selected `g.left_key` and `l.id` outputs. The confined commit test checks the exact SQLR frame `(1, 42)` after relay.
- Implementation: V20 is now exercised by the maintained ABI v3 query artifact; `make -f engine/sdk/Makefile sandbox-query-fixture` rebuilds it when either V20 header changes. The regression uses existing authorized relation resolution and two-scan callbacks, with no new result or authority path.
- Verification: `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` passed inside a delegated `systemd --user` service with the configured NixOS loader/libc/`libgcc_s` allowlist. The full workspace suite passed with all cgroup tests enabled; the focused acceptance was rerun after adding the v20 SQLR frame and again after routing the two-key v19 join through optimizer submission.
- Remaining: optimizer-gated single-key COUNT plus grouped ORDER BY now passes confined acceptance; optimizer kinds for grouped sort expressions and wider pressure/mixed-workload evidence remain open.

## SQL-4 / IPC: sequential SQL request budget — implemented

- Red evidence: the expanded maintained SQL invocation exceeded the executor stream's 256-frame lifetime cap near its final optimizer-backed SELECT, so the confined write/read/analytics acceptance aborted with `RequestLimitExceeded` after earlier v19 join outputs.
- Decision: the connection-level frame cap is now 512 to support multiple sequential bounded SQL operators in one invocation. Each operator keeps its independent 256-request ceiling; the request-ID set remains finite and bounded.
- Verification: the full `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed through the confined executor and durable commit in a delegated `systemd --user` service. The four ignored commit sandbox tests and the disconnected-submission reconciliation test also passed under the same service setup.

## SQL-7 dependency: v19 alias parser and binder fuzzing — implemented

- Contract: [SQL library](../../specs/sql-library.md). Exercise v19 aliased INNER JOIN parsing and binding with bounded arbitrary SQL input, checking AST ABI tags, nested v16 layout, projection/join bounds, alias spans, and successful binding output.
- Implementation: [`sql_v19_fuzz.c`](../../engine/tests/fuzz/sql_v19_fuzz.c) binds against fixed `lefts` and `rights` shapes. Four maintained seeds cover aliased joins, self-joins, case-insensitive aliases, and malformed aliases; `fuzz-sql-v19` builds with libFuzzer, ASan, and UBSan.
- Verification: `nix shell nixpkgs#clang -c make -f engine/sdk/Makefile fuzz-sql-v19` completed 10,000 runs (1,045 coverage edges; 2,009 features). The repository's locked development shell does not include Clang, so the temporary Nix shell supplied the existing fuzz target's compiler requirement.
- Limitation: this target fuzzes the parser and binder only; runner behavior remains covered by the maintained TinyCC fixture and executor tests.

## SQL-7 dependency: v19 INNER JOIN alias parser and binder — implemented

- Contract: [SQL library](../../specs/sql-library.md). V19 adds optional explicit `AS` aliases to the v16 two-to-eight-key INNER JOIN grammar while preserving v16 layouts and rejecting duplicate aliases case-insensitively.
- Red/green evidence: the TinyCC fixture parses and binds aliased projections and two join keys, checks each resolved side/ordinal, rejects duplicate aliases during binding, rejects a missing alias, and verifies malformed spans and failures leave output records unchanged. The focused fixture first exposed an invalid embedded v8 ABI version; fixing the AST version assignment made it pass.
- Implementation: [`prdb_sdk_sql_v19.h`](../../engine/include/prdb_sdk_sql_v19.h) defines sized v19 AST/binding records. The parser stores alias source spans and v19 binding validates spans, maps alias-qualified references to the original relation spans, and delegates relation/column/type validation to the frozen v16 binder. The maintained Makefile builds and runs the fixture as part of `test`.
- Limitation: this parser/binder increment did not include execution; aliased execution was added in the following increment.
- Verification: focused TinyCC v19 fixture and complete `make -f engine/sdk/Makefile test` passed. Locked `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked` (138 unit tests passed, 4 cgroup-dependent tests ignored, plus all active integration/doc tests), and `git diff --check` passed. Confined SQL acceptance was not rerun because this increment changes only parser/binder ABI; existing cgroup-dependent tests remain unavailable in the default run.

## SQL-7 dependency: v19 aliased INNER JOIN runner — implemented; confined acceptance passed

- Contract: [SQL library](../../specs/sql-library.md). Execute aliased v19 joins through independent planner and executor resolution while preserving the established two-scan, materialization, pair-count, request, cancellation, and result-callback bounds.
- Red/green evidence: the v19 runner fixture first failed to link because `prdb_sdk_sql_execute_inner_join_v19` was absent. It now executes aliased single-key joins and the existing two-to-eight-key regression matrix, including LIMIT, NULL, REDACTED, materialization exhaustion, cancellation, callback failure, and request-bound cases.
- Implementation: [`prdb_sdk_sql_exec_v19.h`](../../engine/include/prdb_sdk_sql_exec_v19.h) and `sql_exec_v19.c` expose the v19 callback binding and preserve v16's full bounded execution path. The maintained `sql_v3_write_read.c` sandbox artifact now executes its two-key join with aliases and passes the v19 binding's embedded v16 projection record to the established SQLR writer.
- Verification: full TinyCC SDK suite, `make -f engine/sdk/Makefile sandbox-query-fixture`, locked workspace build, format check, warning-denied Clippy, workspace tests (138 unit tests passed; 4 cgroup-dependent tests ignored in the default run; all active integration and documentation tests passed), and `git diff --check` passed. The opt-in confined durable acceptance passed inside a delegated `systemd --user` service using [`with_delegated_cgroup.sh`](../../engine/tools/with_delegated_cgroup.sh).

## SQL-7 dependency: v19 self-joins — implemented; confined acceptance passed

- Contract: [SQL library](../../specs/sql-library.md). Permit two aliases of the same resolved relation to serve as distinct join sides while preserving left/right predicate binding and independent scans.
- Red/green evidence: the parser/binder fixture initially failed because rebasing aliases to the shared relation name made both sides ambiguous. It now verifies distinct projection sides and ordinals against two identical relation shapes. The runner fixture initially failed on that binding path and the equal relation-ID guard; it now executes both aliased scans and returns stable self-match rows.
- Implementation: v19 binding verifies each source relation name against its independently supplied name, then delegates to v16 binding using the distinct alias spans as side identities. The v19 runner allows equal relation IDs while still checking each planner/executor shape pair. The maintained sandbox artifact now exercises `linked_records AS a INNER JOIN linked_records AS b` and checks the two SQLR frames in the integration test.
- Verification: focused v19 parser/binder and runner fixtures, full TinyCC SDK suite, and `make -f engine/sdk/Makefile sandbox-query-fixture` passed. Locked workspace build, format check, warning-denied Clippy, workspace tests (138 unit tests passed; 4 cgroup-dependent tests ignored in the default run; all active integration and documentation tests passed), and `git diff --check` passed. The extended v19 aliased-join/self-join acceptance passed inside a delegated `systemd --user` service.

## SQL-4: bounded multi-row INSERT — implemented; confined acceptance passed

- Contract: [SQL library](../../specs/sql-library.md) and [client SDK](../../specs/client-sdk.md). The versioned DML v2 parser accepts 1–127 positional VALUES tuples with equal positive arity and at most 4,096 total cells; v1 stays single-row-only. Each row is type-bound and staged in invocation order, with a cancellation check before each stage. The 127-row cap plus one relation resolve keeps the executor stream at 255 calls.
- Red/green evidence: the maintained TinyCC fixture checks flat cell order, parameters/NULL/DEFAULT forms, one-row compatibility, mismatched arity, v1 rejection, the 127-row bound, successful two-row staging, later-row executor failure with unchanged count output, and cancellation after the first staged row.
- Verification: focused parser and executor fixtures passed after forced TinyCC rebuild. The full Rust workspace build, format check, warning-denied Clippy, serial workspace tests, and complete maintained TinyCC SDK suite pass. The maintained confined SQL artifact now calls v2 for two rows and reads both through a following SELECT; `make -f engine/sdk/Makefile sandbox-query-fixture` builds it. The cgroup-dependent acceptance remains unexecuted because this host mounts `/sys/fs/cgroup` read-only and disables sudo with no-new-privileges; the three cgroup-dependent integration tests remain ignored in the default run.
- Verification extension: `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` passed inside a delegated `systemd --user` service, including the maintained two-row insert/read path and final durable commit. Broader SQL DML and workload coverage remain open.

## SQL-3 ABI v4 optimizer callback in maintained SQL artifact — implemented; confined acceptance passed

- Contract: [client SDK ABI v4](../../specs/client-sdk.md), [optimizer](../../specs/planner-compiler.md), and [sandbox acceptance](SANDBOX_TEST.md). The existing write/read query fixture exports v3 compatibility and v4 entry points; the executor's v4 preference now reaches the fixed-operation optimizer callback.
- Behavior: after updating `records.id` to 42, the v4 entry parses `SELECT id FROM records WHERE id = $1 LIMIT 1`, submits it through the optimizer process, validates the returned filtered plan and storage-resolved relation, and confirms one projected integer value equals 42. Failure is reported through the established statement failure callback; the fixture emits no extra success result frames.
- Verification: `make -f engine/sdk/Makefile test`, `make -f engine/sdk/Makefile sandbox-query-fixture`, workspace build, format check, warning-denied Clippy, and `cargo test --workspace --locked` passed after the edit (129 commit tests passed; 3 cgroup tests ignored). `prdb-packager` compiled the actual fixture with TinyCC and validated/published its ABI v4 PMF1 artifact in `/tmp` without loading it. The ignored sandboxed acceptance cannot run on this read-only cgroup host, so optimizer-to-storage execution remains unverified here.
- Verification extension: the optimizer-backed SELECT also passed through the confined executor as part of `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` in the delegated user service. Broader optimizer/SQL parity remains open.

## SQL-1 first bounded SELECT parser — implemented

- Contract: [`prdb_sdk_sql_v1.h`](../../engine/include/prdb_sdk_sql_v1.h) defines the v1 sized/versioned borrowed-span AST for explicit column projections or `*`, one unqualified relation, one comparison against `$n` or `IS [NOT] NULL`, optional LIMIT and one optional semicolon. Keywords are case-insensitive; identifier source spelling is preserved.
- Bounds and rejection: query 65,536 bytes; identifier 128 bytes; projection 64 columns; parameter index 1–65,535; LIMIT 0–1,000,000. Unsupported statement kinds return `UNSUPPORTED_STATEMENT`; malformed/incrementally unsupported syntax returns `SYNTAX_ERROR`; bounds return `LIMIT_EXCEEDED`. The parser copies no SQL or names and does not bind parameters or execute a plan.
- Implementation: [`sql_v1.c`](../../engine/sdk/sql_v1.c) performs bounded tokenization and parsing, stores source offsets/lengths, and is included in the TinyCC SDK archive.
- Regression: [`sdk_sql_select_v1.c`](../../engine/tests/fixtures/sdk_sql_select_v1.c) checks the documented parameterized SELECT, case-insensitive keywords, preserved identifier bytes, star projection, LIMIT 0, `IS NULL` and `IS NOT NULL` without a parameter, malformed null tests, DELETE rejection, malformed SELECT, second-statement rejection, too many columns, query length, parameter range, and LIMIT range. Initial TinyCC compile failed because `prdb_sdk_sql_v1.h` was absent; the fixture passes after implementation.
- Remaining: the parser is now composed with planner and executor bridges and bounded SELECT/DML runners in later SQL-3/SQL-4 increments. The initial SELECT grammar still lacks joins, grouping, sorting, and information_schema SQL integration.

## SQL-3 bounded Boolean SELECT parser — implemented

- Contract: [`prdb_sdk_sql_v2.h`](../../engine/include/prdb_sdk_sql_v2.h) adds a separately versioned SELECT record so v1 layout remains unchanged. V2 supports comparisons, `IS [NOT] NULL`, unary NOT, AND, OR, and parentheses with SQL precedence.
- Bounds and representation: predicates use a postorder array of at most 64 nodes with zero-based child indices and a root index; NOT and parenthesis nesting is bounded to 32. Source column spans borrow immutable SQL bytes.
- Regression: [`sdk_sql_select_v2.c`](../../engine/tests/fixtures/sdk_sql_select_v2.c) checks mixed precedence, grouping, comparison operators, null tests, parameter indices, malformed expressions, and both AST bounds. Before implementation it failed to link because the v2 parser entry point was missing.
- Regression: the fixture uses a resolver that counts column reads; it proves AND/FALSE skips an entire branch, OR/TRUE skips a REDACTED leaf, reached REDACTED errors propagate, UNKNOWN combines correctly with FALSE, missing parameters fail, no-WHERE evaluates TRUE, and malformed tree edges are rejected. Before implementation it failed to link because the v2 evaluator symbol was absent.
- Bound evaluation: `prdb_sdk_sql_eval_bound_select_predicate_v2` consumes the binding, resolves reached leaves by zero-based column ordinal, and takes type/Decimal metadata from the bound record. TinyCC regressions prove ordinal access, three-valued results, short-circuiting, callback error propagation and malformed-binding rejection before any row access. The first fixture run failed to link because the bound evaluator symbol was absent.
- Planner-to-binding bridge: `prdb_sdk_planner_decode_relation_v1` validates the planner's binary `describe_relation` response and exposes borrowed names and SQL column shapes, including Decimal metadata. Its regression feeds the decoded shape directly to the v2 SQL binder; planner handles remain distinct from executor handles. The first TinyCC link failed because the decoder symbol was absent.
- Batch-row evaluation: `prdb_sdk_sql_eval_bound_select_row_v2` decodes each cell of one full-shape authorized row into caller-supplied bounded scratch and evaluates using binding ordinals. Its fixture uses real version-1 batch bytes and checks SQL NULL, short-circuit over REDACTED, shape mismatch, and scratch bounds. The row and batch storage remain borrowed; the helper makes no row authorization decision.
- Executor host bridge: `prdb_sdk_executor_v1.h` builds the version-1 executor request payloads and invokes the host callback for `resolve_relation`, full-shape `open_scan`, `next_batch`, and `close_cursor`. The independent relation decoder requires handle kind 6 and exposes the shape for binding; planner kind 9 is not accepted. The resolver requires explicit schema and relation names, since the invocation ABI has no default-schema field. The maintained fixture checks exact operation tags and payload bytes, response decoding, batch validation, and invalid handle rejection.
- Planner host bridge: `prdb_sdk_planner_describe_relation_v1` submits an explicitly qualified relation name through the host planner callback using `0x1201`, then decodes the authorized shape and kind-9 planner handle. Its maintained fixture checks operation routing, exact qualified-name payload bytes, response decoding, and handle kind.
- Read-only SELECT runner: `prdb_sdk_sql_execute_select_v2` composes parse, planner shape lookup, independent executor resolution, relation-shape consistency checks, bind, static typed-parameter validation, full-shape authorized scans, bound predicate evaluation, LIMIT, and cursor closure. A synchronous callback receives matching sanitized source rows and the projection binding. The fixture checks a parameterized SELECT result, exact planner/executor request sequence, continuation cursor close at LIMIT and callback failure, and rejects parameter type mismatch before opening a scan.
- Sticky SQL failure propagation: the v3 query host ABI adds a statement-failure callback. The SQL runner calls it for every failed statement result, and the executor forces a nonzero exit after query entry returns even if C catches the status. Existing v1/v2 query entry points remain supported. Rust ABI/layout and worker-status tests, plus the TinyCC SELECT fixture, verify the latch path.
- Abort after partial results: the supervisor integration test relays provisional SQL output, observes worker exit failure, verifies the durable invocation outcome is ABORTED, and emits the aborted terminal event after the provisional data.
- Maintained sandbox artifact: [`sql_v3_caught_error.c`](../../engine/tests/fixtures/sql_v3_caught_error.c) writes provisional output, catches an unsupported DELETE error from the SQL runner, and returns success so the v3 host latch must force process failure. `make -f engine/sdk/Makefile sandbox-query-fixture` builds it as an untrusted `.so`; the Makefile explicitly forbids executing it outside the executor sandbox.
- Rust vertical integration harness: `sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output` stages the artifact through an authenticated client, launches it with the configured cgroup/Landlock profile, relays provisional output, and verifies the durable ABORTED outcome and terminal ordering. It is opt-in and requires the exact host cgroup/runtime paths documented in [`SANDBOX_TEST.md`](SANDBOX_TEST.md).
- Verification for sticky statement failures: Rust v3 ABI/layout and descriptor tests, the executor latch-to-exit test, the partial-result durable-abort integration test, maintained TinyCC suite, full workspace build, format, Clippy, workspace tests, and diff check passed. The opt-in durable-abort integration also passed through the confined executor using the temporary delegated cgroup and configured runtime profile.
- Verification for planner/executor/SELECT bridge: maintained TinyCC SDK fixtures, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check` passed.
- Verification: the full maintained TinyCC SDK fixture suite and `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check` passed.
- Remaining: statement lifecycle integration, sequential result-stream rules, cancellation/deadline notification, planner physical plans, and SQL-backed information_schema remain open. The maintained v3 artifacts have passed their confined acceptance paths. The runner enforces a bounded number of executor requests and closes the latest continuation cursor when it stops early.

## SQL-4 dependency: bounded positional INSERT parser and storage submission — partial

- Contract: [SQL library](../../specs/sql-library.md). `INSERT INTO relation VALUES (...)` accepts a nonempty full-row positional list of `$n`, `NULL`, or `DEFAULT`, optionally followed by one semicolon. The AST borrows source SQL, stores no strings, caps at 4,096 entries, and rejects trailing statements. At execution the number of entries must equal the declared schema column count.
- Red/green evidence: after adding the public AST header and fixture scaffolding, TinyCC link failed because `prdb_sdk_sql_parse_insert_v1` was missing. The implementation reuses the SELECT parser's bounded tokenizer and preserves the output-on-success-only rule. The regression checks identifier span, `$2`, DEFAULT, NULL, `$0`, second-statement rejection, and unsupported UPDATE.
- Execution: `prdb_sdk_sql_execute_insert_v1` obtains planner and executor shapes independently, checks they agree, enforces exact full-row arity, binds parameter values to declared scalar/Decimal types, preserves NULL/DEFAULT states, submits `stage_insert`, and returns its provisional opaque row handle. Every valid-host failure latches SQL statement failure.
- Regression: the maintained SQL execution fixture first failed to link because `prdb_sdk_sql_execute_insert_v1` was absent. It now verifies the exact three-column operation payload for INTEGER 42, NULL, and DEFAULT, returned row-handle kind, planner/executor request counts, and a BIGINT/INTEGER mismatch that fails before stage mutation and latches the statement.
- Verification: `make -f engine/sdk/Makefile test`, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check` passed in the required Nix shell. The combined INSERT/UPDATE/DELETE sequence also passed through confined execution and durable commit.
- Limits/next: this remains one-row INSERT only. Default-schema/catalog constraints and broader live SQL/storage coverage remain open.

## SQL-4 dependency: bounded UPDATE parser — partial

- Contract: [SQL library](../../specs/sql-library.md). Parse `UPDATE relation SET column = ($n|NULL)[, ...] WHERE predicate`; WHERE reuses the bounded v2 Boolean grammar. At most 64 assignments are accepted, duplicate column names are rejected case-insensitively, and omission of WHERE is not yet supported.
- Red/green evidence: after adding the sized UPDATE AST contract, the fixture link failed because `prdb_sdk_sql_parse_update_v1` was absent. The parser reuses the bounded tokenizer and predicate-tree parser, and populates the assignment columns into the v2 binding projection so the execution layer can map them to ordinals. Tests check parameter/NULL assignments, Boolean predicate tree structure, case-folded duplicate rejection, zero parameter rejection, and output preservation on failure.
- Verification: focused DML parser fixture passed under TinyCC 0.9.27-unstable-2025-01-06.
- Execution: `prdb_sdk_sql_execute_update_v1` binds SET ordinals and typed values, validates predicate parameters before scanning, evaluates authorized full-shape rows, copies at most 128 matching row handles into caller-owned storage, closes the scan, then stages the same patch set per target. It budgets its own executor requests before staging and invokes sticky failure on any error.
- Regression: the SQL runner fixture checks exact `stage_update` bytes after fetching a matching row and receiving a finished continuation page. It also stages two distinct matching rows successfully and asserts the scan has finished before either stage call. A two-row result with a one-target buffer reports `MATERIALIZATION_LIMIT`, latches failure, leaves the output count unchanged, and sends no update operation.
- Verification: full `make -f engine/sdk/Makefile test`, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed in the required Nix shell with TinyCC 0.9.27-unstable-2025-01-06.
- Limits/next: UPDATE staging is covered through the 128-target cap by the follow-up above. Adversarial live storage coverage remains open; the combined INSERT/SELECT/UPDATE/SELECT/DELETE/SELECT fixture verifies own-write visibility and durable commit through the confined executor.

## SQL-4 dependency: bounded DELETE parser and runner — partial

- Contract: `DELETE FROM relation WHERE predicate` requires a WHERE clause and supports the bounded v2 predicate grammar, plus one optional final semicolon. The parser stores borrowed spans and a v2 predicate AST, rejects trailing syntax, and only writes its output on success.
- Implementation: `prdb_sdk_sql_parse_delete_v1` parses the grammar. `prdb_sdk_sql_execute_delete_v1` compares planner/executor relation shapes, binds typed predicate parameters, evaluates authorized full-shape scan rows, retains at most the caller capacity (maximum 128) of opaque row handles, closes the scan, and then stages deletes. Materialization or 256-request budget failures happen before staging; all execution failures latch the v3 statement-failure callback.
- Regression: [`sdk_sql_dml_v1.c`](../../engine/tests/fixtures/sdk_sql_dml_v1.c) covers a Boolean/NULL predicate and rejects missing WHERE. [`sdk_sql_execute_v2.c`](../../engine/tests/fixtures/sdk_sql_execute_v2.c) covers successful one-row deletion, successful deletion of two distinct matching rows after scan completion, and target-capacity overflow; the overflow case confirms scan closure, unchanged output count, sticky failure, and no delete request.
- Verification: focused TinyCC parser and SQL execution fixtures pass; the full maintained TinyCC SDK fixture suite passes. Rust 1.95.0 build, `cargo fmt --all -- --check`, Clippy with warnings denied, and serial workspace tests pass. The combined SQL DML artifact has passed the confined executor and durable commit acceptance.
- Limits/next: DELETE staging is covered through the 128-target cap by the follow-up above. Adversarial live storage coverage remains open.

## SQL-4 sandboxed SQL DML sequence acceptance — verified

- Artifact: [`sql_v3_write_read.c`](../../engine/tests/fixtures/sql_v3_write_read.c) calls the INSERT, SELECT, UPDATE, SELECT, DELETE, and SELECT runners with the same v3 host callbacks. It checks NULL after insertion, checks the updated Integer value 42, and requires zero rows after deletion. The query emits markers only after the expected staged values are observed.
- Rust acceptance test: `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` provisions an owner-scoped nullable Integer table, stages the TinyCC artifact through authenticated submission, runs it in the confined executor, and expects the two provisional markers followed by a storage-confirmed COMMITTED terminal event.
- Tooling: `make -f engine/sdk/Makefile sandbox-query-fixture` now builds this artifact alongside the caught-error fixture. [`SANDBOX_TEST.md`](SANDBOX_TEST.md) documents its environment variable and opt-in command.
- Verification: the artifact builds with TinyCC. The ignored Rust acceptance passed using the temporary delegated cgroup and configured runtime allowlist; it verified own-write visibility across INSERT/UPDATE/DELETE, filtered metadata reads, stable v8 and composite v16 SQLR output, and a durable COMMITTED outcome after terminal delivery failed. The caught-error fixture separately passed, proving provisional output is followed by durable ABORTED status when C catches a SQL failure.
- Remaining: multi-row DML, broader permission/adversarial coverage, cancellation/deadline notification, and additional supported runtime profiles remain open.

## SQL-4 v2 schema column binding — implemented

- Contract: `prdb_sdk_sql_bind_select_v2` receives the v2 statement, one already-resolved authorized relation name, and its declared column shape. It maps unquoted projection and predicate names to zero-based ordinals using ASCII case folding, and attaches scalar/Decimal metadata to predicate leaves.
- Failure behavior: relation mismatch, missing/ambiguous columns, malformed statement records, and malformed type metadata fail without changing the output record. `SELECT *` remains a star marker with no explicit ordinal list. The mapping grants no storage access.
- Regression: the v2 fixture checks case-insensitive relation/column matching, projection and predicate ordinals, Decimal precision/scale, star binding, relation mismatch, missing/ambiguous columns, and invalid schema metadata. Before implementation it failed to link because the binder symbol was absent.
- Remaining: drive planner response decoding and full-shape executor batches from the SQL call path; the conversion/evaluation primitives do not yet submit or iterate IPC requests.

## SQL-2 typed scalar comparison and truth primitives — implemented

- Contract: comparisons require matching scalar type codes; Decimal additionally requires equal scales. SQL NULL produces UNKNOWN. REDACTED produces an error for comparison and `IS NULL`. Boolean, signed fixed-width scalars, exact decimals, valid UTF-8 text, bytea and UUID follow the ordering specified in [`sql-library.md`](../../specs/sql-library.md). The C truth helpers implement SQL three-valued truth tables; expression traversal and left-to-right short-circuit orchestration remain separate work.
- Implementation: the SQL ABI exposes checked comparison, `IS NULL`, Boolean-to-truth conversion and pure NOT/AND/OR helpers. Decimal scale is supplied by a later binder; this primitive rejects mismatched or out-of-range scale metadata.
- Regression: [`sdk_sql_expression_v1.c`](../../engine/tests/fixtures/sdk_sql_expression_v1.c) uses hand-evaluated integer, text and decimal examples, and checks NULL, REDACTED, type mismatch and truth-table behavior. The fixture first failed to compile because `prdb_sdk_sql_compare_v1` was missing and now passes against the TinyCC SDK archive.
- Parameter binding: `prdb_sdk_sql_bind_select_parameter_v1` connects the parsed one-based `$n` to a caller-owned value-view array and checks the selected value against the planner-resolved column type. It accepts NULL in binder context, rejects REDACTED, missing parameters and type mismatch, and returns a borrowed view. Decimal binding validates precision/scale metadata and the coefficient's precision range. The regression parses `$2` and binds it; before implementation TinyCC failed because the binder symbol was absent.
- Predicate evaluation: `prdb_sdk_sql_eval_select_predicate_v1` evaluates the parsed comparison or null-test operator against a sanitized row-cell view and a separately bound parameter. It checks resolved VALUE types, applies Decimal scale, returns UNKNOWN for SQL NULL comparisons and propagates REDACTED errors. The regression covers true, false, unknown, type mismatch, NULL tests and REDACTED rejection; before implementation the TinyCC fixture failed to link because the evaluator symbol was absent.
- Remaining: obtain relation shape from planner responses and execute SELECT over the scoped storage channel.

## SQL-5 stable projected-row encoding — implemented

- Contract: [`prdb_sdk_sql_result_v1.h`](../../engine/include/prdb_sdk_sql_result_v1.h) defines the bounded `SQLR` v1 record: magic, version, column count, and ordered declared column metadata plus typed VALUE/NULL/REDACTED bytes. `specs/sql-library.md` defines the big-endian framing, 1 MiB row limit, Decimal metadata, NULL/redaction handling, projection order, and fragment reassembly contract.
- Implementation: `prdb_sdk_sql_encode_result_row_v1` validates the SQL binding, declared relation shape, row layout and value types, then writes into caller-owned scratch/output buffers. `prdb_sdk_sql_write_result_row_v1` emits the record through v3 result callbacks in chunks of at most 16 KiB. These helpers preserve redaction and declared types for NULL/redacted cells, support `SELECT *` and duplicate/reordered projections, and expose no durable-result authority.
- Regression: [`sdk_sql_result_v1.c`](../../engine/tests/fixtures/sdk_sql_result_v1.c) first failed to link because the encoder symbol was absent; the follow-on writer assertion likewise failed to link before implementation. TinyCC regression checks exact hand-authored wire bytes, projection reordering and duplicates, star order, VALUE/NULL/REDACTED states, typed Decimal metadata, output bounds, invalid ordinals, relation/value type mismatch, result callback failure, and a second-chunk failure after one provisional chunk. The 17 KiB row test verifies fragmentation into host-sized frames, and the SELECT runner fixture checks exact SQLR bytes from its row callback.
- Verification: TinyCC SDK fixtures, including the SQLR encoder fixture, plus workspace build, format check, Clippy, and Cargo tests passed. The real sandboxed artifact test remains ignored until the host provides a delegated cgroup and the declared runtime profile.

## SQL-6 dependency: filtered `information_schema` executor scans — implemented

- Contract: [SQL library](../../specs/sql-library.md), read-only virtual metadata relations, and [metadata discovery](../../specs/metadata-discovery.md). The executor's ordinary `open_scan`/`next_batch` operations read `schemata`, `tables`, and `columns` through invocation-pinned authorization-filtered scanners.
- Regression: a real-redb transaction test pages all three relations through the executor batch API, verifies stable ordering, declared cell shapes and NULL fields, rejects cursor replay, and checks virtual handles cannot be updated or inserted into.
- Regression: `sdk_sql_information_schema_v2.c` consumes a parameterized schemata query over sanitized rows, checks SQL predicate and LIMIT behavior, and verifies cursor closure. `information_schema_virtual_scans_round_trip_through_executor_ipc` independently checks the real Rust storage/IPC row stream for schemata, tables, and columns.
- Verification: the maintained TinyCC suite, workspace build, format, warning-denied Clippy, and workspace tests passed. The ignored `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance also passed on the writable delegated-cgroup parent, executing the artifact through the confined worker.
- End-to-end behavior: `sql_v3_write_read.c` uses parameterized schema filters to query `schemata`, `tables`, and `columns` during the sequential DML invocation and validates the expected public metadata names. The sandbox fixture seeds the matching schema/table/column grants and allow policies, so authorization is exercised through the production path.
- SDK correction: `prdb_sdk_executor_close_cursor_v1` now validates the server's version-plus-u8 status acknowledgment, matching the executor IPC response and mutation acknowledgments. The sandbox LIMIT queries cover live cursor closure.
- API parity: the confined SQL artifact emits selected values for all three virtual relations. The sandbox acceptance test compares public metadata and granted user-schema/table/column results byte-for-byte with the trusted supervisor API over the same invocation-owned snapshot. It verifies zero rows for an ungranted table and a nonexistent table through both `tables` and `columns`. An authorized empty schema appears through both APIs while its `tables` and `columns` queries return no rows. A hidden schema with a real table and a nonexistent schema both yield zero rows through SQL `schemata`, `tables`, and `columns`; the same invocation's metadata API omits the hidden schema and its objects.
- Remaining: broaden SQL execution grammar and workload coverage. Confined SQL acceptance covers broad and restricted principals, and authenticated SQL metadata filtering now crosses the TCP gateway and supervisor processes.

## SQL-4 / SQL-6: confined SQL DML and failure acceptance — verified

- Regression: the opt-in `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` test passed through the delegated `user@1000.service` cgroup. It ran the maintained TinyCC artifact through authenticated staging and the confined executor, verified sequential INSERT/SELECT/UPDATE/SELECT/DELETE/SELECT behavior, metadata filtering for broad and restricted principals, durable COMMITTED status, and lost-terminal recovery.
- Regression: `sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output` passed through the same executor path and verified a caught statement failure produces durable ABORTED status after provisional output.
- Tooling: built artifacts with `nix-shell -p rustc cargo clippy rustfmt tinycc --run 'make -f engine/sdk/Makefile sandbox-query-fixture'`. Ran both ignored tests with their documented `-- --ignored --exact --nocapture` commands from [SANDBOX_TEST.md](SANDBOX_TEST.md), using the current NixOS glibc 2.42 loader/libc and GCC 15.3 `libgcc_s`; `PRDB_TEST_RUNTIME_READ_ONLY` was empty.
- Remaining: broaden SQL execution grammar and workload coverage; the sandbox acceptance covers only the maintained bounded fixtures and this Linux x86_64 runtime profile.

## SQL-6: restricted SQL metadata over authenticated TCP — verified

- Contract: a restricted principal's SQL scans of `information_schema` must return only schemas, tables, and columns visible through its admitted capabilities, including when submission crosses the external gateway and supervisor process boundary.
- Regression: the optional `PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT` path in `authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes` packages the maintained restricted-metadata artifact, authenticates with the restricted token, uploads and seals it through `SubmissionClient`, starts it over TCP, and checks the only result is `empty_roundtrip`, followed by a COMMITTED terminal and STATUS result. The artifact also queries hidden `sql_roundtrip` schema/table/column entries and fails if any are returned.
- Red/green evidence: the first run returned no visible schema because the fixture lacked a schema USAGE grant. Adding an explicit typed USAGE grant made the authenticated TCP acceptance pass.
- Verification: the process test passes with the optional branch disabled and enabled. With `PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT`, delegated `PRDB_TEST_CGROUP_PARENT`, and the NixOS loader/libc/`libgcc_s` allowlist configured, `cargo test -p prdb-engine --test metadata_process --locked authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes -- --exact --nocapture` passed. Workspace build, format check, warning-denied Clippy, and full tests passed; commit integration reported 125 passed and 3 cgroup-dependent tests ignored.
- Remaining: add broader SQL forms and workload cases; this network acceptance uses the bounded restricted-metadata fixture on Linux x86_64.

## SQL-7: stable bounded single-key sort and SQL ORDER BY — partial

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). Sort one already-authorized declared scalar key across at most 128 rows; NULL sorts first in both directions, ties preserve input order, and REDACTED or invalid values fail without a partial permutation.
- Implementation: `prdb_sdk_sql_order_row_indices_v1` uses a bounded stable insertion sort and returns caller row indexes without allocation. It validates every key before publishing the output order and delegates scalar ordering to the shared typed comparator.
- Red/green evidence: the maintained TinyCC fixture first failed because the order header/target did not exist. It now covers ascending/descending order, NULL placement, stable ties, type mismatch, redaction, output preservation on failures, caller capacity, the 128-row bound, and the empty input.
- Verification: `nix-shell -p rustc cargo clippy rustfmt tinycc --run 'make -f engine/sdk/Makefile test'` passed with TinyCC 0.9.27-unstable-2025-01-06.
- Parser/binder: [`prdb_sdk_sql_v3.h`](../../engine/include/prdb_sdk_sql_v3.h) embeds the frozen v2 SELECT record and adds one `ORDER BY` source-column span and direction. The parser accepts optional ASC/DESC (ASC default) and LIMIT after the order clause; it rejects multiple keys, expressions, LIMIT before ORDER BY, and trailing statements. The binder resolves the key against the complete relation shape, including an unprojected key, and returns its ordinal and type metadata. `sdk_sql_select_v3.c` covers parsing, defaults, binding, missing key, malformed AST, and output preservation on failure. Its first build linked unsuccessfully because the v3 API symbols were missing; it passes with the implementation.
- Execution: [`prdb_sdk_sql_exec_v3.h`](../../engine/include/prdb_sdk_sql_exec_v3.h) adds `prdb_sdk_sql_execute_select_v3`. Ordered scans materialize matching authorized row encodings into caller scratch capped at 1 MiB and 128 rows, close the cursor, extract bound typed keys, apply the stable permutation, and invoke the existing row callback in sorted order before applying LIMIT. Exceeding either bound returns `MATERIALIZATION_LIMIT` before any sorted callback. Queries without ORDER BY retain v2 streaming behavior.
- Regression: `sdk_sql_execute_v2.c` also covers descending order across a continued scan, full sorted output, sort-before-LIMIT, and materialization overflow with no partial output. The focused TinyCC fixture passes.
- Verification: the complete TinyCC SDK suite, locked Cargo workspace build, formatting check, and warning-denied Clippy pass. The full Cargo test run had 124 passing and 3 cgroup-dependent ignored tests, with `storage_manager_creates_and_drops_check_constraint` returning `InvalidAudit`; that test passed alone, and the commit integration test binary passed serially with 125 passed and 3 ignored. The failure is outside the SQL SDK change and occurs under parallel test execution.
- Remaining: bounded execution uses one scalar key and caller-provided in-memory scratch; spill, multiple keys, cancellation, and broader analytical operators remain deferred. SQL-7's single-key bounded sort path is implemented; continue with the next workload-plan capability.

## SQL-7 dependency: bounded single-key GROUP BY COUNT kernel — partial

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). Group at most 128 already-authorized values for one declared scalar key; equal values share a group, NULL keys form one group, output groups preserve first-seen order, and `COUNT(*)` includes every row in the group. REDACTED, malformed, and mismatched values fail without publishing partial output.
- Implementation: `prdb_sdk_sql_group_count_v1` compares typed values through the shared SQL comparator and returns representative input-row indexes with per-group counts. It allocates no heap memory and writes outputs only after validation and capacity checks.
- Red/green evidence: `sdk_sql_group_v1.c` first failed to link because the kernel symbol was absent. It now covers stable grouping, repeated and NULL keys, REDACTED/type errors, insufficient output capacity, the 128-row bound, empty input, and output preservation on failure.
- Verification: the complete TinyCC SDK suite passed. Locked workspace build, format check, warning-denied Clippy, and full workspace tests passed; 3 cgroup-dependent integration tests remain ignored.
- Remaining: the kernel computes group membership/counts but does not yet produce SQL rows; parser/binder and grouped execution integration are tracked below. SUM/MIN/MAX/AVG and multiple group keys are also open.

## SQL-7 dependency: grouped SELECT COUNT(*) parser and binder — partial

- Contract: [SQL library](../../specs/sql-library.md). Accept `SELECT key, COUNT(*) FROM relation [WHERE predicate] GROUP BY key [LIMIT n]`, with one group key repeated as the only non-aggregate projection. Name matching is case-insensitive; output AST/binding records are separately versioned and preserve v1-v3 layouts.
- Red/green evidence: the v4 fixture first failed to link because its parser and binder entry points were absent. It now checks predicate and LIMIT spans, case-insensitive grouping-key agreement, bound ordinal/type metadata, rejection of non-star aggregates, missing/mismatched/multiple group keys, misplaced LIMIT, missing key projection, trailing statements, and output preservation on parse/bind failure.
- Verification: the maintained TinyCC SDK suite, locked workspace build, format check, warning-denied Clippy, and full workspace tests passed; the commit integration suite reported 125 passed and 3 cgroup-dependent ignored.
- Remaining: the runner does not yet scan, aggregate, or encode grouped output rows. HAVING, ORDER BY on grouped output, multiple group keys, and SUM/MIN/MAX/AVG remain unsupported.

## SQL-7 dependency: bounded grouped SELECT COUNT(*) runner and SQLR output — partial

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). Evaluate `WHERE` over authorized scan rows, retain at most 128 matching group keys and 1 MiB of copied key bytes, close the scan, then emit first-seen `COUNT(*)` groups with `LIMIT` applied after aggregation. Scan, predicate, key materialization, or grouping failures produce no group callbacks.
- Red/green evidence: the executor fixture first failed to link because `prdb_sdk_sql_execute_group_count_v4` was not implemented. The maintained fixture now checks stable group order, duplicate-key counts, `WHERE` filtering, post-group LIMIT, callback timing after the terminal scan page, and key-byte overflow with no partial callbacks.
- Implementation: the v4 runner uses the v4 parser/binder and existing stable grouping kernel, validates planner and executor relation shapes, type-checks predicate parameters, copies retained values into caller-owned bounded storage, and closes an unfinished scan on failure. Its corrected 126-page cap accounts for one cancellation check and one batch request per page, plus resolve/open and possible cursor-close requests, within the executor connection's 256-request budget. `prdb_sdk_sql_encode_group_count_result_v1` encodes the key plus `count` BIGINT using SQLR v1; its writer uses the existing bounded provisional result callback.
- Result evidence: `sdk_sql_result_v1.c` first failed to link because the grouped SQLR encoder/writer were missing. It now checks exact wire bytes, positive-count validation, NULL key encoding, insufficient capacity with zero output length, successful callback output, 17 KiB record fragmentation, and failure on the second chunk after one provisional chunk. The executor fixture composes runner groups through the encoder and checks the encoded counts.
- Verification: the complete maintained TinyCC SDK suite passed. Locked workspace build, formatting check, warning-denied Clippy, and full locked workspace tests passed; the commit integration suite reported 125 passed and 3 cgroup-dependent ignored.
- Remaining: live sandbox/result-relay integration for grouped output, cancellation/deadlines, spill, HAVING, grouped ORDER BY, multiple keys, and SUM/MIN/MAX/AVG remain open.

## SQL-7 dependency: bounded MIN/MAX aggregate kernel — partial

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). Compute MIN or MAX over at most 128 authorized values of one declared scalar type. Ignore NULLs, return a canonical NULL view for empty/all-NULL input, compare typed values through the shared exact comparator, and reject any REDACTED/malformed/mismatched operand without modifying the output.
- Red/green evidence: the maintained grouping fixture failed to link because `prdb_sdk_sql_minmax_v1` was not implemented. It now checks MIN/MAX values, NULL behavior, redacted and type mismatch failures with output preservation, undersized output rejection, and exact Decimal ordering at a declared scale.
- Implementation: the no-allocation kernel retains a borrowed view of the first extremum and uses the existing scalar comparator; equal values preserve the first representation. It is an operator primitive, not yet exposed by SQL aggregate parsing or grouped execution.
- Verification: the complete maintained TinyCC suite passed. Locked workspace build, format check, warning-denied Clippy, and full locked workspace tests passed; the commit integration suite reported 125 passed and 3 cgroup-dependent ignored.
- Remaining: SQL aggregate grammar/execution/result encoding, SUM/AVG overflow and result type rules, and live analytical integration remain open.

## SQL-7 dependency: grouped MIN/MAX parser and binder — partial

- Contract: [SQL library](../../specs/sql-library.md). Preserve v1-v4 AST/binding layouts and accept the existing one-key grouped form with `COUNT(*)`, `MIN(column)`, or `MAX(column)`. Bind the aggregate argument to the same authorized relation shape, including Decimal metadata; reject unsupported aggregates and malformed/trailing syntax.
- Red/green evidence: the new maintained v5 fixture first failed to link because `prdb_sdk_sql_parse_select_v5` and `prdb_sdk_sql_bind_select_v5` were missing. It now checks MIN/MAX/COUNT ASTs, WHERE and LIMIT spans, Decimal aggregate binding, missing columns, unsupported SUM/argument forms, grouping-key mismatch, misplaced clauses, trailing syntax, and output preservation on parse/bind errors.
- Implementation: v5 embeds the frozen v2 SELECT record and separately records the group key, aggregate kind, and optional aggregate-column span/binding. It is parser/binder support only; the v4 COUNT runner remains unchanged.
- Verification: the complete maintained TinyCC suite, locked workspace build, format check, and warning-denied Clippy passed. `cargo test --workspace --locked -- --test-threads=1` passed across the workspace; the commit integration suite reported 125 passed and 3 cgroup-dependent ignored. The default parallel test run hit the known `storage_manager_creates_and_drops_check_constraint` `InvalidAudit` interaction; that test passed alone and the commit suite passed serially.
- Remaining: bounded per-group MIN/MAX execution and SQLR aggregate output, plus SUM/AVG typing and overflow rules, remain open.

## SQL-7 dependency: bounded grouped MIN/MAX runner — partial

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). Execute the v5 one-key grouped `MIN(column)`/`MAX(column)` forms over authorized rows, with WHERE before grouping, stable first-seen groups, a 128 matching-row cap, and a combined 1 MiB copied key/aggregate-value cap.
- Implementation: `prdb_sdk_sql_execute_group_v5` validates planner/executor shape agreement, binds predicate parameters and the aggregate column, copies retained values into caller storage, closes an unfinished scan, groups keys with the existing kernel, and computes each MIN/MAX through `prdb_sdk_sql_minmax_v1`. All aggregate groups validate before callbacks; `LIMIT` is applied afterward. The callback receives the group key, typed aggregate view (canonical NULL for all-NULL groups), matching-row count, and v5 binding. COUNT(*) is also accepted by this runner and passes a NULL aggregate-value pointer.
- Regression: `sdk_sql_execute_v5.c` exercises MIN, MAX, WHERE, post-group LIMIT, all-NULL groups, redacted aggregate rejection without callbacks, cursor close on materialization overflow, and stable group counts. The focused TinyCC fixture passed.
- Remaining: SQLR output encoding/writing for MIN/MAX, column labels, SUM/AVG typing and overflow, and live sandbox integration remain open.

## SQL-7 dependency: grouped MIN/MAX SQLR output — partial

- Contract: [SQL library](../../specs/sql-library.md). Encode the group key using its declared metadata and a second aggregate field named `min` or `max` using the bound argument's declared type and Decimal metadata. Preserve VALUE/NULL group-key state and canonical NULL aggregate state; reject REDACTED or mismatched aggregate values.
- Red/green evidence: the result fixture first failed to link because the grouped MIN/MAX encoder and writer symbols were missing. It now checks exact hand-authored MIN wire bytes, the MAX label/value path, typed Decimal metadata for NULL, redacted aggregate rejection, successful callback output, and callback failure handling.
- Implementation: `prdb_sdk_sql_encode_group_minmax_result_v1` writes a standalone SQLR v1 record; its writer emits host-sized chunks through the v3 provisional result callback.
- Sandbox integration: `sql_v3_write_read.c` now executes `SELECT id, MIN(id) ... GROUP BY id` over the row it just updated and writes the SQLR record from the grouped callback. `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` checks the exact relayed SQLR bytes; the ignored acceptance passed through the confined executor, result relay, and durable commit using the documented NixOS runtime profile and a user-owned delegated cgroup with `cpu`, `memory`, and `pids` enabled.
- Remaining: SUM/AVG typing and overflow rules, cancellation/deadlines, spill, HAVING, grouped ORDER BY, and multiple grouping keys remain unsupported.

## SQL-7 dependency: bounded SUM aggregate kernel — partial

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). Accept at most 128 SMALLINT, INTEGER, BIGINT, or DECIMAL values of one declared type. Widen integer results to BIGINT and DECIMAL(p,s) to DECIMAL(38,s); ignore NULLs, return canonical NULL for empty/all-NULL input, and fail on REDACTED, malformed, mismatched, or numerically overflowing input.
- Red/green evidence: the maintained TinyCC grouping fixture now checks BIGINT summation with NULLs, BIGINT final overflow, intermediate cancellation, Decimal coefficient summation and Decimal result overflow, canonical empty input, and unchanged output on overflow.
- Implementation: `prdb_sdk_sql_sum_v1` uses caller-owned output bytes and fixed-size accumulators sized for the 128-row cap. It validates Decimal input coefficients against declared precision and reports overflow only when the final exact sum exceeds the result type range. Failure leaves output view and bytes unchanged.
- Verification: the focused TinyCC grouping fixture was force-rebuilt and passed with `-Wall -Wextra -Werror`.
- Remaining: mixed-workload benchmarks, joins, HAVING, grouped ORDER BY, multiple grouping keys, cancellation/deadlines, and spill remain open.

## SQL-7 dependency: grouped SUM parser and binder — partial

- Contract: [SQL library](../../specs/sql-library.md). Preserve v1-v5 AST/binding layouts and add a separately versioned grouped SELECT form for `SUM(column)`. Bind only SMALLINT, INTEGER, BIGINT, and DECIMAL arguments, carrying Decimal precision/scale from the authorized relation shape.
- Red/green evidence: the maintained v6 fixture checks SUM parsing with WHERE and LIMIT, numeric Decimal binding, rejection of nonnumeric arguments without modifying the output, and confirms the frozen v5 parser still rejects SUM.
- Implementation: the v6 AST and binding have dedicated version tags and aggregate kind. The shared parser grammar accepts SUM only through the v6 entry point; v5 behavior remains unchanged. Binding reuses v5 group-key and column resolution, then enforces the numeric type set.
- Verification: the focused TinyCC parser/binder fixture was force-rebuilt and passed with `-Wall -Wextra -Werror`.
- Remaining: mixed-workload benchmarks, joins, HAVING, grouped ORDER BY, multiple grouping keys, cancellation/deadlines, and spill remain open.

## SQL-7 dependency: grouped SUM runner and SQLR output — partial

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). Execute v6 grouped `SUM(column)` with WHERE before grouping, at most 128 matching authorized rows, and a combined 1 MiB cap for copied keys and operands. Validate all aggregates before callbacks. Encode the aggregate as a `sum` SQLR field using BIGINT for integer inputs or DECIMAL(38,s) for Decimal inputs.
- Implementation: `prdb_sdk_sql_execute_group_v6` validates planner/executor shape agreement, binds predicates and the numeric aggregate, materializes authorized rows into caller memory, closes the scan, groups stably, and computes each SUM with `prdb_sdk_sql_sum_v1`. It retains result bytes in a bounded stack array until callbacks finish. The SQLR v1 encoder/writer emits the widened result metadata and host-sized provisional chunks.
- Regression: the maintained v6 executor fixture checks grouped integer SUM values and group counts; the SQLR fixture checks the `sum` label, BIGINT metadata/value, Decimal(38, scale) NULL metadata, redaction rejection, and writer callback output. The focused TinyCC fixtures passed.
- Sandbox integration: the maintained `sql_v3_write_read.c` artifact now runs grouped `MIN(id)` and grouped `SUM(id)` after the update. `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` verifies the exact SUM SQLR record across the confined executor and result relay before durable commit.
- Remaining: mixed-workload benchmarks, joins, HAVING, grouped ORDER BY, multiple grouping keys, cancellation/deadlines, and spill remain open.

## SQL-7 dependency: bounded AVG aggregate kernel — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). Accept up to 128 authorized SMALLINT, INTEGER, BIGINT, or DECIMAL values. Return DECIMAL(38,r), with integer `r=6` and Decimal `r=min(max(6,s+6),38-(p-s))`; round the exact rational average to nearest, ties to even. NULLs are ignored, empty/all-NULL returns canonical NULL, and REDACTED/malformed/mismatched values fail.
- Red/green evidence: the maintained TinyCC group fixture checks integer average, Decimal scale conversion, both even and odd half-tie cases at the 128-row boundary, all-NULL output, and output/scale preservation when REDACTED input fails.
- Implementation: `prdb_sdk_sql_avg_v1` uses a fixed 20-byte unsigned scaled-division workspace, validates Decimal input precision, rounds ties by quotient parity, and writes result coefficient and scale only after validation and range checks.
- Verification: the focused TinyCC grouping fixture was force-rebuilt and passed with `-Wall -Wextra -Werror`.
- Remaining: integration coverage for mixed workloads, joins, HAVING, grouped ORDER BY, multiple grouping keys, cancellation/deadlines, and spill remain open.

## SQL-7 dependency: grouped AVG parser and binder — implemented

- Contract: [SQL library](../../specs/sql-library.md). Preserve v1-v6 AST/binding layouts and add a separately versioned grouped SELECT form for `AVG(column)`. Bind only SMALLINT, INTEGER, BIGINT, and DECIMAL arguments, retaining Decimal metadata.
- Red/green evidence: the maintained v7 fixture checks AVG parsing with WHERE and LIMIT, Decimal argument binding, nonnumeric argument rejection with output preservation, and confirms the frozen v6 parser still rejects AVG.
- Implementation: the v7 AST/binding have dedicated version tags and aggregate kind. The parser shares the grouped grammar while only exposing AVG through the v7 entry point; binding reuses v6 numeric aggregate validation and preserves the AVG kind.
- Verification: the focused TinyCC parser/binder fixture was force-rebuilt and passed with `-Wall -Wextra -Werror`.
- Remaining: mixed-workload benchmarks, joins, HAVING, grouped ORDER BY, multiple grouping keys, cancellation/deadlines, and spill remain open.

## SQL-7 dependency: grouped AVG runner and SQLR output — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). Reuse the v6 128-row and 1 MiB copied-value bounds; filter before grouping; validate all groups before callbacks; expose DECIMAL(38,r) AVG results and SQLR metadata.
- Implementation: `prdb_sdk_sql_execute_group_v7` runs the v7 AVG binding, persists fixed-width average result bytes through callbacks, and uses the bounded AVG kernel to compute each group. The SQLR v1 AVG encoder/writer emits the `avg` label and result precision/scale.
- Regression: TinyCC v7 execution checks integer results, null groups and error behavior; SQLR fixtures check AVG metadata and writer output. The maintained confined query artifact now runs MIN, SUM, and AVG. `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` checks the exact DECIMAL(38,6) AVG frame after sandbox execution and relay, followed by durable commit. This acceptance passed with the local delegated cgroup/runtime profile.
- Verification: full maintained TinyCC SDK suite, focused sandbox acceptance, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked -- --test-threads=1`, and `git diff --check` passed. Three cgroup-dependent integration tests remain ignored in the full test run; the AVG acceptance was run separately with delegated cgroup and runtime paths.
- Remaining: mixed-workload benchmarks, joins, HAVING, grouped ORDER BY, multiple grouping keys, cancellation/deadlines, and spill remain open.

## SQL-7 dependency: bounded inner equijoin pair kernel — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). Match two authorized scalar-key arrays with inner equijoin semantics, bounded to 128 rows per side and at most 16,384 output pairs. NULL does not match; REDACTED or malformed values fail; errors leave caller output unchanged.
- Red/green evidence: the maintained TinyCC fixture first failed to link on the absent kernel. It now checks stable left-major pair order, duplicate keys, NULL behavior, Decimal keys at the same declared scale, rejected scale/type mismatches, malformed and REDACTED inputs, insufficient output capacity, and the maximum 128×128 result.
- Implementation: `prdb_sdk_sql_inner_join_pairs_v1` validates inputs and preflights comparison results and exact output size before writing the pair array. It reuses the existing scalar comparator, so types must agree and Decimal scales must match.
- Verification: focused TinyCC fixture passed with `-Wall -Wextra -Werror`.
- Remaining: SQL JOIN SQLR encoding and confined end-to-end acceptance remain open. The operator primitive alone did not claim full SQL JOIN support; the later v8 parser/binder and runner are recorded below.

## SQL-8 dependency: bounded composite inner equijoin pair kernel — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). Match row-major authorized tuples across two to eight declared key positions, with SQL equality semantics, stable left-major output, 128 rows per side, and at most 16,384 pairs. Any NULL component makes the pair non-matching; every supplied cell is still validated, so REDACTED or malformed keys fail.
- Red/green evidence: the maintained TinyCC fixture exercises two integer keys, mixed INTEGER/TEXT keys, Decimal scale metadata, NULL components, an eight-key tuple, stable duplicate ordering, type/redaction failures, insufficient capacity, row-count bounds, and output preservation.
- Implementation: `prdb_sdk_sql_inner_join_pairs_composite_v2` is a new operator ABI; v1 remains unchanged. It validates shapes and every cell, preflights tuple comparisons and capacity, then publishes the stable pair array and count.
- Verification: the focused fixture and complete maintained TinyCC SDK suite passed with TinyCC `-Wall -Wextra -Werror` after the new object was added to the maintained SDK archive/test target. Rust 1.95.0 workspace build, rustfmt check, warning-denied Clippy, and serial locked workspace tests also passed (127 commit tests passed, 3 cgroup-dependent cases ignored, all other workspace tests passed); `git diff --check` passed.
- Remaining: parser/binder, two-scan runner, SQLR projection path, and confined end-to-end coverage for multiple join predicates remain open; aliases, self-joins, WHERE, outer joins, cancellation/deadlines, and spill also remain unsupported.

## SQL-8 dependency: multi-predicate INNER JOIN parser and binder — implemented

- Contract: [SQL library](../../specs/sql-library.md). V16 accepts two to eight qualified equality predicates joined by AND, preserves the two separately resolved relation shapes and qualified projections from v8, and requires each left/right pair to bind to its respective side with matching scalar type and Decimal scale.
- Red/green evidence: the maintained v16 fixture covers two-predicate parsing/binding, LIMIT, the eight-key maximum, ninth-key and malformed-clause rejection, wrong relation/type failures, output preservation, and rejection by frozen v8/v15 parsers.
- Implementation: `prdb_sdk_sql_v16.h` defines sized statement/binding records embedding the frozen v8 records. The parser stores source spans for each equality pair; binding reuses v8 projection/relation checks and resolves every pair against the independent relation shapes. V1-v15 records and parsing remain unchanged.
- Verification: the focused v16 fixture and complete maintained TinyCC SDK suite passed with `-Wall -Wextra -Werror`; `git diff --check` passed. The locked Rust workspace build, formatting, Clippy, and serial test suite passed immediately before this C-only parser/binder increment.
- Remaining: connect v16 binding to the composite join kernel in a bounded two-scan runner, add composite SQLR output, and prove the path through confined acceptance. Aliases, self-joins, WHERE, outer joins, cancellation/deadlines, and spill remain unsupported.

## SQL-7 dependency: single INNER JOIN parser and binder — implemented

- Contract: [SQL library](../../specs/sql-library.md). Parse one `INNER JOIN` between two named relations, qualified projected columns, one qualified equality predicate, and optional LIMIT. Bind each reference against its own independently supplied authorized relation shape, and require compatible join-key metadata.
- Red/green evidence: the maintained v8 TinyCC fixture parses the two-relation join form and verifies projection sides/ordinals and key bindings. It rejects malformed and unqualified syntax, preserves outputs on failed parse/bind, rejects relation and type mismatches, and confirms the frozen v7 parser does not accept the new grammar.
- Implementation: v8 uses a separately versioned borrowed-span AST and binding. Relation aliases and self-joins are not accepted; one key equality is supported, with Decimal scale agreement matching the shared comparator.
- Verification: focused TinyCC parser/binder fixture passed with `-Wall -Wextra -Werror`.
- Remaining: encode joined rows as SQLR and verify the planner/executor path through the confined sandbox. WHERE, outer joins, multiple join predicates, aliases, and self-joins remain open.

## SQL-7 dependency: bounded two-scan INNER JOIN runner and SQLR — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). Resolve two relations independently through planner and executor APIs, verify both relation shapes, scan sequentially, close before result callbacks, and cap each side at 128 rows plus 1 MiB total copied join-key/projection bytes. Apply inner equijoin and LIMIT after both scans; latch statement failure on errors.
- Implementation: `prdb_sdk_sql_execute_inner_join_v8` uses separate planner/executor response buffers and shapes for each relation, stores only join keys and projected values in caller-owned bounded scratch, invokes the join kernel after both scans close, and dispatches synchronous projected-row callbacks.
- Regression: the maintained v8 executor fixture checks two relation resolutions, two finished scans, stable joined projection values, NULL non-matches, LIMIT, redacted join-key rejection, copy-budget exhaustion, and callback failure propagation. The focused TinyCC fixture passed.
- SQLR result path: `prdb_sdk_sql_encode_join_result_v1` and `prdb_sdk_sql_write_join_result_v1` use the v8 binding's source side and schema ordinal to validate and encode projected values with the declared source name/type metadata. The maintained result fixture checks exact bytes for a VALUE and NULL from opposite sides, successful provisional host output, invalid-side rejection, and value/type mismatch with zero output length.
- Verification: the focused SQLR fixture and complete maintained TinyCC SDK suite passed. Locked Rust workspace build, rustfmt check, warning-denied Clippy, and serial workspace tests passed; the commit integration binary reported 128 passed and 3 cgroup-dependent ignored tests.
- Confined acceptance: `sql_v3_write_read.c` now runs `records INNER JOIN linked_records` after updating `records.id` to 42. `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` creates and seeds the second table, grants the query principal SELECT on its table and column, includes both relations in the requested scope, executes the v8 runner through planner/executor IPC, and checks the exact two-field SQLR frame after result relay. The test passed through the confined executor and durable commit path.
- Remaining: aliases, self-joins, WHERE, outer joins, multiple join predicates, cancellation/deadlines, and spill remain unsupported; broader mixed-workload and adversarial acceptance remains future work.

## SQL-7 dependency: bounded grouped HAVING — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). The v9 grammar accepts one comparison between the selected COUNT(*), MIN, MAX, SUM, or AVG expression and a positional parameter. The parameter must exactly match the derived aggregate type; NULL comparison results exclude a group. HAVING runs after aggregation and before LIMIT, preserving first-seen order.
- Red/green evidence: the v9 parser/binder fixture first failed to link because the v9 entry points were missing. It now covers HAVING parse/bind metadata, COUNT BIGINT, Decimal SUM/AVG result types, and malformed/mismatched aggregate syntax, parameter position, and operator rejection. The v9 executor fixture covers post-HAVING LIMIT, NULL filtering, wrong-type and REDACTED parameter failures before scanning, and callback suppression on failure.
- Implementation: the v9 grouped runner shares the bounded v7 scan/aggregate path. It validates the HAVING parameter before opening a scan, computes all group comparisons before the first callback, encodes COUNT as canonical BIGINT bytes for comparison, and counts LIMIT over qualifying groups. v7 remains on its original parser/binding behavior.
- Verification: focused v9 parser/binder and grouped execution fixtures passed. The complete maintained TinyCC SDK suite passed. In Rust 1.95.0, locked workspace build, formatting, warning-denied Clippy, and serial workspace tests passed; 4 cgroup-dependent integration tests remain ignored.
- Remaining: arbitrary Boolean HAVING expressions, literal operands, HAVING-only projections, grouped ORDER BY, multiple group keys, cancellation/deadlines, spill, and mixed-workload measurements remain open.

## SQL-7 dependency: bounded grouped ORDER BY — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). The v10 grammar orders by one selected group key or the exact selected aggregate, with optional ASC/DESC. HAVING filters first, NULL sorts first in either direction, ties remain first-seen, and LIMIT applies after sorting.
- Red/green evidence: the v10 parser/binder fixture first failed to link on the absent v10 entry points. It now covers key and aggregate expressions, COUNT/SUM/AVG metadata, default and explicit directions, HAVING plus LIMIT placement, mismatched/unselected expressions, malformed direction, output preservation, and frozen v9 rejection. The execution fixture covers key DESC, aggregate DESC, HAVING then aggregate ASC and LIMIT, stable bounded group output, and callback values.
- Implementation: v10 derives the sort type from the key or aggregate result and reuses the stable 128-entry order kernel after all aggregates and HAVING comparisons have been validated. It stores only a bounded group permutation; callbacks run after scan closure and sort-key validation.
- Confined acceptance: `sql_v3_write_read.c` now runs `SELECT id, COUNT(*) ... ORDER BY COUNT(*) DESC LIMIT 1` after its update and writes the COUNT SQLR record through the host result callback. `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` verifies the exact frame through the confined executor and gateway relay before durable commit. The same fixture's metadata result assertion was made order-independent after the added second visible relation exposed differing table iteration order between metadata APIs and SQL scans.
- Verification: focused v10 parser/binder and grouped execution TinyCC fixtures passed. The confined acceptance passed on the delegated user cgroup/runtime profile. The full maintained TinyCC suite passed. In Rust 1.95.0, locked workspace build, format check, warning-denied Clippy, and serial workspace tests passed; 4 cgroup-dependent cases remain ignored in the general suite, including the SQL acceptance that was run separately.
- Remaining: arbitrary expressions, multiple group keys, cancellation/deadlines, spill, non-COUNT v10 SQLR composition, and mixed-workload measurements remain open.

## SQL-7 dependency: explicit NULL placement for grouped ordering — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). The separately versioned v11 grammar accepts `NULLS FIRST` or `NULLS LAST` after optional ASC/DESC; omitted placement retains NULLS FIRST, independent of value direction.
- Red/green evidence: the maintained v11 parser fixture initially failed to link because v11 parse/bind entry points were absent. It now checks ASC/DESC metadata, explicit and default NULL placement, LIMIT placement, malformed placement, output preservation, and v10 rejection. The maintained grouped executor fixture covers NULL aggregate results sorted last with ASC and default NULLS FIRST with DESC; the ordering kernel fixture checks stable NULLS LAST ordering in both directions.
- Implementation: v11 adds sized AST/binding records and an execution entry point while preserving v10 behavior. The shared bounded runner dispatches its validated group sort keys to the new explicit-null-placement kernel; NULL handling is independent of ASC/DESC and callbacks still run after scan closure and validation.
- Confined acceptance: the maintained `sql_v3_write_read.c` artifact now invokes v12 with two grouped order expressions; the ignored `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed through the delegated user cgroup and checked the v12 binding and relayed SQLR record before durable COMMITTED.
- Verification: focused v11 parser/binder, grouped executor, and order-kernel fixtures passed; the full maintained TinyCC SDK suite passed. Rust 1.95.0 locked workspace build, format check, warning-denied Clippy, and workspace tests passed; three cgroup-dependent tests remain ignored by the general suite.
- Remaining: multiple group keys, arbitrary expressions, cancellation/deadlines, spill, broader v11/v12 SQLR composition, and mixed-workload measurements remain open.

## SQL-7 dependency: bounded grouped multi-key ordering — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). V12 retains optional ORDER BY and accepts the selected group key and selected aggregate as a lexicographic list of one or two distinct expressions when present. Each item has its own direction and NULL placement; omitted direction is ASC and omitted NULL placement is NULLS FIRST.
- Red/green evidence: the v12 parser/binder fixture first failed to link on missing v12 entry points. It now covers both item kinds, metadata binding, independent directions/null placement, LIMIT placement, duplicates, third-key rejection, and malformed list syntax. The composite sorter fixture checks two-key precedence, stable output, NULL placement, redacted input rejection, unchanged output on error, and zero-row input. The grouped executor fixture uses equal primary COUNT values to verify that the group key resolves ties as the second key.
- Implementation: new sized v12 statement/binding/runner records retain v11 behavior. The parser caps the list at two because the grouped projection exposes only one group key and one selected aggregate. The sorter validates all values before building a stable permutation; the runner applies HAVING first, closes the scan, sorts, then applies LIMIT and calls back.
- Confined acceptance: the maintained SQL artifact now uses `ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS LAST`; `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` passed through the delegated cgroup and verified the two v12 binding items, relayed SQLR output, and durable COMMITTED.
- Verification: full maintained TinyCC SDK suite and focused confined SQL acceptance passed. Rust 1.95.0 workspace build, format, warning-denied Clippy, and full tests passed; no Rust source changed in this increment.
- Remaining: more than two grouped sort keys are unsupported because only two selected grouped outputs currently exist; multiple group keys, arbitrary expressions, cancellation/deadlines, spill, broader SQLR composition, and mixed-workload measurements remain open.

## SQL-7 dependency: bounded composite grouping kernel — implemented

- Contract: [SQL library](../../specs/sql-library.md), Operational and Analytical Execution. Group one to eight already-authorized scalar key columns over at most 128 rows, treating NULL components as equal, preserving first-seen group order, and leaving outputs untouched on invalid/redacted input or insufficient capacity.
- Red evidence: the maintained TinyCC grouping fixture failed to link because `prdb_sdk_sql_group_count_composite_v1` was absent.
- Implementation: the SDK now exposes a row-major composite-key count kernel with per-column type and Decimal-scale validation. It validates all cells before building groups, compares each tuple using the shared SQL comparator, and publishes groups only after capacity validation.
- Regression: the fixture groups `(INTEGER, TEXT)` pairs, distinguishes tuples by either component, groups NULL combinations correctly, and checks redacted input, row/column bounds, zero rows, capacity failure, and output preservation.
- Verification: the focused fixture passed with `-Wall -Wextra -Werror`; the complete maintained TinyCC SDK suite (`make -f engine/sdk/Makefile test`) also passed after implementation.
- Scope at implementation time: this was an operator primitive only. The v13 parser, binder, runner, and SQLR writer now consume multi-key statements; confined runtime acceptance remains open.

## SQL-7 dependency: multi-key GROUP BY parser, runner and SQLR — implemented

- Contract: [SQL library](../../specs/sql-library.md). V13 accepts two to eight group keys and corresponding non-aggregate projections in the same order, with COUNT(*), MIN, MAX, SUM, or AVG, optional WHERE, and final LIMIT. Key matching is case-insensitive. V1-v12 ASTs and behavior stay frozen; v13 does not accept HAVING or ORDER BY.
- Red/green evidence: the maintained v13 fixture first failed to link because both entry points were absent. It now checks two-key WHERE/LIMIT binding, typed ordinal metadata, the eight-key and AVG bounds, one/duplicate/mismatched/over-limit keys, unsupported HAVING, output preservation, malformed binding records, and rejection by the frozen v12 parser.
- Implementation: `prdb_sdk_sql_v13.h` defines sized statement/binding records embedding v7 plus ordered projection and group-key spans. The parser enforces matching key lists; the binder resolves all key ordinals and type/Decimal metadata and rejects duplicate ordinals.
- Verification: focused v13 parser/binder fixture and complete maintained TinyCC SDK suite passed with TinyCC `-Wall -Wextra -Werror`.
- Implementation: `prdb_sdk_sql_execute_group_v13` uses the prior planner/executor shape checks, 128-row/1 MiB materialization limits, predicate evaluation and cursor-close-before-callback contract. It materializes packed row-major tuples for the composite grouping kernel, computes COUNT/MIN/MAX/SUM/AVG for each stable group, and calls back with the representative key tuple and v13 binding. The SQLR v1 composite encoder/writer writes all group columns plus the correctly typed aggregate field.
- Regression: the maintained grouped executor fixture groups repeated `(id, score)` tuples and checks COUNT and SUM. The result fixture verifies exact three-field COUNT SQLR bytes, chunked writer output and malformed binding rejection. The confined SQL artifact now scans an admitted two-column `grouped_records` table and emits two multi-key SQLR records; the Rust acceptance asserts both exact frames after result relay.
- Verification: the complete maintained TinyCC SDK suite passed with `-Wall -Wextra -Werror`; `make -f engine/sdk/Makefile sandbox-query-fixture` built and linked the confined artifact; the focused `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed through the confined executor, exact SQLR result relay, and durable commit. The acceptance used the configured NixOS runtime libraries and a temporary delegated cgroup created with authorized root access because the invoking desktop process could not migrate into a user-owned sibling cgroup. The fixture now grants the table and both key-column SELECT policies, filters metadata discovery to `records`, and checks that seeded composite rows were committed. Exact SQLR expectations use zero precision and scale for non-DECIMAL types as required by the SQLR v1 contract.
- Remaining: cancellation/deadlines, spill, and mixed-workload measurements remain open.

## SQL-7 dependency: composite GROUP BY HAVING — implemented

- Contract: [SQL library](../../specs/sql-library.md). V14 retains v13's two-to-eight ordered group keys and adds one comparison of the exact selected aggregate to a positional parameter before optional LIMIT. V1-v13 layouts and grammar remain frozen.
- Red/green evidence: the maintained v14 fixture checks multi-key WHERE/GROUP BY/HAVING/LIMIT parsing, aggregate expression matching, parameter index/comparison metadata, typed BIGINT binding for integer SUM, no-HAVING compatibility, and malformed binding output preservation. The grouped runner fixture verifies that a `SUM(score) >= $1` predicate filters composite groups before its callback and receives v14 binding metadata.
- Implementation: the v14 parser preserves original SQL offsets by parsing a same-length scratch copy with the HAVING clause blanked for the v13 parser, then restores the original SQL pointer. The binder derives the HAVING parameter type from the selected aggregate. The runner validates the parameter before scanning, validates all group aggregates and HAVING comparisons before callbacks, applies HAVING before LIMIT, and emits surviving groups through the v14 callback.
- Verification: focused v14 parser/binder and grouped runner fixtures passed with TinyCC `-Wall -Wextra -Werror`; the complete maintained TinyCC SDK suite (`make -f engine/sdk/Makefile test`) passed. Confined runtime acceptance for the multi-key SQL artifact remains unavailable in this container because `/sys/fs/cgroup` is read-only and `sudo -n` is blocked by `no new privileges`.
- Remaining: cancellation/deadlines, spill, and mixed-workload measurements remain open. Composite GROUP BY ORDER BY is implemented and accepted under confinement in the v15 entry below.

## SQL-7 dependency: nine-key stable order kernel — implemented

- Contract: [SQL library](../../specs/sql-library.md). The bounded v3 stable sorter validates and lexicographically sorts up to nine authorized values across at most 128 groups; equal tuples retain input order and failures leave the output permutation untouched. This capacity matches eight composite group keys plus one aggregate output.
- Red/green evidence: the maintained sorter fixture failed at its new capacity assertion when the kernel allowed only two keys. After the bound change, it orders nine values where the first eight tie and the ninth determines the expected stable permutation.
- Implementation: `PRDB_SDK_SQL_ORDER_MAX_KEYS` is now nine; the existing prevalidation and stable insertion-sort implementation already handles arbitrary configured key count within this bound. V12's SQL grammar and ABI remain capped at two expressions.
- Verification: the focused TinyCC sorter fixture and complete maintained TinyCC SDK suite (`make -f engine/sdk/Makefile test`) passed with `-Wall -Wextra -Werror`; `git diff --check` passed. Composite ORDER BY parser, binder, and runner integration remains open.
- Remaining: cancellation/deadlines, spill, and mixed-workload measurements remain open. The separately versioned composite grouped ORDER BY contract and v15 runner are implemented and covered below.

## SQL-7 dependency: composite GROUP BY ORDER BY — implemented

- Contract: [SQL library](../../specs/sql-library.md). V15 preserves v14 composite GROUP BY/HAVING and accepts up to nine distinct ORDER BY outputs selected from the one-to-eight group keys plus the selected aggregate. Each sort item has independent direction and NULL placement; defaults remain ASC and NULLS FIRST. HAVING filters before sorting, ties remain stable, and LIMIT applies after sorting. Earlier AST/binding versions stay frozen.
- Implementation: [`prdb_sdk_sql_v15.h`](../../engine/include/prdb_sdk_sql_v15.h) adds the sized statement/binding records and typed order metadata. The parser/binder enforce selection, duplicate, key-count and list syntax limits. [`prdb_sdk_sql_execute_group_v15`](../../engine/include/prdb_sdk_sql_exec_v15.h) uses the existing bounded nine-key sorter after aggregation and HAVING, then invokes a v15 callback. Composite SQLR output can use the embedded v13 binding.
- Regression: [`sdk_sql_select_v15.c`](../../engine/tests/fixtures/sdk_sql_select_v15.c) checks two-key parsing, HAVING, typed three-item binding, nine selected sort outputs, duplicate/unselected expressions, LIMIT placement, v14 rejection and output preservation. The maintained grouped executor fixture now checks HAVING-before-sort, aggregate and key tie breakers, LIMIT after sorting, and callback order over a four-row input. Its first hand-written expected permutation was incorrect; correcting the independent lexicographic expectation made the focused fixture pass.
- Verification: the complete maintained TinyCC SDK fixture suite and focused v15 executor fixture passed. The confined `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance now passes with the v15 artifact path, verifies both composite GROUP BY records and both lexicographically ordered records, and reaches durable commit. This run used the configured NixOS runtime library paths and a temporary delegated cgroup created with authorized root access. The fixture's policy table epoch read now has the required expected catalog value; the earlier `InvalidMetadata` was caused by that missing fixture metadata. Rust 1.95.0 workspace build, rustfmt check, warning-denied Clippy, and serial `cargo test --workspace --locked --offline -- --test-threads=1` passed (127 commit integration tests passed, 3 sandbox cases ignored, and all other workspace tests passed). The parallel workspace test attempt had one known `InvalidAudit` failure in `storage_manager_creates_and_drops_check_constraint`; the same test passed in the serial run.
- Remaining: cancellation/deadlines, spill and mixed-workload measurements remain open.

## SQL-7 dependency: multi-predicate INNER JOIN execution — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). V16 accepts two to eight qualified equality predicates across two independently authorized relations, with matching types and Decimal scales, stable left-major results, NULL non-matches, and optional LIMIT.
- Implementation: [`sql_exec_v16.c`](../../engine/sdk/sql_exec_v16.c) independently describes and resolves each side, checks planner/executor shape agreement, scans sequentially and closes both cursors before callbacks. It retains at most 128 rows per side and 1 MiB of copied join keys and projected values, uses the composite pair kernel, and returns the embedded v8 binding to callbacks for SQLR encoding.
- Regression: the maintained executor fixture checks two-key filtering, the full eight-key runner bound, stable projected values, LIMIT, materialization-cap failure, and callback failure after both scans close. The confined artifact joins two authorized two-column relations on both keys and checks two exact SQLR result frames through the gateway relay.
- Verification: the full TinyCC SDK suite and `make -f engine/sdk/Makefile sandbox-query-fixture` passed. Rust build, format check, warning-denied Clippy, serial locked workspace tests (127 commit tests passed; 3 cgroup-dependent tests ignored), and the dedicated confined acceptance passed. The acceptance ran the artifact through the real sandbox and durable commit using the configured NixOS runtime paths and a temporary delegated cgroup created with authorized root access. `git diff --check` passed.
- Remaining: cancellation/deadlines, spill, and mixed-workload measurements remain open; aliases, self-joins, WHERE, and outer joins are unsupported.

## SQL-8 dependency: cooperative cancellation in bounded scan runners — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [executor IPC](../../specs/ipc-protocol.md). Each SQL scan runner checks invocation cancellation/deadline state between bounded executor batches.
- Implementation: SELECT, DML scan, grouped aggregation, and inner-join runners use `prdb_sdk_executor_check_cancelled_v1`. A positive result or malformed check stops scanning, closes a live cursor where possible, returns an executor error, and triggers the host's sticky statement-failure callback before partial result publication. After accepted cancellation, the Rust supervisor keeps executor channels available for up to 500 ms for cooperative polling, then kills and reaps a worker that remains active; hard invocation deadlines still apply during this period.
- Red/green evidence: the maintained grouped SQL runner fixture initially rejected the newly emitted operation. It now simulates cancellation both before the first batch and after one nonterminal batch, verifying no further fetch or group callback occurs and statement failure is latched.
- Verification: complete `make -f engine/sdk/Makefile test` passed with TinyCC 0.9.27-unstable-2025-01-06. The confined `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance now relays `CANCEL_OBSERVED`, which the TinyCC query writes only after `prdb_sdk_executor_check_cancelled_v1` returns a positive response. The test confirms the output follows the accepted cancel request, arrives within two seconds, and precedes durable ABORTED terminal relay. The real gateway/supervisor process acceptance also bounds durable ABORTED for a non-cooperative query to two seconds. Both focused sandbox acceptances passed on a delegated cgroup with configured NixOS runtime paths. Workspace format, warning-denied Clippy, and serial tests pass; the maintained SDK suite passed earlier in the same validation cycle.
- Remaining: complete broader mixed-workload measurements and spill support; SQL aliases, self-joins, WHERE, and outer joins remain unsupported.

## SQL-8 dependency: v16 composite join cancellation and request budget — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [workload profile](../../specs/workload-profile.md). The multi-predicate INNER JOIN runner now polls cancellation after pair generation and every 2,048 output pairs, and shares a 121-page budget across both scans so setup, scan, close, and cancellation requests remain within the executor's 256-request ceiling.
- Red/green evidence: the v16 executor fixture first failed when cancellation was requested after both scans but before join output. It now verifies statement failure with no callback output. A second fixture supplies 121 nonterminal left pages and verifies the runner returns `REQUEST_LIMIT` before another fetch, closes the live cursor, and sends exactly 246 executor requests.
- Implementation: the runner reserves six calls for relation resolution/open/close and eight for post-scan cancellation checks; each counted scan page reserves one cancellation poll and one batch fetch. It aborts before exceeding the shared page budget.
- Verification: `make -f engine/sdk/Makefile test` passed with TinyCC 0.9.27-unstable-2025-01-06. Locked workspace build, rustfmt check, warning-denied Clippy, and all workspace tests passed (128 commit tests passed; 3 cgroup-dependent tests ignored). `git diff --check` passed. This increment's cancellation and request ceiling are covered by the maintained TinyCC executor fixture; no separate sandboxed v16 cancellation acceptance was run.
- Remaining: broader mixed-workload measurements, spill, and stress coverage of cancellation during long-running callbacks remain open.

## SQL-7 dependency: v17 nonnegative BIGINT HAVING literal parser and binder — implemented

- Contract: [SQL library](../../specs/sql-library.md). V17 preserves v15 composite GROUP BY, parameterized selected-aggregate HAVING, ORDER BY, and LIMIT, and accepts a nonnegative integer literal up to `INT64_MAX` as the HAVING comparison's right operand. Binding accepts it only when the selected aggregate result is BIGINT; earlier AST and binding versions remain unchanged.
- Red/green evidence: the maintained v17 fixture first failed to link because the versioned parser and binder were absent. It now checks literal parsing with composite ORDER BY/LIMIT, parameter compatibility, overflow and negative rejection, aggregate-expression matching, v15 rejection, BIGINT type binding, Decimal-result rejection, malformed literal-bound rejection, and output preservation on binding failure.
- Implementation: [`prdb_sdk_sql_v17.h`](../../engine/include/prdb_sdk_sql_v17.h) embeds the v15 records. The parser masks only the HAVING clause while reusing the frozen v15 parser for the rest of the statement, then validates the selected aggregate/comparison and records the literal. The binder reuses v15 schema/aggregate resolution and rejects non-BIGINT literals and tampered values above `INT64_MAX`.
- Verification: the focused TinyCC parser/binder fixture passed with `-Wall -Wextra -Werror` after a forced rebuild, proving the header dependency is included in the SDK build. Runner behavior was completed in the follow-up entry below; compound Boolean HAVING remains open.

## SQL-7 dependency: v17 literal HAVING grouped runner — implemented

- Contract: [SQL library](../../specs/sql-library.md). Execute the v17 composite grouping query with either a nonnegative BIGINT literal or the unchanged v15 parameter operand; apply HAVING before composite ordering and LIMIT, and emit no callback for groups that fail the predicate.
- Red/green evidence: the maintained grouped executor fixture initially failed to link because `prdb_sdk_sql_execute_group_v17` was absent. It now checks literal and parameter execution over a three-group input, verifies literal metadata in the v17 callback binding, observes only the qualifying SUM group, and confirms an overflowing literal fails before any scan or callback.
- Implementation: [`prdb_sdk_sql_exec_v17.h`](../../engine/include/prdb_sdk_sql_exec_v17.h) exposes the runner with a v17 callback binding. The shared bounded v13-v15 grouped execution path now accepts v17 parse/bind mode and creates canonical big-endian BIGINT bytes for the literal, then reuses the existing aggregate comparison, stable ordering, LIMIT, cancellation, and output validation.
- Verification: focused `sdk_sql_execute_v10` fixture passed after a forced full TinyCC rebuild; `make -f engine/sdk/Makefile test` and the locked Rust workspace build, format, warning-denied Clippy, and serial test suite passed. Compound Boolean HAVING and other literal types remain open.

## SQL-7 dependency: v17 literal HAVING confined acceptance — verified

- Integration: the maintained `sql_v3_write_read.c` artifact executes a v17 composite `COUNT(*) >= 1` literal query in the confined executor, verifies the literal in the callback binding, and writes the ordered results through SQLR and gateway relay. The Rust acceptance checks the two exact frames and durable COMMITTED outcome alongside the existing v15 parameterized HAVING results.
- Verification: `make -f engine/sdk/Makefile sandbox-query-fixture` compiled the artifacts with TinyCC and linked them with the maintained linker. `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` passed through the delegated cgroup, runtime allowlist, confined worker, result relay, and storage commit. The temporary delegated cgroup was removed after the run.
- Remaining: compound Boolean HAVING, other literal types, and stress/measurement work remain open.

## SQL-7 dependency: v18 compound HAVING parser and binder — implemented

- Contract: [SQL library](../../specs/sql-library.md). V18 adds bounded AND/OR/NOT HAVING expressions with parentheses, exact selected-aggregate comparisons, positional parameters or nonnegative BIGINT literals, SQL three-valued semantics, a 64-node limit, and a nesting limit of 32. V1-v17 layouts stay unchanged.
- Red/green evidence: the maintained v18 fixture first failed to link because the parser and binder entry points were absent. It now checks precedence and postorder children for `(SUM(c) >= 5 OR SUM(c) = $2) AND NOT SUM(c) < 20`, selected-aggregate matching, parameter/literal metadata, the 64-node and depth bounds, BIGINT typing, Decimal literal rejection, malformed literal tampering, and output preservation on binding failure.
- Implementation: [`prdb_sdk_sql_v18.h`](../../engine/include/prdb_sdk_sql_v18.h) defines the sized AST/binding and operand variants. The parser masks HAVING while reusing the frozen v15 parser for composite grouping, ORDER BY, and LIMIT, then builds a bounded postorder Boolean tree. The binder validates tree shape/reachability and derives aggregate operand metadata through the v15 binder while keeping embedded v15 HAVING fields neutral.
- Verification: focused TinyCC v18 fixture and full `make -f engine/sdk/Makefile test` passed. Locked Rust workspace build, format, warning-denied Clippy, serial tests (129 commit tests passed; 3 cgroup-dependent tests ignored), and `git diff --check` passed.
- Remaining at parser/binder commit: the v18 runner and confined acceptance are recorded in the follow-up entry below.

## SQL-7 dependency: v18 compound HAVING grouped runner — implemented and accepted

- Contract: [SQL library](../../specs/sql-library.md). Execute the v18 Boolean HAVING tree with parameter and BIGINT-literal leaves, preserving SQL three-valued logic; validate all operands before scanning and evaluate all group results before callbacks, sorting, and LIMIT.
- Red/green evidence: the maintained grouped runner fixture first failed because the v18 runner was absent. It now combines `SUM(score) >= 9`, parameterized `SUM(score) = $1`, `OR`, `NOT`, and `AND` across groups whose sums produce TRUE, FALSE, and UNKNOWN. Only the 9 and 3 groups reach the callback; the 5 group is FALSE and the all-NULL group is UNKNOWN. The runner then sorts and applies LIMIT. Missing and INTEGER-typed parameters fail before the first batch. Callback checks verify the v18 AST binding and neutral embedded v15 HAVING fields.
- Implementation: [`prdb_sdk_sql_execute_group_v18`](../../engine/include/prdb_sdk_sql_exec_v18.h) uses the bounded composite scan/aggregation path, binds every parameter leaf before scanning, constructs canonical BIGINT values for literal leaves, evaluates the postorder Boolean tree through the SQL truth helpers, and filters before the existing stable sorter and LIMIT.
- Confined acceptance: the maintained `sql_v3_write_read.c` artifact executes `(COUNT(*) >= 2 OR COUNT(*) = 3) AND NOT COUNT(*) < 2`, checks the v18 callback metadata, and emits one composite SQLR frame. The Rust acceptance compares that frame byte-for-byte after result relay and verifies durable COMMITTED.
- Verification: the full maintained TinyCC suite and `make -f engine/sdk/Makefile sandbox-query-fixture` passed. The repository cgroup probe reported a writable delegated cgroup v2 parent with `cpu memory pids` available and enabled; the opt-in `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed. The temporary child cgroup was removed. Rust format, whitespace, locked workspace build, warning-denied Clippy, and serial tests passed (129 commit tests; 3 cgroup-dependent tests ignored by default).
- Remaining: other literal types and broader analytical stress/measurement work remain open. The runner fixture covers TRUE, FALSE, and UNKNOWN across distinct groups, while the v18 truth-helper fixture covers the complete NOT/AND/OR table.

## SQL-7 dependency: v18 compound HAVING parser and binder fuzz target — verified

- Contract: fuzz the C parser independently from Rust protocol decoders, and run successful parses through the matching binder without executing query code.
- Implementation: `engine/tests/fuzz/sql_v18_fuzz.c` exposes a libFuzzer entry point that parses up to the documented 65,536-byte SQL limit, checks sized/versioned AST invariants, and binds successful parses against a fixed two-column schema. Four checked-in seeds cover compound Boolean HAVING, NOT/parentheses, the maximum BIGINT literal, and malformed syntax. `make -f engine/sdk/Makefile fuzz-sql-v18` builds the parser, value, and batch SDK sources with Clang libFuzzer, AddressSanitizer, and UndefinedBehaviorSanitizer, uses fixed seed 18, and resets its generated corpus under `target/` before each run.
- Verification: the target completed 10,000 mutations from the checked-in seed corpus with no crash or sanitizer finding (1,257 coverage counters); the standard TinyCC SDK suite remains separate from this optional Clang fuzz target.
- Remaining: broader analytical stress/measurement work remains open.

## SQL-7 dependency: v18 three-valued truth helper matrix — verified

- Contract: SQL `NOT`, `AND`, and `OR` use three-valued truth tables, with UNKNOWN preserved unless a decisive FALSE for AND or TRUE for OR determines the result.
- Regression: the maintained v18 parser/binder fixture now checks NOT for all three inputs and AND/OR for all nine input pairs against explicit expected tables. The grouped runner fixture separately verifies that an all-NULL SUM comparison is UNKNOWN and does not reach the callback.
- Verification: the focused v18 fixture and complete TinyCC SDK suite pass. `cargo fmt --all -- --check` and `git diff --check` pass.
- Remaining: the current grammar compares one selected aggregate against each operand, so it does not express independent input columns within a single group's HAVING tree. Across groups, the runner regression covers TRUE, FALSE, and UNKNOWN outcomes; broader analytical stress/measurement work remains open.

## Confined SQL acceptance fixture corrections — verified

- The delegated-cgroup SQL DML acceptance now exercises its multi-row `INSERT ... VALUES ($1), ($2)` with both bound values populated. Previously it passed a null/empty parameter array, so the fixture emitted its error marker instead of testing the intended successful DML sequence.
- Verification: the confined SQL commit acceptance and durable-abort acceptance both pass. The latter confirms a caught SQL error after partial output still produces a durable abort. The complete maintained TinyCC suite and locked Rust workspace format/build/Clippy/serial test gates pass.

## SQL-7 dependency: v18 grouped runner mixed truth regression — verified

- Regression: strengthened the maintained v18 runner fixture to state and assert the four independent group outcomes for `(SUM(score) >= 9 OR SUM(score) = $1) AND NOT SUM(score) < 3`: sums 9 and 3 are TRUE, sum 5 is FALSE, and the NULL-only sum is UNKNOWN. Only the two TRUE groups reach the callback, in sorted order; the FALSE and UNKNOWN groups are filtered before LIMIT output.
- Correction: the earlier progress note incorrectly concluded that the runner could not exercise mixed truth outcomes because each comparison uses the selected aggregate. Different groups supply different aggregate values, so the existing bounded grammar is sufficient.
- Verification: the focused `sdk-sql-execute-v10` fixture and the complete `make -f engine/sdk/Makefile test` suite passed under the TinyCC Nix shell.
- Remaining: broader analytical stress/measurement work remains open.

## SQL-7 dependency: AVG negative tie-rounding regression — verified

- Regression: the bounded AVG fixture now checks negative half ties at the 128-row limit. `-1/128` rounds to the even scaled coefficient `-7812`, while `-3/128` rounds to `-23438`, confirming nearest/ties-to-even is symmetric for negative inputs.
- Verification: the focused `sdk-sql-group-v1` fixture passed under TinyCC with `-Wall -Wextra -Werror`.
- Remaining: broader analytical stress/measurement work remains open.

## SQL-7 dependency: grouped scan request-budget regression — verified

- Red/green evidence: the v18 grouped runner fixture streams one row per page. Before the fix, the runner completed 128 data pages plus the terminal page, requiring more than the 256-request stream cap once cancellation polls were counted. It now requires a request-limit failure before callback publication and checks the early cursor close keeps the count to 255. The independent v4 COUNT runner has the same 126-page boundary fixture and asserts the same no-callback, 255-request result.
- Implementation: the shared v5-v18 grouped runner and the separate v4 COUNT runner cap scans at 126 pages. Each page consumes a cancellation check and batch request; resolve/open and a possible early close fit in the remaining stream budget.
- Specification: [`sql-library.md`](../../specs/sql-library.md) records the page/request relationship for grouped scans.
- Remaining: further grouped-runner cancellation and pressure stress cases remain open.

## SQL-7 dependency: group cardinality memory ceiling boundary — verified

- Regression: the maintained TinyCC group-kernel fixture submits exactly `PRDB_SDK_SQL_GROUP_MAX_ROWS` rows both as one repeated key and as 128 distinct keys. It checks the full repeated-key count and every distinct group's representative/count. The adjacent 129-row case continues to verify rejection without changing caller outputs.
- Verification: `nix develop --offline --command make -f engine/sdk/Makefile test` passed the complete maintained TinyCC SDK suite, including both exact 128-row cases and over-limit rejection.
- Remaining: grouped cancellation/pressure stress and mixed-workload measurements remain open.

## SQL-8 dependency: cancellation checkpoints during grouped result callbacks — implemented

- Contract: the shared v5-v18 grouped runner must observe cooperative cancellation after scan completion and while publishing a bounded result set, while keeping its own executor calls within the 256-request connection limit.
- Red/green evidence: a maintained TinyCC fixture creates 40 composite groups and requests cancellation immediately after scan completion; it also raises cancellation from the 32nd group callback. With callback polling removed, a forced clean SDK rebuild failed because the first callback ran after cancellation. With the implementation restored, cancellation before the first callback publishes zero groups, and cancellation after 32 callbacks prevents the next one.
- Implementation: the shared runner checks before the first group callback and before every 33rd output. It caps scans at 124 pages, reserving up to four callback checks; a full 124-page scan then uses at most 254 executor calls. The independent v4 COUNT runner retains its 126-page limit.
- Specification: [`sql-library.md`](../../specs/sql-library.md) records the v5-v18 callback interval and page budget.
- Verification: focused and forced-clean full `make -B -f engine/sdk/Makefile test` passed under TinyCC 0.9.27-unstable-2025-01-06. Locked workspace build, format check, warning-denied Clippy, serial tests, and `git diff --check` passed; commit tests reported 138 passed and 4 delegated-cgroup tests ignored.
- Remaining: cancellation checks cannot interrupt a callback while it is executing; supervisor cancellation/deadline termination remains responsible for a callback that does not return. More grouped-runner stress and mixed-workload measurements remain open.

## SQL-8 follow-up: v4 COUNT output cancellation checkpoints — implemented

- Contract: the independent v4 COUNT runner follows the same cancellation checkpoints during grouped output as the shared v5-v18 runner and keeps scan plus callback polling inside the executor request budget.
- Red/green evidence: the maintained TinyCC fixture creates 40 groups, requests cancellation after scan completion, and separately raises cancellation from the 32nd callback. Removing the callback poll made the forced rebuild fail because output continued; restoring it yields zero callbacks for pre-output cancellation and exactly 32 callbacks for cancellation during output.
- Implementation: v4 now checks before its first group callback and before each 33rd output, and lowers its scan ceiling from 126 to 124 pages to reserve up to four callback checks. Its 124-page boundary fixture confirms 251 scan/setup/close executor requests and no callback publication.
- Specification: [`sql-library.md`](../../specs/sql-library.md) records the v4-v18 output checkpoint interval and v4 page budget.
- Verification: focused forced-clean v4 TinyCC rebuild and regression fixture passed. Full `make -B -f engine/sdk/Makefile test`, locked workspace build, format check, warning-denied Clippy, serial tests, and `git diff --check` passed in the Nix development shell.
- Remaining: cancellation checks cannot interrupt a callback while it is executing; supervisor cancellation/deadline termination remains responsible for a callback that does not return. More grouped-runner stress and mixed-workload measurements remain open.

## SM-6 / SQL-6: expose filtered `information_schema.schemata` through executor scans — implemented

- Behavior and contract: [storage manager](../../specs/storage-manager.md), [metadata discovery](../../specs/metadata-discovery.md), and [SQL library](../../specs/sql-library.md). The executor's normal `open_scan`/`next_batch` route can read the fixed virtual schema relation from the invocation's pinned, permission-filtered catalog view.
- Red evidence: the real-redb transaction regression resolved the public `information_schema.schemata` relation successfully but `ExecutorMetadataConnection::open_relation_scan` returned `InvalidMetadataRequest` before the virtual path existed.
- Implementation: executor scans now route that relation through `InvocationTransaction::scan_information_schema_schemata`, preserving stable schema-ID ordering, the invocation's USAGE filtering, one-use IPC continuations, and connection-scoped opaque row handles. Virtual row handles are not entered in the writable row map; attempts to update them and inserts into the built-in relation fail closed. Closing the cursor clears virtual scan state.
- Regression evidence: the test pages one schema at a time through the bounded executor batch API, checks the visible public and USAGE schemas in order, rejects cursor replay, and rejects UPDATE/INSERT on the virtual relation.
- Verification: the focused transaction regression passed. Rust 1.95.0, Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06 were verified in the prescribed Nix shell. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check` passed. The transaction target had 67 passing tests; commit integration had 109 passing and 2 cgroup-dependent tests ignored. The maintained TinyCC suite `make -f engine/sdk/Makefile test` passed before this increment.
- Follow-up: the next SM-6 / SQL-6 increment connects the `tables` and `columns` virtual relations below. SQL execution and API parity remain open; sandboxed end-to-end tests still require a delegated-cgroup host.

## SM-6 / SQL-6: expose `information_schema.tables` and `columns` through executor scans — implemented

- Contract: [storage manager](../../specs/storage-manager.md), [metadata discovery](../../specs/metadata-discovery.md), and [SQL library](../../specs/sql-library.md). The executor's bounded scan API now reads all three fixed information_schema relations through the invocation's authorization-filtered scanners.
- Implementation: the executor converts the existing `tables` and `columns` metadata rows to their declared cell shapes, including one-based integer ordinals and typed NULL cells for inapplicable length/precision/scale fields. Virtual row handles remain read-only; closing cursors, relation handles, and connections clears the matching invocation scan state.
- Regression: the real-redb transaction test pages `schemata`, `tables`, and `columns` one row at a time through `next_relation_scan_batch`, verifies ordered row content and nullable cells, rejects cursor replay, and rejects virtual-row UPDATE and virtual-relation INSERT.
- Verification: Rust 1.95.0, Cargo 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, and TinyCC 0.9.27-unstable-2025-01-06 were used in the prescribed Nix shell. `cargo build --workspace --locked`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --locked` passed. Commit integration had 109 passing tests and 2 delegated-cgroup tests ignored in the default run; both ignored sandbox acceptance tests were then run explicitly and passed. The maintained TinyCC SDK suite and sandbox fixture build passed, as did `git diff --check`.
- Remaining: SQL API execution with a real query callback fixture and full API/SQL parity remain open. Sandboxed end-to-end execution still requires a delegated-cgroup host.

## SQL-7 follow-up: document v18 in the public ABI inventory — verified

- Contract: the SQL library ABI inventory must match the separately versioned parser and binder APIs already implemented and exercised by the v18 compound-HAVING runner and fuzz target above.
- Correction: added `prdb_sdk_sql_v18.h` to the linked header inventory, extended the parser-version range to v1–v18, and summarized its bounded compound Boolean HAVING grammar. The previous inventory ended at v17 despite the v18 contract and implementation being present.
- Verification: all local links in `specs/sql-library.md` resolve, the v18 header and grammar appear in the ABI inventory, and `git diff --check` passed. Documentation-only change; no runtime checks were run.

## SQL-8 follow-up: composite join cancellation during result publication — verified

- Contract: the v16 two-relation runner must poll cancellation during potentially long joined output and latch statement failure after provisional rows have been delivered, allowing the supervisor/storage path to abort the invocation.
- Red/green evidence: the maintained TinyCC host fixture now creates 47 matching rows per side (2,209 composite join results), requests cancellation at the fourth check, and verifies that the runner stops after exactly 2,048 callbacks with the SQL statement-failure callback latched. Temporarily moving the poll interval to 4,096 made the regression fail because execution did not terminate at the expected checkpoint.
- Implementation: no production change was needed; `sql_exec_v16.c` already polls before the first output and every 2,048 joined pairs. This acceptance test now protects that cancellation and request-budget behavior under output pressure.
- Verification: targeted `sdk-sql-execute-v8` TinyCC fixture passed with TinyCC 0.9.27-unstable-2025-01-06 after the regression failed under the intentionally delayed polling interval.
- Remaining: broader mixed-workload measurements, spill support, and cancellation pressure coverage during other long-running operators remain open.

## SQL-8 follow-up: composite join page-limit boundary — verified

- Contract: the v16 join runner must accept each requested page within the caller's row/materialization budget and fail before callbacks if the executor violates the requested candidate limit.
- Regression: the TinyCC runner fixture now accepts exactly 128 rows on both sides and emits the 128 matching rows. A malformed 129-row response to a request for 128 is rejected before opening the right-side scan or publishing output.
- Implementation: no SQL runner change was needed; the executor SDK now rejects response row counts above `max_candidates` and leaves the decoded batch output unchanged on failure.
- Verification: focused executor and SQL runner fixtures plus the full TinyCC SDK suite passed; locked workspace build, formatting, warning-denied Clippy, workspace tests (138 passed; 4 delegated-cgroup tests ignored), and `git diff --check` passed.
- Remaining: broader analytical memory-pressure, spill, and mixed-workload evidence remains open.

## SQL-4 / SM-6 follow-up: generated UUID DEFAULT through confined SQL — verified

- Contract: [SQL transactional execution](../../specs/sql-library.md) and [generated UUID identity DDL](../../specs/ddl-schema.md). A SQL `DEFAULT` marker must reach storage as a generated identity request; only storage materializes the UUID, and the invocation must commit it through the normal supervisor path.
- Regression: `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` now creates `identity_records` with a generated UUID column, gives the query principal INSERT scope and an INSERT allow policy, and runs `INSERT INTO identity_records VALUES (DEFAULT)` through the TinyCC SQL SDK and confined planner/executor services. After durable COMMITTED, the test scans the real redb table and verifies exactly one nonzero UUID tuple.
- Verification: forced TinyCC sandbox fixture build, complete TinyCC SDK suite, focused delegated-cgroup acceptance, locked workspace build, format check, warning-denied Clippy, and full serial workspace suite all passed. The fixture's artifact is loaded only by the confined executor; the Rust assertion reads the committed row after terminal outcome confirmation. Touched documentation links and `git diff --check` pass.
- Remaining: broader SQL/API parity, transaction conflict and policy acceptance, spill support, and mixed-workload measurements remain open.

## SQL-7 / WI-7 follow-up: assert v20 WHERE parser and binder invariants under fuzzing — verified

- Contract: the v20 aliased INNER JOIN parser accepts a single qualified comparison or NULL test, and the binder resolves that predicate to one side's declared column shape. See [SQL library ABI and grammar](../../specs/sql-library.md).
- Harness: `engine/tests/fuzz/sql_v20_fuzz.c` now checks nonempty in-bounds qualified spans, valid comparison codes, the required parameter-index/null-test relationship, and that a successfully bound WHERE ordinal/type belongs to the selected relation side. Added corpus seeds for the maximum parameter index and a left-side `IS NULL` predicate.
- Verification: `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile fuzz-sql-v20` completed 10,000 libFuzzer runs with ASan/UBSan, 891 coverage counters and 2,284 features, with no crash or sanitizer finding. The maintained TinyCC suite, locked workspace build, format check, warning-denied Clippy, and full serial delegated-cgroup workspace suite passed. `git diff --check` passed.
- Scope: this strengthens the parser/binder fuzz oracle; it does not expand SQL grammar or execution support. Longer campaigns and broader analytical stress remain open.

## SQL-7 follow-up: support signed BIGINT literals in compound HAVING — verified

- Contract: [SQL library](../../specs/sql-library.md). V18 compound HAVING comparisons accept signed BIGINT literals across the full `INT64_MIN..=INT64_MAX` range; v17 retains its earlier nonnegative-only grammar and AST behavior.
- Implementation: the shared lexer recognizes a unary minus token, and the v18 literal parser bounds positive and negative magnitudes separately before storing a canonical two's-complement bit pattern in the existing `uint64_t` node field. V18 binding accepts the full signed bit range while retaining the BIGINT aggregate type requirement. The v18 grouped runner already serializes the stored bits as canonical big-endian BIGINT bytes, so negative thresholds use the normal typed comparison path without changing the AST layout.
- Regression: maintained parser/binder fixture covers `INT64_MIN`, rejects `-9223372036854775809`, and confirms signed metadata survives binding. The grouped runner fixture executes `SUM(score) >= -1` and verifies all three non-NULL groups are emitted. The v18 fuzz corpus now includes the signed minimum boundary.
- Verification: focused v18 parser and grouped runner fixtures passed. `make -f engine/sdk/Makefile test sandbox-query-fixture` passed. `make -f engine/sdk/Makefile fuzz-sql-v18` completed 10,000 runs from five checked-in seeds without sanitizer findings (1,219 coverage counters). Locked workspace build, format check, warning-denied Clippy, and the full serial delegated-cgroup workspace suite passed; the suite reported 233 library tests, 172 commit tests, and 75 transaction tests, with no ignored tests. `git diff --check` passed.
- Historical status at this checkpoint: fractional and exponent HAVING literals were unsupported; the v21 implementation above addresses that gap. Broader analytical stress and measurement work remains open.

## SQL DDL boundary follow-up: CREATE TABLE stays on the admin channel — implemented

- Contract: [SQL library](../../specs/sql-library.md) and [schema DDL](../../specs/ddl-schema.md). SQL DDL text must not enter the user invocation dispatcher, executor callbacks, or query gateway. The first supported SQL DDL convenience method is `AdminClient::execute_create_table_sql`, which translates locally to the established typed control operation.
- Implementation/evidence: the bounded parser and authenticated control-session integration test are recorded in the owning [schema DDL progress](../ddl-schema/PROGRESS.md). This SQL-library entry records the boundary and keeps query-execution DDL rejected.
- Verification: see DS-8 targeted parser and mock TCP control-session test results in the schema DDL progress entry.
- Next smallest behavior: keep the query dispatcher rejection regression aligned as SQL administrative syntax coverage expands.


## SQL DDL follow-up: exact typed CREATE TABLE defaults — implemented

- Contract: administrative SQL remains client-parsed and routed over authenticated control mode; query invocation SQL continues to reject DDL.
- Implementation/evidence: the owning schema DDL entry now records supported typed default parsing and the control-session serialization regression in [schema DDL progress](../ddl-schema/PROGRESS.md).
- Verification: focused parser and TCP control-session tests passed; see the DS-8 follow-up entry for details.
- Next smallest behavior: preserve the query execution DDL rejection while adding generated UUID identity syntax to the administrative CREATE TABLE grammar.


## SQL DDL defaults follow-up — verified

- Contract: client-parsed administrative CREATE TABLE defaults must use the existing typed control operation; user invocation SQL continues to reject DDL.
- Evidence: exact typed literal conversion and the control-session serialization path are covered by the DS-8 default follow-up in [schema DDL progress](../ddl-schema/PROGRESS.md).
- Verification: the full serial workspace suite passed under delegated cgroups with all confined/process tests enabled and zero ignored tests.
- Next: generated UUID identity SQL syntax remains the next parser extension.

## SQL-8 follow-up: Decimal MIN through confined SQL — verified

- Contract: v21 numeric-literal HAVING binds Decimal thresholds to the aggregate's result shape; MIN preserves its input precision and scale. The bounded composite grouping runner applies ORDER BY and LIMIT before publishing its result.
- Regression: the sandbox SQL artifact runs `MIN(decimal_value) <= 1.00e0 ORDER BY left_key ASC LIMIT 1` over DECIMAL(4,2) rows, validates the v21 binding and literal coefficient, and writes one composite-key SQLR row. The confined acceptance checks the exact relayed record bytes. The maintained v10 runner fixture also covers Decimal MIN with ORDER BY and LIMIT.
- Verification: `make -f engine/sdk/Makefile test sandbox-query-fixture`, `cargo fmt --all -- --check`, and the focused `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` test passed. The acceptance ran inside the delegated user cgroup service and exercised its live analytics/operational workload.
- Red/green evidence: the first acceptance exposed that the fixture had incorrectly expected SUM's widened Decimal precision for MIN HAVING metadata; correcting the assertion to input precision 4 made execution pass. Exact SQLR comparison then exposed and corrected one extra expected zero byte.
- Remaining: broader SQL/API parity, spill support, and mixed-workload analytical measurements remain open.

## SQL-7 follow-up: grouped MAX NULL and redaction behavior — verified

- Contract: `MAX` ignores NULL inputs, returns NULL for an all-NULL group, and fails the statement when a required aggregate input is REDACTED. No partial group callbacks are published on that evaluation failure.
- Regression: the maintained grouped runner fixture now checks all-NULL MAX groups, a grouped input with one all-NULL group and one VALUE group, and a REDACTED input that fails before any callback. This complements the existing corresponding MIN, SUM, and AVG cases.
- Verification: the focused `sdk-sql-execute-v10` fixture passed under TinyCC with `-Wall -Wextra -Werror`.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-7 follow-up: Decimal MAX through v21 and confined SQL — verified

- Contract: composite Decimal MAX retains the source DECIMAL precision and scale for HAVING comparison, and applies exact numeric-literal coercion before scanning. ORDER BY and LIMIT run after HAVING.
- Regression: the maintained v21 runner executes `MAX(score) >= 8.00e0 ORDER BY id ASC LIMIT 1` over DECIMAL(10,2) data and verifies the bound literal, input metadata, maximum coefficient, and selected group. The confined query artifact runs `MAX(decimal_value) >= 2.00e0 ORDER BY left_key ASC LIMIT 1` on DECIMAL(4,2) groups and the Rust acceptance verifies the exact `(2,9,2.25)` SQLR frame.
- Verification: focused `sdk-sql-execute-v10`, `make -f engine/sdk/Makefile sandbox-query-fixture`, and the delegated `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed. The acceptance also exercised the live analytics/operational workload.
- Remaining: broader SQL/API parity, spill support, and mixed-workload analytical measurements remain open.

## SQL-7 follow-up: v21 Decimal MAX NULL and redaction behavior — verified

- Contract: Decimal MAX ignores NULL inputs; a NULL aggregate yields UNKNOWN for numeric HAVING and is filtered. REDACTED is an evaluation error and prevents partial result callbacks.
- Regression: the v21 Decimal runner now checks an all-NULL MAX query with numeric HAVING returns success and no rows, and a REDACTED Decimal row returns evaluation failure with no callbacks and statement failure latched.
- Verification: the focused `sdk-sql-execute-v10` TinyCC fixture passed with `-Wall -Wextra -Werror`.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-7 / SQL-8 follow-up: Decimal SUM overflow through grouped SQL — verified

- Contract: grouped Decimal SUM returns DECIMAL(38,s), but an exact sum outside the result precision fails before publishing group callbacks; statement failure is latched for the invocation.
- Regression: the maintained v21 runner fixture supplies two rows with the maximum DECIMAL(38,2) coefficient in the same group. `SUM(score) >= 0.00e0` overflows the aggregate and must return evaluation error with zero callbacks and a latched statement failure.
- Verification: focused `sdk-sql-execute-v10` TinyCC fixture passed with `-Wall -Wextra -Werror`.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-8 follow-up: v21 grouped cancellation checkpoints — verified

- Contract: the v21 composite grouping entry point inherits bounded runner cancellation before scanning, during scans, and during group result publication; cancellation latches statement failure and stops later callbacks.
- Regression: the v21 Decimal runner fixture now checks cancellation before the first batch, cancellation after one fetched batch, and callback-triggered cancellation after 32 of 40 qualifying groups. The last case confirms the shared checkpoint prevents callback 33.
- Verification: focused `sdk-sql-execute-v10` TinyCC fixture passed with `-Wall -Wextra -Werror`.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-7 / SQL-8 follow-up: v21 Decimal HAVING coercion fails before scan — verified

- Contract: Decimal HAVING literals must coerce exactly to the aggregate result scale; a literal that would lose a nonzero fractional digit fails binding before executor batches are requested.
- Regression: the maintained v21 grouped Decimal runner now checks `SUM(score) >= 1.251` against a scale-2 aggregate. It returns `BIND_ERROR`, publishes no groups, requests no scan batches, and latches statement failure.
- Verification: `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed, including the focused `sdk-sql-execute-v10` fixture under TinyCC with `-Wall -Wextra -Werror`.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-7 / SQL-8 follow-up: v21 Decimal exponent overflow fails before scan — verified

- Contract: a v21 Decimal HAVING literal outside the aggregate result precision is rejected during binding, before the grouped runner requests scan batches.
- Regression: the maintained v21 Decimal runner now checks `SUM(score) >= 1e36` against its `DECIMAL(38,2)` aggregate result. It returns `BIND_ERROR`, publishes no groups, requests no batches, and latches statement failure.
- Verification: `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed, including the focused `sdk-sql-execute-v10` fixture under TinyCC with `-Wall -Wextra -Werror`.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-7 follow-up: v21 Decimal HAVING equality filters exact group — verified

- Contract: Decimal HAVING comparisons use the exact bound scale and filter groups after aggregation, before callbacks.
- Regression: the v21 grouped Decimal runner now evaluates `SUM(score) = 4.50e0` over group totals 4.50, 4.49, and 5.00, emitting only the exact-match group with its composite key and aggregate value.
- Verification: `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed, including the focused `sdk-sql-execute-v10` fixture under TinyCC with `-Wall -Wextra -Werror`.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-7 follow-up: v21 Decimal AVG literal HAVING — verified

- Contract: Decimal AVG returns `DECIMAL(38,r)` with `r=min(max(6,s+6),38-(p-s))`; v21 HAVING literals must bind exactly to that output scale before comparison.
- Regression: the v21 grouped runner now evaluates `AVG(score) >= 1.40000000e0` against Decimal(10,2) groups, verifies the bound Decimal(38,8) literal coefficient, and emits only groups with averages 1.50 and 2.50, filtering 1.30.
- Verification: the maintained TinyCC `sdk-sql-execute-v10` fixture passed as part of `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test`.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-8 follow-up: catch cancellation after the final grouped scan batch — verified

- Contract: the shared grouped runner must observe cancellation raised during the final scan batch before it starts local grouping/aggregation, even when HAVING will remove every group and no result callback will run.
- Red/green evidence: the v21 Decimal fixture now raises cancellation from the final batch callback while its SUM/HAVING threshold filters all groups. Before the final scan checkpoint, that invocation returned success because no group callback polled cancellation; after the change it returns executor error with zero callbacks and statement failure latched.
- Implementation: the shared v5-v21 runner polls cancellation once after scan completion and cursor cleanup, before grouping or aggregate evaluation. At the 124-page scan ceiling this keeps a full scan with the final checkpoint and up to four callback polls within 255 of the executor's 256 requests.
- Specification: [`sql-library.md`](../../specs/sql-library.md) records the final checkpoint and request ceiling.
- Verification: `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed with TinyCC 0.9.27-unstable-2025-01-06; `git diff --check` passed.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-8 follow-up: v4 COUNT observes terminal-batch cancellation with no groups — verified

- Contract: the independent v4 COUNT runner must notice cancellation during its terminal batch before local grouping, even when WHERE filters out every row and no output callback is reached.
- Red/green evidence: the maintained TinyCC fixture applies a no-match `WHERE` predicate to two scan pages and raises cancellation at the final poll. Before the fix the invocation returned success; after it the runner returns executor error, emits no groups, and latches statement failure.
- Implementation: v4 polls once after scan completion and cursor cleanup. Its 124-page cap leaves room for the final poll and up to four group-output polls within 255 executor requests.
- Specification: [`sql-library.md`](../../specs/sql-library.md) records the terminal-batch poll and request bound.
- Verification: the focused `sdk-sql-execute-v2` fixture and full TinyCC SDK suite passed; `git diff --check` passed.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-8 follow-up: v8 join shares its scan budget and observes terminal cancellation — verified

- Contract: the v8 two-relation join runner shares one page budget across both scans and observes cancellation after pair generation, including when the join has no matches. Long result emission also polls cancellation at a bounded interval.
- Regression: the maintained fixture raises cancellation during the terminal right-side batch of a no-match join and verifies zero callbacks plus a latched statement failure. It also exhausts the shared 121-page limit on the left scan and verifies the active cursor closes before another fetch or the right scan begins.
- Implementation: v8 now shares a 121-page ceiling across both relations, polls before each page, polls after pair generation, and polls every 2,048 emitted pairs. At the page and pair limits, the runner uses at most 254 executor requests within the 256-request ceiling.
- Specification: [`sql-library.md`](../../specs/sql-library.md) records the shared budget, cancellation checkpoints, and request bound.
- Verification: focused `sdk-sql-execute-v8` fixture and `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed with TinyCC; `git diff --check` passed.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-7 follow-up: v21 HAVING enforces signed BIGINT literal bounds — verified

- Contract: v21 numeric HAVING literals bound to BIGINT aggregates only when exact rescaling produces a signed 64-bit value; both signed endpoints are valid.
- Red/green evidence: the v21 parser/binder fixture now checks `INT64_MIN` and `INT64_MAX` bind exactly and rejects each adjacent overflow value. The positive overflow regression failed before the fix because the binder checked upper-byte sign extension without validating the sign bit of the retained 64-bit value.
- Implementation: BIGINT coercion now verifies that the retained value's sign bit agrees with the source sign after rescaling and sign extension.
- Specification: [`sql-library.md`](../../specs/sql-library.md) already specifies exact signed 64-bit range checking for v21 HAVING literals.
- Verification: focused v21 parser/binder and grouped runner TinyCC fixtures passed with `-Wall -Wextra -Werror`.
- Remaining: broader analytical type/workload, spill, and mixed-workload measurements remain open.

## SQL-8 follow-up: v2 SELECT request budget and terminal cancellation — verified

- Contract: the v2 SELECT runner bounds all executor requests, including one cancellation check per fetched page, and observes cancellation raised by the terminal batch even when WHERE filters out every row.
- Regression: the maintained fixture uses a no-match predicate to raise cancellation on the post-scan poll, asserting zero callbacks and a latched statement failure. A second case exhausts the 126-page ceiling and checks the active cursor closes with exactly 255 executor requests.
- Implementation: v2 now caps scans at 126 pages, polls cancellation after scan completion and cursor cleanup, and keeps both the page-limit and full-scan paths within the 256-request ceiling.
- Specification: [`sql-library.md`](../../specs/sql-library.md) records the request budget and terminal checkpoint.
- Verification: focused `sdk-sql-execute-v2` fixture and `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed under TinyCC; `git diff --check` passed.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-7 / WI-7 follow-up: v21 HAVING fuzz seeds exercise successful binding — verified

- Correction: the v21 fuzz corpus previously used `category` and `scores`, which did not match the harness's bound `id`/`score` shape. Those seeds could parse but did not reach successful binding. Replaced them with schema-matched statements and added a compound Decimal seed covering signed exponent literals, Boolean precedence, ORDER BY, and LIMIT.
- Oracle: under its Decimal(10,2) shape, the fuzz target checks numeric HAVING operands remain comparison nodes with cleared parameter/literal fields and Decimal literal scale matches the bound aggregate scale.
- Verification: all three seed statements passed an explicit v21 parse/bind check. `make -f engine/sdk/Makefile fuzz-sql-v21` completed 10,000 ASan/UBSan/libFuzzer runs without findings (1,572 coverage counters, 3,822 features).
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-7 / WI-7 correction: v21 BIGINT binding now reaches the fuzz oracle — verified

- Correction: the prior v21 fuzz target bound every input only against a Decimal(10,2) aggregate shape. Its BIGINT sign-extension oracle was unreachable, despite the existing exact-boundary fixture testing the binder directly.
- Change: the fuzz target now binds each successfully parsed statement against both Decimal and INTEGER aggregate shapes, checks the derived result type for each HAVING binding, and applies the corresponding scale or signed-extension oracle. It requires the schema-matched boundary seed to bind successfully as BIGINT; statements without HAVING correctly have no HAVING result type.
- Verification: `make -f engine/sdk/Makefile fuzz-sql-v21` completed 10,000 ASan/UBSan/libFuzzer runs without findings (1,582 coverage counters, 3,994 features). The exact signed BIGINT endpoint seed bound successfully.
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-7 / WI-7 follow-up: v21 malformed input preserves outputs under fuzzing — verified

- Contract: v21 parse and bind APIs write output records only on success; malformed or inexact input must not expose partially initialized AST or binding data.
- Oracle: the v21 fuzzer initializes outputs with sentinel bytes and traps if either API changes them on failure. Successful bindings continue to be checked for sized ABI fields and canonical numeric literal metadata.
- Verification: `make -f engine/sdk/Makefile fuzz-sql-v21` completed 10,000 ASan/UBSan/libFuzzer runs without findings (1,574 coverage counters, 3,766 features).
- Remaining: broader analytical pressure, spill, and mixed-workload measurements remain open.

## SQL-8 follow-up: confined analytical acceptance under operational load — verified

- Acceptance: `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` passed through the delegated user cgroup with the current TinyCC SDK, release library, and Linux x86_64 NixOS glibc 2.42 runtime. The test ran the maintained SQL artifact in the confined executor and completed durable commit.
- Pressure evidence: eight analytical invocations remained active through the measured 30-second interval while 64 operational workers continued reads and commits; 2,599 operational commits completed. The live analytics cgroup reported 13,088 KiB current and 17,432 KiB peak memory.
- Local latency sample: baseline versus concurrent SQL p99 was 2.713 ms versus 78.173 ms for point reads and 26.346 ms versus 298.472 ms for durable commits (64 baseline and 3,022 concurrent samples). The optimizer range query measured 85.1 ms versus 48.6 ms for the reference scan. This is one host/run comparison, not a performance target; the operational tail and range-query overhead remain optimization signals.
- Verification: host cgroup v2 probe reported writable `cpu memory pids` delegation; the focused confined acceptance passed in 39.49 seconds. Then `cargo test --workspace --locked -- --test-threads=1` passed in the delegated user service in 3m54s with sandbox and process tests enabled, including the SQL acceptance; zero tests were ignored.
- Remaining: repeatable multi-run and target-hardware measurements, broader client saturation, and secure spill support remain open.

## SQL-7 follow-up: hidden COUNT(*) in grouped HAVING — implemented

- Contract: [SQL library](../../specs/sql-library.md). V22 adds `COUNT(*)` comparisons when the selected aggregate is another expression, records the hidden-count comparison nodes in a new sized AST/binding, and coerces those operands to BIGINT while retaining v21 coercion for selected-aggregate comparisons.
- Implementation: [`prdb_sdk_sql_v22.h`](../../engine/include/prdb_sdk_sql_v22.h) and [`prdb_sdk_sql_exec_v22.h`](../../engine/include/prdb_sdk_sql_exec_v22.h) add the versioned parser, binder, and runner API. The shared bounded grouped runner validates hidden count parameters before scanning, derives the visible group row count, and evaluates hidden COUNT comparisons with SQL three-valued logic.
- Regression: [`sdk_sql_select_v22.c`](../../engine/tests/fixtures/sdk_sql_select_v22.c) checks a mixed COUNT/SUM predicate, BIGINT coercion for the hidden count literal, Decimal coercion for the selected SUM literal, parameter marking, and v21 rejection. The grouped executor fixture proves the hidden COUNT threshold filters groups independently of SUM values, that COUNT includes rows whose SUM argument is NULL (`COUNT(*) >= 2 OR SUM(score) >= 4.50` keeps an all-NULL SUM group with two visible rows), and that hidden COUNT parameters must be BIGINT before scanning. The v22 ASan/UBSan/libFuzzer oracle validates AST mask membership, failure-output preservation, and separate BIGINT/Decimal literal coercions.
- Verification: TinyCC compiled both modified SDK sources with `-Wall -Wextra -Werror`; the v21/v22 parser-binder fixtures and grouped executor fixture passed. The full pinned-shell SDK `make test` suite passed, including release metadata checks. `make -f engine/sdk/Makefile fuzz-sql-v22` completed 10,000 ASan/UBSan/libFuzzer runs without findings (1,502 coverage counters, 3,354 features).
- Remaining: broader analytical type/workload coverage, secure spill, and mixed-workload measurements remain open.

## SQL-7 follow-up: grouped SELECT through statement dispatcher — implemented

- Contract: [`sql-library.md`](../../specs/sql-library.md). The statement dispatcher accepts supported composite grouped SELECT statements alongside row SELECT and DML, and writes grouped output using the existing SQLR composite result schema.
- Regression: the dispatch fixture first demonstrated that grouped SQL did not reach the planner through `prdb_sdk_sql_run_statement_v1`. The successful scan case now checks a grouped row result for `SELECT id, score, SUM(score) ... GROUP BY id, score HAVING COUNT(*) >= 2 AND SUM(score) >= 4`.
- Implementation: `prdb_sdk_sql_run_v1` recognizes v22 statements and invokes the v22 grouped runner with bounded materialization storage. The result callback reuses the composite grouped SQLR writer. The end-to-end case also exposed and fixed evaluation of selected SUM comparisons when a hidden COUNT comparison appears in the same Boolean HAVING tree.
- Verification: focused `target/sdk-v1/sdk-sql-dispatch-v1` fixture passed. `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed, including the release metadata checks. `git diff --check` passed.
- Remaining: broader analytical type/workload coverage, secure spill, and mixed-workload measurements remain open.

## SDK release linker: keep Nix Clang wrapper paths out of the artifact — fixed

- Finding: the development shell exports `CC=clang`; using that value as the SDK shared linker caused the Nix Clang wrapper to embed libc/GCC RUNPATH entries even after the Makefile cleared `NIX_LDFLAGS`.
- Change: the SDK Makefile now defaults `SHARED_LINKER` to GNU `cc`, as required for the SDK's executable-stack linker flags and release profile. Explicit `SHARED_LINKER` overrides remain supported. The toolchain spec records the separation between Rust's `CC` and the SDK linker.
- Verification: `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed the full maintained TinyCC suite and shared-library metadata tests. `readelf -d` on the resulting SDK library showed only the libc NEEDED entry and no RPATH/RUNPATH.

## SQL-7 / SQL-8 follow-up: v22 grouped dispatcher through confined SQL — verified

- Contract: [`sql-library.md`](../../specs/sql-library.md). The SQL statement dispatcher must carry composite grouped v22 SELECT through the installed SDK, authorized executor scan, SQLR writer, and normal confined result relay.
- Regression: the maintained query artifact now calls `prdb_sdk_sql_run_statement_v1` for `SELECT left_key, right_key, SUM(decimal_value) ... HAVING COUNT(*) >= 2 AND SUM(decimal_value) >= 4.50e0`. The delegated acceptance checks the exact relayed SQLR record for `(2,9,4.50)`.
- Integration finding: the first run used a stale staged SDK release and failed at dispatch. Rebuilding `release-profile` loaded the current dispatcher and produced the expected SQLR, then exposed the artifact's shared 256-request ceiling. The confined fixture now uses the dispatcher query in place of the separate Decimal SUM runner and drops redundant selected-COUNT and Decimal MAX scans; the TinyCC parser/binder/runner suite retains that v21 coverage.
- Verification: `make -f engine/sdk/Makefile sandbox-query-fixture`, `make -f engine/sdk/Makefile release-profile`, and `cargo fmt --all -- --check` passed. The exact `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` test passed inside the delegated `systemd --user` service in 39.47 seconds with the operational-load scenario active.
- Remaining: broader analytical type/workload coverage, secure spill, repeatable pressure measurements, and mixed-workload measurements remain open.

## SQL-7 follow-up: single-key grouped SELECT through statement dispatcher — implemented

- Contract: [`sql-library.md`](../../specs/sql-library.md). The statement dispatcher accepts supported single-key grouped SELECT forms through the existing v10 parser, binder, bounded runner, and single-key SQLR writers.
- Regression: the dispatch fixture first failed for `SELECT id, SUM(score) FROM widgets GROUP BY id`, then passed after routing it to the v10 grouped runner. It checks the emitted SQLR fields, source key value, and widened SUM value.
- Implementation: `prdb_sdk_sql_run_v1` now classifies v22 composite grouping first and then v10 single-key grouping; both paths share bounded materialization storage. The single-key callback selects the existing COUNT, MIN/MAX, SUM, or AVG SQLR writer from the v7 binding.
- Verification: focused dispatch fixture and full TinyCC SDK suite passed. `git diff --check` passed.
- Remaining: broader analytical type/workload coverage, secure spill, repeatable pressure measurements, and mixed-workload measurements remain open.

## SQL-7 follow-up: filtered aliased INNER JOIN through statement dispatcher — implemented

- Contract: [`sql-library.md`](../../specs/sql-library.md). The general statement dispatcher carries supported v20 alias-aware INNER JOIN statements with qualified WHERE comparisons through the existing bounded two-scan runner and SQLR writer.
- Regression: the dispatcher fixture executes a self-join with two equality keys, a typed qualified filter parameter, and LIMIT. It checks both relation descriptions and scans, then verifies the emitted SQLR projection values.
- Implementation: `prdb_sdk_sql_run_v1` recognizes v20 JOIN statements before the single-key grouped and row SELECT routes, allocates disjoint bounded two-relation response, shape, row, and materialization buffers, and reuses the established joined SQLR writer.
- Confined acceptance: the maintained `sql_v3_write_read.c` artifact now routes its parameterized v20 filtered join through `prdb_sdk_sql_run_statement_v1`. The delegated `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` test passed and checked the exact relayed SQLR frame before durable commit.
- Verification: `make -f engine/sdk/Makefile test`, `make -f engine/sdk/Makefile sandbox-query-fixture release-profile`, the focused dispatch fixture, and `git diff --check` passed. The confined acceptance passed inside the delegated `systemd --user` service in 39.00 seconds.
- Remaining: broader analytical type/workload coverage, secure spill, repeatable pressure measurements, and mixed-workload measurements remain open.


## SQL-7 follow-up: single-key INNER JOIN through statement dispatcher — implemented

- Contract: [`sql-library.md`](../../specs/sql-library.md). The SQL statement dispatcher accepts v8 single-key INNER JOIN statements through the existing planner, bounded two-scan executor, and joined SQLR writer.
- Regression: the dispatcher fixture first demonstrated that a v8 JOIN did not reach planner resolution through `prdb_sdk_sql_run_statement_v1`; it now checks independent resolution and scans for both relations and the exact SQLR row.
- Implementation: `prdb_sdk_sql_run_v1` classifies the v8 form after v20 JOIN detection, allocates separate bounded planner/executor response and relation-shape buffers, and routes result rows through the established joined SQLR writer. The confined write/read fixture now uses the standard statement dispatcher for its linked-table JOIN.
- Verification: `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` and `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile sandbox-query-fixture release-profile` passed. The exact `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` test passed inside the delegated `systemd --user` service in 39.25 seconds, checking the dispatched join result through the confined relay.
- Remaining: broader analytical type/workload coverage, secure spill, repeatable pressure measurements, and mixed-workload measurements remain open.

## SQL-7 follow-up: aggregate scalar type matrix — verified

- Contract: [`sql-library.md`](../../specs/sql-library.md). The bounded MIN/MAX kernel supports all declared scalar types; grouping uses the same typed comparator and Decimal scale rules.
- Regression: `sdk_sql_group_v1.c` now exercises MIN and MAX for BOOLEAN, SMALLINT, INTEGER, BIGINT, DECIMAL, TEXT, VARCHAR, BYTEA, UUID, DATE, and TIMESTAMPTZ. It checks signed numeric encodings, UTF-8 and binary ordering, fixed-width UUID and temporal ordering, NULL omission from MIN/MAX, and grouping equal values into one stable group for each type.
- Verification: focused `sdk-sql-group-v1` fixture passed with the pinned TinyCC toolchain. `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed the complete maintained SDK suite, including ABI and release metadata checks; `git diff --check` passed.
- Remaining: broader analytical type/workload coverage, secure spill, repeatable pressure measurements, and mixed-workload measurements remain open.

## SQL-8 follow-up: five-run confined analytical pressure sample — verified

- Setup: ran `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` five times sequentially in one delegated `systemd --user` service, with a fresh test database per run, the current TinyCC artifact and installed SDK release, eight simultaneous analytical invocations, 64 operational threads, and the built-in 30-second saturation interval. Every run passed, including SQL relay, cancellation, result backpressure, and durable commit checks.
- Measurements: the test's 64-sample operational-only baseline and roughly 2,700–3,000 concurrent samples report p99 values in microseconds. The five repeated runs below measured the five-row `grouped_records` fixture (two matching rows), not a representative large-table benchmark.

| Run | Operational commits / 30 s | Analytical cgroup current / peak (KiB) | Point read p99 baseline / concurrent (µs) | Commit p99 baseline / concurrent (µs) | Optimizer / reference range (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 2,603 | 13,648 / 17,952 | 3,580 / 73,325 | 28,317 / 331,032 | 90.1 / 56.9 |
| 2 | 2,406 | 13,396 / 17,516 | 5,891 / 83,457 | 31,998 / 318,433 | 87.9 / 54.5 |
| 3 | 2,378 | 13,152 / 17,608 | 3,847 / 77,592 | 20,398 / 293,987 | 69.8 / 51.1 |
| 4 | 2,426 | 13,148 / 17,400 | 3,795 / 83,633 | 32,034 / 347,047 | 50.4 / 23.6 |
| 5 | 2,414 | 13,240 / 17,280 | 4,043 / 88,441 | 24,267 / 356,153 | 91.7 / 72.1 |

- Finding: all five runs sustained the required operational work and stayed far below the configured 256 MiB per-worker ceiling. Concurrent point-read and durable-commit p99s were substantially higher than their 64-sample baselines. The optimizer range path was slower on this five-row fixture, but the fixed relation-resolution and IPC work can dominate such a tiny population; these timings do not establish a large-table range performance result. None of the local samples establishes hardware-independent targets.
- Initial follow-up run: the confined acceptance compared an indexed range on a 1,024-row table against the reference scan, returning and validating eight duplicate keys from the first 16-row page. One successful run measured 87.6 ms optimizer versus 64.5 ms reference, with 2,386 operational commits in 30 seconds, analytical cgroup current/peak of 13,264/17,136 KiB, point-read p99 of 4.157/78.047 ms baseline/concurrent, and durable-commit p99 of 24.339/306.114 ms. This is an early-page bounded range comparison; it does not measure a deep range or establish a performance target. The confined acceptance passed after the added query stayed within the shared request budget.
- Follow-up: moved the duplicate range to key 3 (rows 24–31) and a 32-row page. The confined acceptance passed and measured 77.5 ms optimizer versus 141.2 ms reference in one run, with 2,424 operational commits in 30 seconds, analytical cgroup current/peak of 13,996/17,388 KiB, point-read p99 of 3.942/85.594 ms baseline/concurrent, and durable-commit p99 of 24.082/348.115 ms. A 128-row probe later exceeded the same invocation's scan budget at an existing optimizer operation, so the acceptance keeps this range comparison at 32 candidates. These local single-run results do not establish stable relative performance or a target.
- Follow-up deep-key comparison: changed the optimizer/reference range to key 10 on the 1,024-row table, returning all eight duplicates from a range whose reference scan must cross multiple 32-row pages. The confined acceptance passed with 2,231 operational commits in 30 seconds; analytical cgroup current/peak was 13,492/17,172 KiB; point-read p99 was 6.567/79.842 ms baseline/concurrent; durable-commit p99 was 66.688/316.945 ms; and optimizer/reference range latency was 128.9/314.4 ms. These are one-run measurements under concurrent load, not performance targets. A key-120 probe returned correct optimizer values but exceeded the invocation's cumulative reference scan budget, so it is not part of the acceptance.
- Remaining: target-hardware measurements, broader client saturation, deeper range selectivity and pagination measurements, analytical workload diversity, and secure spill remain open.

## SQL-7 follow-up: integral SUM and AVG input matrix — verified

- Contract: [`sql-library.md`](../../specs/sql-library.md). SUM accepts SMALLINT, INTEGER, and BIGINT with BIGINT results; AVG accepts the same types and returns DECIMAL(38,6).
- Regression: the maintained aggregate fixture now sums `2 + 4` for each integral input type and checks widened BIGINT `6`; it averages the same values and checks the exact DECIMAL coefficient for `3.000000` and result scale 6. Existing Decimal SUM and AVG cases continue to cover Decimal precision/scale handling.
- Verification: focused `sdk-sql-group-v1` and the complete pinned-toolchain `make -f engine/sdk/Makefile test` suite passed; `git diff --check` passed.
- Remaining: broader analytical type/workload coverage, secure spill, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-7 follow-up: ORDER BY scalar comparator matrix — verified

- Contract: [`sql-library.md`](../../specs/sql-library.md) defines signed fixed-width numeric comparison, Decimal scale-aware comparison, Boolean order, UTF-8 text order, and bytewise UUID/BYTEA order for SQL sorting.
- Regression: the maintained stable-order fixture now checks ascending and descending permutations for BOOLEAN, SMALLINT, INTEGER, BIGINT, DECIMAL, TEXT, VARCHAR, BYTEA, UUID, DATE, and TIMESTAMPTZ values. The cases include signed extrema, negative Decimal, multibyte UTF-8, and binary/fixed-width values.
- Verification: focused `sdk-sql-order-v1` fixture and `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed with the pinned TinyCC toolchain; `git diff --check` passed.
- Remaining: broader analytical type/workload coverage remains open.

## SQL-7 follow-up: grouped ORDER BY and SQLR across scalar types — verified

- Contract: `sql-library.md` requires typed stable ordering for supported GROUP BY keys and preserves each key's declared type metadata in SQLR output.
- Regression: `sdk-sql-execute-v10` now runs the v12 grouped COUNT runner for ascending and descending `ORDER BY id` over all 11 scalar types. It supplies keys in unsorted order, checks the callback's exact sorted key sequence, and compares each encoded group-count SQLR row byte-for-byte with an independently constructed expected record, including Decimal precision/scale and source type metadata.
- Verification: focused `sdk-sql-execute-v10` fixture and the complete pinned-toolchain `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` suite passed; `git diff --check` passed.
- Remaining: broader analytical workload coverage, secure spill, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-7 follow-up: composite aggregate optimizer wrapper matrix — verified

- Contract: `sql-library.md` defines optimizer-gated composite grouping for COUNT, MIN, MAX, SUM, and AVG. The SDK must validate the returned plan against the bound query and re-resolve the planned relation before execution.
- Regression: `sdk_sql_execute_v10.c` now drives the public v13 optimizer wrapper through mock optimizer and executor callbacks for all five aggregate kinds. Each case checks the encoded request kind, a kind-specific plan, plan matching, plan-relation identity/column metadata re-resolution, grouped callback values, and row counts.
- Verification: the focused `sdk-sql-execute-v10` fixture and the complete pinned-shell `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` suite passed; `git diff --check` passed.
- Remaining: confined process-level end-to-end optimizer coverage still exercises SUM only; broader analytical workload coverage, secure spill, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-7 follow-up: confined composite COUNT optimizer execution — verified

- Change: the confined write/read artifact now runs its existing two-key v13 `COUNT(*)` case through `prdb_sdk_sql_execute_group_composite_aggregate_optimizer_v1` (plan kind 13) instead of the separate composite-count plan path. This keeps the same query and SQLR assertion while exercising the real optimizer process, AST/plan validation, executor-side relation re-resolution, authorized scan, and result relay.
- Verification: rebuilt `sandbox-query-fixture` and `release-profile`; the focused `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed in the delegated cgroup service, including its live mixed-load checks and durable commit. The full serial `cargo test --workspace --locked -- --test-threads=1` suite also passed in the delegated service.
- Remaining: confined process-level optimizer-backed composite aggregate coverage now includes COUNT and SUM; MIN, MAX, and AVG remain covered by SDK wrapper tests but not this process acceptance. Broader analytical workload coverage, secure spill, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-7 follow-up: confined aggregate matrix request budget — measured

- Experiment: added optimizer-backed composite MIN, MAX, and AVG scans to the same confined write/read invocation, with callback value checks and exact SQLR expectations. All six MIN/MAX/AVG result frames were relayed and matched their expected values.
- Boundary: the later optimizer-backed DELETE then failed with `Executor(RequestLimitExceeded)` because the invocation had consumed its cumulative executor request allowance. The added scans were removed from this single-invocation fixture; the previously passing COUNT/SUM process acceptance remains intact.
- Next: place the additional aggregate process cases in a separate invocation with its own executor connection budget, or identify and measure enough redundant work to remove without weakening existing acceptance coverage.

## SQL-7 follow-up: confined composite MIN, MAX, and AVG optimizer execution — verified

- Change: the confined write/read artifact accepts a dedicated `A` parameter mode that runs optimizer-backed composite MIN, MAX, and AVG queries in a second invocation after the main DML invocation commits. The result callback checks the committed two-key groups and counts; the process acceptance compares all six relayed SQLR frames, including exact DECIMAL(38,6) AVG coefficients, and confirms the second invocation's durable committed outcome.
- Integration finding: the primary DML sequence changes the original `(1,7)` group to `(4,9)` before commit. The follow-up invocation validates the committed groups `(4,9)` with two rows and `(2,9)` with three rows. The metadata acceptance now recognizes `analytics_range_benchmark_records`, which the same fixture already creates for its bounded range comparison.
- Verification: `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile sandbox-query-fixture release-profile`, the complete maintained SDK `make ... test` suite, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `git diff --check` passed. The focused `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` process acceptance passed inside the delegated user service, including mixed-load, cancellation, relay, and durable outcome checks. The full serial workspace suite reached `metadata_process` and failed `authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes` while reconciling a lost package result; the exact test also failed on an isolated delegated retry. That test and its implementation were not changed in this increment. Full suite acceptance remains open.
- Remaining: broader analytical workload coverage, secure spill, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-7 follow-up: confined optimizer-backed Decimal aggregate matrix — verified

- Change: the separate confined aggregate invocation runs composite Decimal MIN, MAX, SUM, and AVG through the plan-kind-13 optimizer path over `decimal_grouped_records`. The callback derives the result writer's source-shape width from the bound group and aggregate ordinals, so a three-column relation is encoded correctly even though only three projected SQLR fields are emitted.
- Regression: the real process acceptance compares exact SQLR values and metadata for all four Decimal aggregates: MIN `1.00`/`1.00` at `(4,2)`, MAX `1.00`/`2.25` at `(4,2)`, SUM `1.00`/`4.50` at `(38,2)`, and AVG `1.00000000`/`1.50000000` at `(38,8)`, alongside the existing integer MIN/MAX/AVG frames. This covers non-integer optimizer aggregate inputs through optimizer request, plan matching, executor re-resolution, authorized scan, SQLR serialization, and relay.
- Verification: the updated TinyCC `sandbox-query-fixture` built successfully; `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `git diff --check` passed. The expanded `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed in focused and full-suite delegated runs. The full serial workspace suite also passed after correcting the metadata process proxy to drop a streamed result-data frame while draining the gateway stream; this keeps the package alive so PACKAGE_STATUS can observe its completed, result-lost outcome. The isolated metadata process test and full workspace suite both pass with this correction.
- Remaining: broader analytical workload coverage, secure spill, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-8 follow-up: bounded grouped SUM under live operational pressure — verified

- Workload: the confined pressure query now runs the maintained v6 grouped SUM runner over 128 authorized rows, forming 64 duplicate text-key groups. Each group has two integer measures and the callback checks the exact widened BIGINT sum `300 + 2 * group_key`. The 8,172-byte group key plus 4-byte operand across all rows uses 1,046,528 copied bytes, near the runner's 1 MiB materialization limit while leaving room for tuple encoding in the storage scan budget.
- Concurrency: the aggregate still holds eight analytical invocations active while 64 operational workers perform reads and durable commits. The group callback coordinates the overlapping index build, maintains the 30-second mixed-load window, and then validates all 64 group results before the query can commit.
- Verification: `make -f engine/sdk/Makefile sandbox-query-fixture release-profile`, the focused confined acceptance, formatting, warning-denied Clippy, `git diff --check`, and the full serial locked workspace suite passed. The focused run completed 2,288 operational commits in 30 seconds, measured analytical cgroup current/peak at 12,876/17,424 KiB, and observed point-read p99 of 5.209/82.031 ms and durable-commit p99 of 29.089/391.470 ms for baseline/concurrent samples. These are one-run local measurements, not performance targets.
- Remaining: broader analytical workload diversity, secure spill, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-7 follow-up: confined optimizer-backed integral aggregate matrix — verified

- Change: the TinyCC fixture adds a `B` mode that runs composite SUM, MIN, MAX, and AVG over BIGINT and SMALLINT inputs through optimizer plan matching, executor-side relation re-resolution, the authorized scan, and the SQLR callback. The Rust acceptance creates a five-row numeric relation with both integral widths after the primary DML invocation and gives `B` a separate staging scope and executor connection. The generated UUID `DEFAULT` insert also runs in `B`, keeping it within its own scan-work budget.
- Workload: the nullable BIGINT measure contains one NULL in the `(1,7)` group. That group still counts two visible rows, while SUM, MIN, MAX, and AVG all return the remaining value `2`; the `(2,9)` group checks non-NULL values. This verifies SQL NULL skipping through the authorized confined scan and optimizer path.
- Authorization: the new principal credential retains its catalog-valid 60-second expiry. The aggregate principal receives row SELECT and per-column cell SELECT policies on all four numeric columns; without cell policies the confined aggregate correctly returned `EVALUATION_REDACTED_INPUT`. Policy audit event IDs are unique.
- Verification: the focused `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed in the delegated user service and checked exact SQLR frames for SUM, MIN, MAX, and AVG across both groups, plus the durable committed result. The focused acceptance passed with both numeric input types. The full serial `cargo test --workspace --locked -- --test-threads=1` suite passed against the expanded integral matrix in the delegated service. The updated TinyCC fixture built, and formatting, warning-denied Clippy, and `git diff --check` passed. SMALLINT SUM widens to BIGINT, MIN/MAX retain SMALLINT, and AVG emits DECIMAL(38,6). The initial sandbox worker-start failures came from stale runtime library paths; using the exact loader, libc, and libgcc paths from the locked Nix shell fixed the launch.
- Remaining: broader analytical workload coverage, secure spill, target-hardware measurements, and mixed-workload optimization remain open.


## SQL-7 follow-up: Decimal aggregate NULL handling through confined SQL — verified

- Change: the confined `decimal_grouped_records` relation now permits a NULL aggregate input. The `(1,7)` group has two visible rows with one NULL Decimal measure; the `(2,9)` group retains three non-NULL measures.
- Regression: optimizer-backed MIN, MAX, SUM, and AVG ignore the NULL measure while preserving the two-row group count. Exact relayed SQLR values are `1.00`, `1.00`, `1.00`, and `1.00000000` for the `(1,7)` group; prior non-NULL values for `(2,9)` remain unchanged.
- Verification: the focused `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance and full serial delegated-cgroup workspace suite passed after this Decimal NULL extension. The TinyCC fixture build, formatting, warning-denied Clippy, and `git diff --check` passed.

## SQL-7 follow-up: composite INNER JOIN rejects NULL key matches — verified

- Contract: every equality in a composite INNER JOIN must evaluate to TRUE; a NULL comparison yields UNKNOWN, so two rows with NULL in the same join-key position do not match.
- Regression: the maintained v19 executor fixture now supplies both relations with the same non-NULL first key and NULL second key, then runs a two-key join. It verifies zero callbacks, both scans closed, and no statement failure. The fixture helper retains the existing 129-row malformed response case.
- Verification: focused `sdk-sql-execute-v19` and complete `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed under the pinned TinyCC toolchain; `git diff --check` passed.
- Remaining: broader analytical workload diversity, secure spill, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-7 follow-up: filtered join preserves NULL as UNKNOWN — verified

- Contract: a joined row with a NULL filter operand does not pass a comparison WHERE predicate; NULL remains distinct from FALSE and REDACTED.
- Regression: the maintained v20 executor fixture creates a row pair that satisfies the composite join keys but has NULL in the filtered right-side value. A parameterized `>=` filter emits no callback, closes both scans, and leaves the statement successful. The existing REDACTED-input case continues to require statement failure.
- Verification: focused `sdk-sql-execute-v19` and complete `nix develop --no-update-lock-file -c make -f engine/sdk/Makefile test` passed under the pinned TinyCC toolchain; `git diff --check` passed.
- Remaining: broader analytical workload diversity, secure spill, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-7 follow-up: confined optimizer-backed filtered MIN(TEXT) — verified

- Change: the separate confined aggregate invocation now filters the existing 128-row pressure relation by a typed INTEGER parameter and computes composite `MIN(group_key)` over the visible large TEXT value. The callback checks the two group keys, TEXT aggregate input/result, exact 8,172-byte key contents, and one matching row.
- Result contract: the process acceptance independently constructs the three-column SQLR row and checks its bytes after reassembling the two bounded gateway result chunks. This exercises optimizer request validation, executor relation re-resolution, authorized filtered scan, TEXT comparison/aggregation, SQLR encoding, and result relay in one confined invocation.
- Packaging: the additional confined case brought the maintained dynamically linked fixture to 157,160 bytes, above its previous 153,000-byte guard. The guard is now 160,000 bytes; the fixture reuses existing bounded buffers so the query artifact's static memory footprint does not grow for this case.
- Verification: `make -f engine/sdk/Makefile test`, `make ... release-profile`, workspace build, format check, warning-denied Clippy, and `git diff --check` passed. The focused `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed inside the delegated user service, and the full serial locked workspace suite passed there as well. The acceptance confirms durable COMMITTED status and exact SQLR output.
- Remaining: broader analytical workload coverage, secure spill, target-hardware measurements, and mixed-workload optimization remain open.

## SQL-7 follow-up: aggregate NULL tests in compound HAVING (v23) — verified

- Contract: [SQL library](../../specs/sql-library.md), operational and analytical execution. V23 retains composite grouping and adds `IS NULL` / `IS NOT NULL` on the selected aggregate and hidden COUNT(*); comparisons, Boolean truth rules, ordering, LIMIT, and existing bounds remain supported. Earlier versioned parser/binder APIs continue rejecting these leaves.
- Observed red: the maintained `sdk-sql-select-v23` fixture built with pinned TinyCC, then failed its parse-success assertion for `HAVING SUM(amount) IS NULL` while the initial v23 shim delegated to v22. The implemented parser, binder, runner, and general statement dispatcher now accept the query.
- Regression evidence: parser/binder fixtures cover Decimal SUM, TEXT MIN, hidden and selected COUNT(*), malformed suffixes, unchanged outputs on parse failure, invalid NULL-test fields, and oversized node counts. The maintained grouped executor fixture covers all-NULL versus non-NULL SUM/AVG/MIN/MAX, both NULL-test operators, NOT/AND/OR with UNKNOWN comparisons, hidden COUNT(*) after NULL-test leaves, ordering/LIMIT, and redacted input failure before callbacks. Static and shared-library parser fixtures run in the normal SDK suite. A new sanitizer-enabled v23 fuzz harness completed 10,000 runs with seed 23 without findings.
- Confined evidence: the primary dispatcher query retains its exact `4.50` sum while requiring non-NULL COUNT(*) and SUM. The separate `A` invocation filters the committed all-NULL `(3,11)` group with compound HAVING and emits a NULL DECIMAL(38,2) sum; the gateway checks the exact SQLR frame and durable COMMITTED result. The focused `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` acceptance passed inside `prdb-sql23-acceptance.service` with delegated `cpu memory pids`, including the existing mixed-load, cancellation, and redaction branches.
- Checks completed: pinned Rust 1.95.0, rustfmt 1.9.0, Clippy 0.1.95, TinyCC 0.9.27-unstable-2025-01-06, GCC 15.3.0; host capability probe passed; `make -f engine/sdk/Makefile test sandbox-query-fixture`, release-profile build/validation, `cargo build --workspace --locked`, `cargo fmt --all -- --check`, warning-denied workspace/all-target Clippy, local documentation links, and `git diff --check` passed. This Nix version rejects `nix develop --locked`; the equivalent locked-input shell used `nix develop --no-update-lock-file`. The full `cargo test --workspace --locked -- --test-threads=1` suite passed in `prdb-sql23-workspace.service`, including all confined and process tests.
- Remaining: v23 uses the bounded reference aggregate runner and independently described/re-resolved authorized relation; no new optimizer plan kind is introduced. Secure spill, broader analytical workloads, and target-hardware performance gates remain open. Next smallest behavior: validate aggregate NULL tests under confined redacted-input failure, then continue broader SQL/type coverage.

## SQL-7 follow-up: confined denial of a redacted HAVING input — verified

- A restricted Decimal input reaches the v23 grouped runner through the confined host. The HAVING tree would exclude every group through `COUNT(*) IS NULL`. The runner must still return the precise evaluation error for the required redacted SUM input before reaching any group callback.
- The query catches the error and returns success. The host latch forces a worker failure, the gateway receives exactly one ABORTED terminal frame and no result frames, and the owner-scoped storage lookup confirms the durable abort. The existing redacted aggregate and WHERE NULL cases run through the same acceptance loop.
- The pinned TinyCC fixture built below the existing 160,000-byte package bound. Workspace build, formatting, Clippy with warnings denied, and the full serial locked Rust workspace suite passed. The latter ran in the `prdb-sql23-denial.service` delegated user unit, including the sandboxed SQL acceptance.

## SQL-7 follow-up: compound HAVING over a single grouping key (v24) — verified

- V24 extends the bounded grouped parser, binder, and runner to one through eight keys. The general dispatcher routes v24 through the existing tuple grouping and HAVING evaluation and uses the existing single-key SQLR writers when the query selects one group key.
- The first TinyCC v24 regression failed on the one-key parse assertion while the entry point still used v23's two-key minimum. The parser/binder fixture now passes with typed WHERE, compound HAVING, two ORDER BY keys, and LIMIT and verifies that v23 rejects a one-key v24 AST.
- The confined Decimal query exposed a separate parameter byte-order mismatch. PSP1 numeric payloads from the client are little endian and SDK evaluators and executor encoders read big endian. The dispatcher now normalizes those payloads before binding while the public PSP1 decoder keeps its borrowed view. With canonical bytes, the maintained JOIN fixture previously emitted no results. It now finds its expected row, and the INSERT fixture verifies the exact signed INTEGER bytes passed to the executor.
- The confined acceptance binds Decimal `1.25` in WHERE, preserves NULL inputs, and compares the single projected key and exact DECIMAL(38,2) sum `3.50` from the dispatcher. The original run failed at PSP1 decode when the fixture used the SDK's byte order. The corrected confined run returned the expected row and durably committed. The interactive gateway test also now checks numeric SQLR values instead of the previously swapped bytes.
- The complete TinyCC SDK suite, sandbox fixture build, release-profile validation, locked workspace build, rustfmt check, and warning-denied Clippy passed. The v24 parser/binder sanitizer harness completed 10,000 cases with seed 24.
- The full serial workspace suite with `--no-fail-fast` passed in the delegated `prdb-sql24-complete-check.service` run with 1,040 tests passed and zero ignored. Earlier runs recorded a catalog race returning `Commit(InvalidCatalog)` and a gateway-restart connection refusal. Each passed in isolation and in the final full run.
- Follow-up: match PSP1's declared parameter type and Decimal scale against each bound grouped WHERE and HAVING operand, then extend optimizer and mixed-workload coverage.


## SQL-7 follow-up: type declarations in grouped parameters — in progress

- Grouped dispatch carries PSP1 metadata into the v24 runner and the legacy v10 fallback. Every referenced WHERE and HAVING comparison requires the declared scalar type and exact Decimal scale of its bound operand before opening a scan. Hidden COUNT(*) expects BIGINT. The validation also applies to typed NULL, repeated placeholders, short-circuited branches, and LIMIT 0. Row SELECT and grouped runners share the parameter binding check.
- The initial TinyCC dispatcher fixture accepted a wrong-scale Decimal WHERE parameter under LIMIT 0. It now rejects mismatched WHERE and HAVING declarations before OPEN_SCAN or SQLR output, including a typed NULL with a mismatched declaration and one placeholder used against WHERE and AVG operands with different scales. Matching Decimal scale with smaller declared precision and matching typed NULLs succeed.
- The confined fixture uses the same typed parameter in WHERE and HAVING and checks the existing DECIMAL(38,2) sum of `3.50`. The mismatch invocation changes only the declaration's scale, before installing a redaction policy. The fixture catches the SQL error and the acceptance requires one terminal ABORTED frame, zero SQLR frames, and a durable ABORTED outcome.
- The pinned TinyCC SDK suite, shared SDK checks, fixture packaging, release profile, locked Rust build, rustfmt and warning-denied Clippy passed. The full serial workspace suite is running in the delegated user service.
- Follow-up: extend the optimizer request and plan contract to cover compound HAVING while keeping evaluation over authorized grouped results.
