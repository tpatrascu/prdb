# SQL Library Implementation Plan

## Contract and Dependencies

Implement the C [SQL frontend/interpreter](../../specs/sql-library.md) in the prebuilt TinyCC SDK archive. Depend on [SDK/ABI](../client-sdk/README.md), [optimizer](../planner-compiler/README.md), [authorized storage](../authorization/README.md), [transactions](../concurrency-mvcc/README.md) and [schema types](../ddl-schema/README.md). No SQL/native query code runs inside trusted storage.

## Decisions Before Implementation

Specify grammar/parameter syntax, AST and physical-plan formats, SQL API and result encoding. Fix typed coercions, overflow/decimal/date semantics, Boolean/null logic, evaluation/error order and redaction behavior before rewrites/operators rely on them. Initial syntax is single-table SELECT/WHERE/LIMIT and INSERT/UPDATE/DELETE; reject DDL, explicit transaction control, unsupported syntax and reentrant/concurrent statements.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| SQL-1 | Maintained C parser fixtures distinguish supported SELECT from invalid/unsupported syntax and bounded AST exhaustion | Bounded tokenizer/parser and typed parameters; no SQL values can select identity or authority |
| SQL-2 | Hand-evaluated expressions distinguish NULL from REDACTED and reject redacted required operands | Checked typed scalar evaluator with explicit evaluation/error rules; projection preserves redaction; validate against schema's full scalar matrix incrementally |
| SQL-3 | A real SQL call submits a bounded request, rejects malformed plans and resolves logical references on its own connection | Versioned planner bridge and reference scan/filter/project/limit interpreter; never reuse optimizer-local handles |
| SQL-4 | Sequential read/write/read sees own writes; UPDATE does not revisit changed targets | INSERT/UPDATE/DELETE with fixed bounded target/value materialization, close input scans before staging and forbid previous live result streams |
| SQL-5 | A late error invalidates partial results and earlier writes even if C code catches it | Terminal statement failure propagated to trusted transaction state, explicit result lifetimes, no per-statement commit or recovery |
| SQL-6 | The three fixed information_schema relations match filtered discovery and reject mutations | Use storage virtual scans; SDK predicates never inspect unfiltered catalog rows |
| SQL-7 | Small authorized-view join/group/sort fixtures match independently calculated results | Add required inner equijoins (including aliases and qualified filters), GROUP BY, COUNT/SUM/MIN/MAX/AVG, ORDER BY and HAVING one operator at a time with optimizer support and precise null/overflow/redaction rules |
| SQL-8 | Hash/sort/aggregate budget exhaustion and cancellation fail without uncontrolled spill or partial commit | Bounded operator memory/output; COUNT(*) counts visible rows; required redacted operands fail rather than silently disappearing |

SQL-7's HAVING type coverage proceeds through separately versioned SDK increments. V21 adds exact signed integer, fractional, and exponent literals for compound grouped HAVING, with decimal scale coercion checked before scans; earlier parser and binding ABIs remain frozen. V23 adds selected-aggregate and hidden COUNT(*) `IS [NOT] NULL` leaves to compound HAVING while preserving earlier parser/binder behavior. V24 extends the same HAVING behavior to a single grouping key. The typed grouped runners bind PSP1 operand types and Decimal scales before scanning. Acceptance evidence belongs in [`PROGRESS.md`](PROGRESS.md), and broader analytical type/workload and spill evidence remains open. Administrative SQL DDL is owned by the schema DDL plan and uses the separate authenticated control client; it is not added to the query-invocation SQL dispatcher.

## Acceptance and Handoff

Maintain a feature/type/operator matrix against specs and compare interpreter results/errors with small independent authorized-view fixtures. Fuzz the C parser separately from Rust query/plan decoders. C unit executables are maintained SDK tests; query artifacts run only through the sandbox harness. Initial SQL completion does not complete analytical support. First behavior: one supported SELECT parse fixture and one unsupported-statement error, once grammar and bounds are specified.

### Analytical test coverage

| Behavior | Maintained evidence | Open work |
| --- | --- | --- |
| Inner equijoins | [SDK pair and executor fixtures](../../engine/tests/fixtures/sdk_sql_execute_v19.c) cover aliased and filtered composite joins, NULL handling, and bounded output. [Confined acceptance](../../engine/tests/commit.rs) compares relayed SQLR rows. | Broader mixed-workload measurements with joins. |
| Group keys and COUNT(*) | [Grouping fixture](../../engine/tests/fixtures/sdk_sql_group_v1.c) checks stable groups and NULL keys. [Grouped runner](../../engine/tests/fixtures/sdk_sql_execute_v10.c) checks scalar grouping and SQLR. The confined suite checks counts over visible rows. | Broader workloads and optimizer coverage for compound HAVING. |
| MIN/MAX/SUM/AVG | The [grouped runner fixture](../../engine/tests/fixtures/sdk_sql_execute_v10.c) covers Decimal, integral and other comparable operands, result metadata, overflow, NULL and redaction. The confined suite covers Decimal and integral results and filtered TEXT MIN. | More confined cases across the scalar matrix. |
| ORDER BY and HAVING | The [sort kernel](../../engine/tests/fixtures/sdk_sql_order_v1.c) checks stable order and NULL placement. The [dispatcher fixture](../../engine/tests/fixtures/sdk_sql_dispatch_v1.c) checks single-key HAVING and exact SQLR. The confined suite checks the composition of WHERE, HAVING, ORDER BY and LIMIT. | Optimizer coverage for compound HAVING. |
| Operator budgets and cancellation | The SDK join and grouping fixtures exercise request and materialization ceilings. The confined suite exercises cancellation and mixed operational work. | Representative larger datasets and repeatable measurements on target hardware. |
