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
| SQL-7 | Small authorized-view join/group/sort fixtures match independently calculated results | Add required inner equijoins, GROUP BY, COUNT/SUM/MIN/MAX/AVG, ORDER BY and HAVING one operator at a time with optimizer support and precise null/overflow/redaction rules |
| SQL-8 | Hash/sort/aggregate budget exhaustion and cancellation fail without uncontrolled spill or partial commit | Bounded operator memory/output; COUNT(*) counts visible rows; required redacted operands fail rather than silently disappearing |

## Acceptance and Handoff

Maintain a feature/type/operator matrix against specs and compare interpreter results/errors with small independent authorized-view fixtures. Fuzz the C parser separately from Rust query/plan decoders. C unit executables are maintained SDK tests; query artifacts run only through the sandbox harness. Initial SQL completion does not complete analytical support. First behavior: one supported SELECT parse fixture and one unsupported-statement error, once grammar and bounds are specified.
