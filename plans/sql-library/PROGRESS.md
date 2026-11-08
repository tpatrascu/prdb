# SQL Library Progress

## SQL-1 first bounded SELECT parser — implemented

- Contract: [`prdb_sdk_sql_v1.h`](../../engine/include/prdb_sdk_sql_v1.h) defines a sized/versioned borrowed-span AST for explicit column projections or `*`, one unqualified relation, one comparison against `$n`, optional LIMIT and one optional semicolon. Keywords are case-insensitive; identifier source spelling is preserved.
- Bounds and rejection: query 65,536 bytes; identifier 128 bytes; projection 64 columns; parameter index 1–65,535; LIMIT 0–1,000,000. Unsupported statement kinds return `UNSUPPORTED_STATEMENT`; malformed/incrementally unsupported syntax returns `SYNTAX_ERROR`; bounds return `LIMIT_EXCEEDED`. The parser copies no SQL or names and does not bind parameters or execute a plan.
- Implementation: [`sql_v1.c`](../../engine/sdk/sql_v1.c) performs bounded tokenization and parsing, stores source offsets/lengths, and is included in the TinyCC SDK archive.
- Regression: [`sdk_sql_select_v1.c`](../../engine/tests/fixtures/sdk_sql_select_v1.c) checks the documented parameterized SELECT, case-insensitive keywords, preserved identifier bytes, star projection, LIMIT 0, DELETE rejection, malformed SELECT, second-statement rejection, too many columns, query length, parameter range, and LIMIT range. Initial TinyCC compile failed because `prdb_sdk_sql_v1.h` was absent; the fixture passes after implementation.
- Remaining: bind and type-check parameters against authorized schema, submit a bounded structured request to the planner, implement expression semantics and storage-backed SELECT execution, and add mutations only after statement-abort behavior is wired end to end.

## SQL-2 typed scalar comparison and truth primitives — implemented

- Contract: comparisons require matching scalar type codes; Decimal additionally requires equal scales. SQL NULL produces UNKNOWN. REDACTED produces an error for comparison and `IS NULL`. Boolean, signed fixed-width scalars, exact decimals, valid UTF-8 text, bytea and UUID follow the ordering specified in [`sql-library.md`](../../specs/sql-library.md). The C truth helpers implement SQL three-valued truth tables; expression traversal and left-to-right short-circuit orchestration remain separate work.
- Implementation: the SQL ABI exposes checked comparison, `IS NULL`, Boolean-to-truth conversion and pure NOT/AND/OR helpers. Decimal scale is supplied by a later binder; this primitive rejects mismatched or out-of-range scale metadata.
- Regression: [`sdk_sql_expression_v1.c`](../../engine/tests/fixtures/sdk_sql_expression_v1.c) uses hand-evaluated integer, text and decimal examples, and checks NULL, REDACTED, type mismatch and truth-table behavior. The fixture first failed to compile because `prdb_sdk_sql_compare_v1` was missing and now passes against the TinyCC SDK archive.
- Remaining: parse Boolean/`IS NULL` expressions, bind schema and typed parameters, evaluate expression trees with bounded left-to-right short-circuiting, then plan and execute SELECT over the scoped storage channel.
