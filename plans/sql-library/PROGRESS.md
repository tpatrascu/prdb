# SQL Library Progress

## SQL-1 first bounded SELECT parser — implemented

- Contract: [`prdb_sdk_sql_v1.h`](../../engine/include/prdb_sdk_sql_v1.h) defines a sized/versioned borrowed-span AST for explicit column projections or `*`, one unqualified relation, one comparison against `$n`, optional LIMIT and one optional semicolon. Keywords are case-insensitive; identifier source spelling is preserved.
- Bounds and rejection: query 65,536 bytes; identifier 128 bytes; projection 64 columns; parameter index 1–65,535; LIMIT 0–1,000,000. Unsupported statement kinds return `UNSUPPORTED_STATEMENT`; malformed/incrementally unsupported syntax returns `SYNTAX_ERROR`; bounds return `LIMIT_EXCEEDED`. The parser copies no SQL or names and does not bind parameters or execute a plan.
- Implementation: [`sql_v1.c`](../../engine/sdk/sql_v1.c) performs bounded tokenization and parsing, stores source offsets/lengths, and is included in the TinyCC SDK archive.
- Regression: [`sdk_sql_select_v1.c`](../../engine/tests/fixtures/sdk_sql_select_v1.c) checks the documented parameterized SELECT, case-insensitive keywords, preserved identifier bytes, star projection, LIMIT 0, DELETE rejection, malformed SELECT, second-statement rejection, too many columns, query length, parameter range, and LIMIT range. Initial TinyCC compile failed because `prdb_sdk_sql_v1.h` was absent; the fixture passes after implementation.
- Remaining: bind and type-check parameters against authorized schema, submit a bounded structured request to the planner, implement expression semantics and storage-backed SELECT execution, and add mutations only after statement-abort behavior is wired end to end.
