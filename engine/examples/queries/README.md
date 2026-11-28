# Custom query program examples

This directory has three small, complete, hand-written query programs that
use the SQL SDK directly, plus a walkthrough for compiling and submitting
them with `prdb-packager`/`prdb-client submit`. This is the **native-artifact
submission path** — a different thing from `prdb-client interactive-sql`,
which submits plain SQL text with no compiled program at all (see
[`specs/sql-library.md`](../../../specs/sql-library.md)).

Write a program like these when you need host-side logic a plain SQL
statement can't express: branching on invocation parameters, combining SQL
with direct storage calls in one transaction, or producing output in your
own format instead of generic SQLR rows. For ordinary ad hoc SQL, use
`prdb-client interactive-sql` instead — it is simpler and needs no
compilation step.

| File | Demonstrates |
| --- | --- |
| [`hello_sql.c`](hello_sql.c) | Calling the SQL SDK (`prdb_sdk_sql_run_statement_v1`) for a parameterized SELECT. |
| [`direct_scan.c`](direct_scan.c) | Reading a table with only the low-level storage SDK — no SQL at all — and formatting plain-text output. |
| [`mixed_insert_and_verify.c`](mixed_insert_and_verify.c) | Writing through SQL and reading back through direct storage calls in the same invocation. |
| [`common.h`](common.h) | The handful of lines shared by all three (host validation, a `write_result` wrapper). Not part of the SDK itself. |

Every submitted program is untrusted native code, confined the same way
regardless of what it calls: see [`specs/client-sdk.md`](../../../specs/client-sdk.md)
and [`specs/sdk-library-repository.md`](../../../specs/sdk-library-repository.md).

## 1. Schema prerequisite

All three examples read and/or write one table. Create it once with
`admin-sql` (a separate authenticated control-channel command — it never
touches the query-submission path these examples use):

```sh
prdb-client admin-sql --address 127.0.0.1:PORT --credentials admin.json --sql-file - <<'SQL'
CREATE TABLE IF NOT EXISTS public.widgets (id INTEGER NOT NULL, name TEXT)
SQL
```

## 2. Locate your SDK release

`prdb-packager` links against one specific, already-built
`libprdb_sql.so.<release>` and its headers. In a `release-profile` build
(see [`specs/toolchain.md`](../../../specs/toolchain.md)) these live at:

```
<release-profile>/sdk/include/                                                  # --include-dir
<release-profile>/sdk/releases/<target>/<cpu-baseline>/<runtime-profile>/sdk-<release>-abis<N>/libprdb_sql.so.<release>   # --sdk-library
```

Substitute your deployment's actual target/CPU-baseline/runtime-profile/release;
`prdb-client sdk-library list --instance NAME` reports which releases the
server you're submitting to has installed and will accept.

## 3. Build one example with `prdb-packager`

```sh
prdb-client scope --address 127.0.0.1:PORT --credentials user.json \
  --schema public --relation widgets --operations select,insert \
  --output widgets.psc1

prdb-packager \
  --source hello_sql.c \
  --sdk-library /path/to/sdk/releases/linux-x86_64/x86_64-v1/<runtime-profile>/sdk-1-abis1/libprdb_sql.so.1 \
  --include-dir /path/to/sdk/include \
  --scope widgets.psc1 \
  --output-dir ./hello_sql-package \
  --abi 1 --sdk-release 1 \
  --cpu-baseline x86_64-v1 --runtime-profile <runtime-profile> \
  --compiler-identity "my-org/hello_sql@1"
```

This produces `./hello_sql-package/` containing `query.so`, `manifest.pmf1`,
`requested_scope.psc1`, and a digest — exactly what `prdb-client submit
--package` expects. `--scope` must include every schema/table/operation the
program actually touches, or storage will reject it at admission regardless
of what the program's C code tries to do; scope is a request ceiling, not
something the program can expand. `direct_scan.c` and `mixed_insert_and_verify.c`
build the same way, just with `--source direct_scan.c`/`--source
mixed_insert_and_verify.c` and (for `mixed_insert_and_verify.c`) `insert` added
to `--operations`.

## 4. Encode parameters, if the example needs any

`hello_sql.c` and `mixed_insert_and_verify.c` expect PSP1-encoded parameters.
Unlike `interactive-sql`, `submit` never encodes parameters for you — a
custom program can define its own parameter format entirely, so the client
has no way to guess one. Use `prdb-client parameters-encode` to build a PSP1
file from the same JSON shape `interactive-sql` uses internally:

```sh
prdb-client parameters-encode --input - --output hello_sql.psp1 <<'JSON'
{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"integer","value":"1"}]}
JSON
```

`direct_scan.c` takes no parameters; omit `--parameters` entirely for it.

## 5. Submit and read the result

```sh
prdb-client submit --address 127.0.0.1:PORT --credentials user.json \
  --package ./hello_sql-package --receipt hello_sql.receipt \
  --parameters hello_sql.psp1
```

`direct_scan.c` and `mixed_insert_and_verify.c` print plain text you can read
directly, because they format their own `write_result` output. `hello_sql.c`
instead streams raw **SQLR v1** records — the same binary row format the SQL
SDK always emits for `write_result` (documented in
[`prdb_sdk_sql_result_v1.h`](../../include/prdb_sdk_sql_result_v1.h) and
[`specs/sql-library.md`](../../../specs/sql-library.md)) — not text. A real
client needs its own SQLR decoder to render that output, the same way
`prdb-client interactive-sql` used to before it moved to plain SQL-text
submission. That tradeoff — plain SQL convenience calls are easy to write but
emit a binary wire format, while direct storage calls take more code but let
you format output yourself — is exactly the choice these three examples are
here to show.

## Notes

- All three examples reject a malformed host/invocation before calling any
  SDK function (`prdb_example_require_host` in `common.h`); do this in any
  program you write, even though a real confined executor always supplies a
  valid pair.
- A nonzero SQL/storage status already latches the executor's sticky
  statement-failure callback. The function's own return value only picks the
  worker's local exit code — it does not have, and cannot be given, any
  authority over whether the invocation commits.
- None of these examples poll cancellation between scan pages, because the
  demo table fits in one page. A long-running scan should poll with
  `prdb_sdk_executor_check_cancelled_v1` between pages, as the real SQL SDK
  does internally.
