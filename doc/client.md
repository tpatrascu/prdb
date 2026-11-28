# Client & SDK Guide (`prdb-client`)

`prdb-client` is the command-line and interactive SQL client for PRDB. It compiles SQL queries locally using **TinyCC** against a versioned C SDK and submits the resulting `query.so` to the PRDB Gateway.

## Client Modes

1. **`discover`**: Inspects server capabilities and available SDK profiles.
2. **`scope`**: Fetches and writes a `requested_scope` file (e.g., `/tmp/scope.psc1`) for a target schema.
3. **`admin-sql`**: Submits bounded administrative DDL (schemas, tables, users, roles, grants, and policies) over an authenticated control path.
4. **`interactive-sql`**: Starts an interactive SQL REPL or executes a `--sql-file` script within a retained MVCC snapshot.

## TinyCC Compilation & SDK Profiles

When running `interactive-sql`, `prdb-client` compiles statements into native code using TinyCC. The client and server must agree on the compatibility profile:

| Flag | Example Value | Purpose |
| :--- | :--- | :--- |
| `--sdk-library` | `/opt/prdb/sdk/.../libprdb_sql.so.1` | Versioned SQL/runtime SDK shared library |
| `--include-dir` | `/opt/prdb/include` | C SDK headers |
| `--sdk-release` | `1` | SDK release version |
| `--cpu-baseline` | `x86_64-v1` | Target CPU instruction baseline |
| `--runtime-profile` | `debian-bookworm-glibc-2.36` | Target OS/libc profile |
| `--compiler-identity` | `tcc-container-debian-bookworm` | TinyCC compiler identifier |

> [!NOTE]
> `prdb-client --address` requires a numeric IP address and port (e.g., `127.0.0.1:8765`), not a DNS hostname.

## Interactive SQL & `RUNAS`

`interactive-sql` supports:
- Sequential SQL statements sharing a retained MVCC snapshot.
- Explicit transaction control (`BEGIN`, `COMMIT`, `ROLLBACK`).
- **`RUNAS <user>`**: Executes a query package as another authenticated actor without inheriting permissions from the session controller.
