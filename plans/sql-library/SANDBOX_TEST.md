# Sandboxed SQL v3 integration tests

The `sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output` test in `engine/tests/commit.rs` runs the maintained TinyCC artifact through authenticated staging, the configured cgroup/Landlock executor launch path, result relay, and storage terminal decision. The artifact writes provisional bytes, catches an unsupported SQL statement error, and returns success. The test requires the v3 host latch to turn that caught error into a worker failure, then checks that storage confirms ABORTED before the gateway terminal frame follows the provisional result frame.

The `sandboxed_v3_sql_dml_sequence_is_visible_and_commits` test uses `engine/tests/fixtures/sql_v3_write_read.c` to run a sequence of SDK calls over one sandboxed executor connection. It creates an owner-scoped nullable integer table, inserts NULL, verifies the row in a following SELECT/UPDATE scan, updates it to 42, verifies that value in a following SELECT, deletes it, then confirms a final SELECT returns no row. After receiving all provisional SQL results, the test closes the gateway before terminal delivery, requires the terminal frame write to fail, and checks that storage retained COMMITTED. Its restricted-principal query follows the normal terminal path. This exercises SQL parsing/binding, planner and executor IPC, private-write visibility across SELECT/UPDATE/DELETE, result relay, and storage commit through the real invocation path, including lost-terminal recovery.

The existing `authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes` process test can also run the restricted metadata artifact over authenticated TCP. Set `PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT` to the built `sql-v3-restricted-metadata.so`, and configure the same cgroup and runtime variables below. This optional branch packages the artifact for the test principal, uploads and starts it through `SubmissionClient`, and verifies filtered SQL results and committed STATUS through the external gateway and supervisor processes.

First build the artifact and SDK archive:

```sh
nix-shell -p rustc cargo clippy rustfmt tinycc --run 'make -f engine/sdk/Makefile sandbox-query-fixture'
```

This builds `sql-v3-caught-error.so`, `sql-v3-write-read.so`, and `sql-v3-restricted-metadata.so`. These are untrusted query artifacts and must only be loaded through the sandboxed executor tests.

Configure these environment variables for the host running the test:

- `PRDB_TEST_CGROUP_PARENT`: an existing cgroup v2 parent delegated with enabled `cpu`, `memory`, and `pids` controllers.
- `PRDB_TEST_QUERY_ARTIFACT`: absolute path to `target/sdk-v1/sql-v3-caught-error.so`.
- `PRDB_TEST_WRITE_READ_QUERY_ARTIFACT`: absolute path to `target/sdk-v1/sql-v3-write-read.so`.
- `PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT`: absolute path to `target/sdk-v1/sql-v3-restricted-metadata.so`, used by the restricted-principal metadata acceptance.
- `PRDB_TEST_RUNTIME_READ_EXECUTE`: platform-separated absolute paths for the pinned dynamic loader, libc, and other executor runtime dependencies.
- `PRDB_TEST_RUNTIME_READ_ONLY`: platform-separated absolute paths for runtime configuration files the dynamic loader needs, such as its loader cache when applicable. Set it to an empty string when the selected runtime has no read-only files; do not use absent paths as placeholders.

Run the opt-in integration test after configuring the exact runtime profile:

```sh
cargo test --locked --test commit sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output -- --ignored --exact --nocapture
cargo test --locked --test commit sandboxed_v3_sql_dml_sequence_is_visible_and_commits -- --ignored --exact --nocapture
```

Run the optional authenticated TCP SQL branch with:

```sh
cargo test -p prdb-engine --locked --test metadata_process authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes -- --exact --nocapture
```

Do not execute or load the artifact outside this executor path. A successful artifact build and a skipped Cargo test do not establish sandbox or durable-abort acceptance; the test must run on a host with the configured cgroup delegation and runtime profile.
