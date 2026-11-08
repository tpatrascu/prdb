# Sandboxed SQL v3 failure integration test

The `sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output` test in `engine/tests/commit.rs` runs the maintained TinyCC artifact through authenticated staging, the configured cgroup/Landlock executor launch path, result relay, and storage terminal decision. The artifact writes provisional bytes, catches an unsupported SQL statement error, and returns success. The test requires the v3 host latch to turn that caught error into a worker failure, then checks that storage confirms ABORTED before the gateway terminal frame follows the provisional result frame.

First build the artifact and SDK archive:

```sh
nix-shell -p rustc cargo clippy rustfmt tinycc --run 'make -f engine/sdk/Makefile sandbox-query-fixture'
```

Configure these environment variables for the host running the test:

- `PRDB_TEST_CGROUP_PARENT`: an existing cgroup v2 parent delegated with enabled `cpu`, `memory`, and `pids` controllers.
- `PRDB_TEST_QUERY_ARTIFACT`: absolute path to `target/sdk-v1/sql-v3-caught-error.so`.
- `PRDB_TEST_RUNTIME_READ_EXECUTE`: platform-separated absolute paths for the pinned dynamic loader, libc, and other executor runtime dependencies.
- `PRDB_TEST_RUNTIME_READ_ONLY`: platform-separated absolute paths for runtime configuration files the dynamic loader needs, such as its loader cache when applicable.

Run the opt-in integration test after configuring the exact runtime profile:

```sh
cargo test --locked --test commit sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output -- --ignored --exact --nocapture
```

Do not execute or load the artifact outside this executor path. A successful artifact build and a skipped Cargo test do not establish sandbox or durable-abort acceptance; the test must run on a host with the configured cgroup delegation and runtime profile.
