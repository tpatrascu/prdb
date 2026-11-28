# SDK Library Repository

## Purpose and Authority

Each database instance maintains an administrator-managed repository of immutable SQL SDK shared libraries. The repository replaces deployment-time release directories and the supervisor's content-addressed SDK cache as the source of libraries used by query workers. A repository release is installed on that database instance by an authenticated database administrator; clients select an installed release, and ordinary query submission cannot add, replace, or remove repository content.

The repository contains executable native code, but the trusted services treat it only as bounded bytes and metadata. The supervisor and storage manager never load or execute an SDK library. A clean confined executor loads the selected library with `query.so` only after sandbox restrictions are active. Its constructors and functions are untrusted code under the same confinement as the query.

The repository is instance-local durable service state, stored outside transactional user catalog records. It survives supervisor restart and database reopen. Install and removal are atomic repository operations with durable outcomes; they do not participate in user MVCC transactions or SQL DDL transactions. A repository change does not alter an already sealed package or the SDK release pinned by a running invocation.

## Release Identity and Contents

An installed release consists of one versioned SQL SDK shared object and canonical release metadata. The identity is the tuple `(sdk_release, native_abi, target, cpu_baseline, runtime_profile)`; a single library release may declare several compatible native ABIs. The shared object SONAME is exactly `libprdb_sql.so.<sdk_release>`. The metadata records the release number, ABI list, target, CPU/runtime profile, SONAME, actual `DT_NEEDED` names, format version, and SHA-256 digest of the library bytes. The digest is integrity metadata, not an authenticity claim; install authority comes from the authenticated administrator operation.

For this profile, the supported target is Linux x86_64. CPU baseline and runtime profile must exactly match the database instance's configured compatibility profile. Metadata and ELF dynamic information must agree: SONAME and dependency set match, and the SDK library has no RPATH or RUNPATH. Malformed metadata, duplicate release identities, unsupported ABI, unexpected dependencies, invalid ELF, or digest mismatch reject installation before publication. The versioned library and metadata become immutable after install. A changed build requires a new release identity; reusing a release number with different bytes is rejected.

## Administrative API

Repository operations use the authenticated ADMIN control channel, not query invocation IPC or SQL execution. Each operation reauthenticates the caller. Listing is available to any authenticated database principal because it returns only compatibility metadata; install and removal require current database-administrator authority. Executor processes, client package artifacts, and SDK calls cannot invoke repository operations.

The typed control API provides:

| Operation | Behavior |
| --- | --- |
| `sdk_libraries.list` | Return bounded metadata for installed releases, ordered by release and ABI, to any authenticated principal. It does not return library bytes or host filesystem paths. |
| `sdk_libraries.install` | Accept one bounded metadata record and library byte stream, validate both, then atomically publish the complete immutable release. A successful response includes its identity and digest. |
| `sdk_libraries.remove` | Remove one exact SDK release number, including all native ABIs declared by that release, from future resolution. Reject removal while any live invocation pins any of those ABIs. Removal does not rewrite packages or change a client's saved selection. |
| `sdk_libraries.operation_status` | Return the durable outcome for an operation ID created by the authenticated caller, or `unknown` if no completed outcome is recorded. |

These requests use the authenticated ADMIN frame (`kind: 10`) with `operation_version: 1`. Listing uses `phase: "read"`, `operation: "sdk_libraries.list"`, and the standard `limit` plus opaque `cursor` arguments. Install and removal carry a client-generated, nonzero 128-bit `operation_id`; retrying the same operation ID and intent returns or resumes that operation, while reusing it for a different owner or intent is rejected. Removal uses `phase: "write"`, `operation: "sdk_libraries.remove"`, and a canonical positive decimal `release`; the server rechecks current database-admin authority on the write request and reports the removed byte count only after the repository directory is synchronized. Removal addresses the whole release, so it does not accept an ABI selector. Operation status uses `phase: "read"` and `operation: "sdk_libraries.operation_status"` with the operation ID in `arguments`; the result is owner-scoped and reports `unknown` or the recorded successful install/remove result.

Install size is capped at 128 MiB for the library and 64 KiB for metadata. The control request carries the operation ID, declared lengths, and SHA-256; upload is bounded and must finish before validation. Partial uploads and failed validations publish nothing. Repeating an install for the same identity and digest is idempotent; the same identity with different bytes or metadata is a conflict. The repository validates with the same profile and ELF rules used at query admission. It never calls a dynamic loader.

Install publication writes and synchronizes temporary files, sets read-only file permissions and non-writable release-directory permissions, then atomically publishes without replacement. Removal unlinks the exact installed release atomically after confirming it is not pinned. The service records a durable operation outcome before acknowledging success. Concurrent list/install/remove operations observe only complete releases; conflicting mutations serialize per release identity. Crash recovery discards incomplete temporary installs and preserves completed operations. Storage quota is bounded by an instance-configured maximum repository byte count; an install exceeding remaining capacity fails without evicting another release. Eviction is not automatic: an administrator explicitly removes an unneeded release.

The list response includes release, ABI set, target, CPU baseline, runtime profile, SONAME, digest, dependencies, and library byte count. It omits paths and executable bytes. Pagination is bounded and uses the established authenticated control metadata paging rules. An authenticated principal may list releases without being able to read arbitrary database files; the API exposes only the declared repository metadata.

## Resolution and Invocation Lifetime

At SEAL, the service validates the PMF1 manifest and query ELF as data. The declared SDK release, ABI, target, CPU baseline, and runtime profile must be supported by the instance and agree with the query's `DT_NEEDED` SONAME. At START, before storage admission or worker launch, the supervisor resolves the exact installed repository identity. A missing release, incompatible profile, ABI mismatch, digest mismatch, or dependency mismatch fails closed and does not create a storage transaction or execute user code.

Resolution acquires an invocation pin on the immutable repository entry. The supervisor stages the library under its SONAME beside `query.so` in the invocation's private read/execute-only artifact directory. The library is then loaded only by the confined executor using the query's exact `$ORIGIN` RUNPATH. The pin remains held through worker exit and all failure/cleanup paths. A pinned version cannot be removed. After the invocation ends, the pin is released and an administrator may remove that version.

The repository is a source of immutable files, not a loaded-code cache. Workers have separate processes, address spaces, mutable SDK globals, and SQL state. The kernel may share file page-cache pages. Repository identity is not authorization, and a digest does not prove code safe. Storage continues to authorize and validate every SDK host request independently.

### Default release for plain SQL-text submissions

A plain SQL-text submission (`submission_kind: "sql_text"`, see [SQL library](sql-library.md)) carries no manifest, so there is no client-declared release to resolve. The repository instead exposes `resolve_default()`, keyed by a `default_release` fixed at supervisor startup (`prdb-supervisor --default-sdk-release N`), never by client input. If no default is configured, or the configured release is not installed, `resolve_default()` fails closed with `UnavailableRelease` before storage admission or worker launch, exactly as an unresolvable manifest-declared release does for native-artifact submissions. The resolved pin is staged and loaded identically to the manifest-resolved case, except no `query.so` is staged alongside it: the confined executor `dlsym`s the SQL engine's batch entry points directly from the staged library's own path.

## Persistent Client Version Selection

The client keeps a durable SDK selection for each configured database instance in one client configuration file. Unless overridden by `PRDB_CLIENT_CONFIG`, its path is `$XDG_CONFIG_HOME/prdb/client.json`, or `$HOME/.config/prdb/client.json` when `XDG_CONFIG_HOME` is unset. The file is created with mode 0600 in a mode-0700 parent directory, must be owned by the invoking user, is opened without following symlinks, and is atomically replaced on updates. The client rejects broader permissions, duplicate JSON keys, unknown fields in the current format, invalid UTF-8, noncanonical IDs, excessive nesting, or files larger than 1 MiB. A failed read or update is an error; the client must not silently fall back to a different instance, SDK release, or credential.

The version-1 file is UTF-8 JSON with this canonical shape (all keys shown are required unless marked optional):

```json
{
  "format": "PRDB-CLIENT-CONFIG-1",
  "default_instance": "local-dev",
  "client_tools": {
    "include_dir": "sdk/include",
    "tcc": "tcc",
    "shared_linker": "cc",
    "patchelf": "patchelf"
  },
  "instances": [
    {
      "name": "local-dev",
      "address": "127.0.0.1",
      "port": 7443,
      "sdk": {
        "release": 1,
        "target": "linux-x86_64",
        "cpu_baseline": "x86_64-v1",
        "runtime_profile": "nixos-glibc-2.42",
        "native_abi": 1
      },
      "default_credential": "alice",
      "credentials": [
        { "name": "alice", "username": "alice", "token_id": "<32 lowercase hex>", "token_secret": "<base64url secret>" }
      ]
    }
  ]
}
```

`format` is exactly `PRDB-CLIENT-CONFIG-1`. `default_instance` names exactly one entry in `instances`; when there are no configured instances, it is absent and commands requiring an endpoint fail until one is configured. `client_tools` is optional; when present, its `include_dir`, `tcc`, `shared_linker`, and `patchelf` fields are all required, nonempty UTF-8 paths/program names with no shell expansion. Relative paths resolve from the config file's parent directory. Instance names and credential profile names are unique, normalized ASCII identifiers of 1–64 bytes. `address` is an IP literal; DNS names are deferred until the client has a separately specified TLS identity policy. `port` is an integer from 1 through 65535. `sdk.release` is a nonzero u32, `target` is a supported target token, `cpu_baseline` and `runtime_profile` are tokens of 1–64 bytes, and `native_abi` is a supported nonzero ABI version. `default_credential` names exactly one entry in that instance's `credentials` array. Each credential has a unique normalized database `username`, a token ID encoded as 32 lowercase hex digits, and a token secret encoded as canonical unpadded base64url of 32 secret bytes. An instance may have additional named credential profiles for `RUNAS`; the selected default name is explicit because server credentials do not establish a username by themselves. All secrets are local client authentication material and are never sent to the executor, included in query artifacts/parameters, or logged.

This file contains credentials in cleartext and is protected by its owner-only filesystem permissions; it is not an encrypted secret vault. The client zeroizes secret buffers on shutdown and on parse/authentication failures where practical. Updates write a mode-0600 temporary file in the same directory, synchronize it, atomically rename it over the old file, and synchronize the parent directory. The client never writes secrets to diagnostics, generated C, compiler environments, receipts, or package metadata. Users needing stronger at-rest protection must use a future credential-provider integration; that does not change the version-selection contract.

The configured database instance is selected by its unique `name`; commands may omit `--instance` to use `default_instance`. They may accept an explicit `--address`/`--port`, but when both an instance name and endpoint are present they must match the config exactly. Commands that use persistent settings require an explicit instance or configured default. The saved SDK selection is the selected SDK release plus target, CPU baseline, runtime profile, and native ABI. The selected server endpoint, default credential name, and SDK release are not inferred from other instances.

The client provides authenticated commands to inspect repository releases and set the selected release for an instance. Selection requires that the release is listed by that instance and that the local client SDK package for the same release/profile/ABI is available when compilation is required. The selection command persists the setting; `interactive-sql` uses it without requiring a release flag on each invocation. A one-command override may be supported only when explicitly named and does not silently rewrite the persistent selection. Config editing uses an explicit client command and preserves unrelated instance and credential entries.

The CLI surface is `prdb-client instance list|show|set`, `prdb-client sdk-library list --instance NAME`, `prdb-client sdk-library select --instance NAME --release N --abi N`, `prdb-client sdk-library install --instance NAME --metadata FILE --library FILE`, and `prdb-client sdk-library remove --instance NAME --release N`. `instance set` adds or updates endpoint and credential settings without changing the SDK selection unless an explicit SDK selection is supplied. `sdk-library select` performs an authenticated repository list, verifies a matching local client SDK build is available, then atomically updates the config. Listing/selection use the configured authenticated client identity; install/remove require database-admin credentials. These commands are conveniences over typed client/control APIs; parsing command arguments never grants administrative authority.

Before building or submitting a package, every client path checks the selected version against the package manifest and any locally used SDK library. `interactive-sql` uses the locally installed SDK library matching the saved release for every package, including packages in a long-running explicit transaction. `prdb-client submit` for an existing shared-library query package checks the package manifest against the saved release; it does not rewrite or relink the package. A package for another release fails locally with an actionable mismatch error unless the caller explicitly selects an override. If the server no longer has the selected release, normal admission rejects before execution; the client does not silently choose another installed release.

An SDK selection change affects only packages built or submitted afterward. It does not mutate in-flight packages, explicit transactions, stored client packages, or repository releases. Inside an interactive explicit transaction, changing the selected release is rejected until the transaction ends, preventing different SDK versions from participating in one transaction. In autocommit mode, selection can be changed between statements. `RUNAS` changes the effective database identity only and never changes the SDK release.

## Compatibility and Migration

PMF1 continues to declare the SDK release and native ABI. Repository selection therefore uses existing manifest fields; no client-supplied SQL shared object is accepted as a substitute for a repository release. Package PREPARE carries query bytes, manifest, scope, and parameters only. SQL-library byte/digest fields and kind-12 query chunks are rejected. A peer using the old optional library-upload fields fails closed; the service never falls back to uploaded library bytes or a deployment cache.

Supervisors open the persistent instance repository with an optional repository quota; they do not accept release-directory or SDK-cache startup flags. Existing release files must be explicitly reinstalled through the administrator API after migration, using the same validation and durable publication as any install. There is no automatic import, default release, static-library fallback, or cache-miss fallback. Administrators install all releases clients are expected to use and remove old versions only after clients and active invocations have moved away from them.

## Acceptance Evidence

Conformance requires tests across the real control API, gateway, supervisor, client, confined executor, and storage boundaries:

1. Any authenticated principal can list release metadata; only a current database administrator can install or remove releases. Query workers and ordinary clients cannot mutate the repository.
2. Valid metadata/library pairs install atomically and survive restart. Malformed, incompatible, oversized, digest-mismatched, or hostile ELF inputs publish nothing and are never loaded by trusted services.
3. Same identity/same digest install is idempotent; same identity/different digest conflicts. Concurrent operations reveal only complete entries.
4. `interactive-sql` and shared-library package submission persist and enforce the selected instance release. A package built for another release fails before upload or START as appropriate, with no silent fallback.
5. Missing or incompatible server releases fail before storage admission. The matching library is staged and loaded only inside confinement; hostile constructors remain confined.
6. Removal is denied while pinned, succeeds after worker cleanup, and prevents later resolution. A failed or interrupted install leaves no visible partial release.
7. The suite covers client setting permissions, corrupt configuration, endpoint scoping, atomic updates, saved selection after restart, explicit override behavior, and release changes around interactive transaction boundaries.

The host compatibility profile remains Linux x86_64 with the configured CPU/runtime baseline. Other targets require separate repository compatibility and confinement validation.
