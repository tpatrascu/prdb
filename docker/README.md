# PRDB images with Podman

The root `Dockerfile` contains two image targets for Linux x86_64:

- `client` provides `prdb-client`, TinyCC, and the matching SQL SDK. It connects
  to a separately running gateway.
- `server` provides the supervisor, gateway, executor, optimizer, client,
  bootstrap tools, and SQL SDK. Its default command initializes the instance
  on first start and launches the supervisor and gateway.

The examples below use Podman commands. Run them from the repository root.

## Build the images

```sh
podman build --target client --tag localhost/prdb-client:latest .
podman build --target server --tag localhost/prdb-server:latest .
```

The image build defaults to CPU baseline `x86_64-v1`, runtime profile
`debian-bookworm-glibc-2.36`, SDK release `1`, and SDK ABI `1`. If the
server uses a different compatible profile, build both images with matching
arguments. For example:

```sh
podman build --target client --tag localhost/prdb-client:latest \
  --build-arg PRDB_CPU_BASELINE=x86_64-v1 \
  --build-arg PRDB_RUNTIME_PROFILE=debian-bookworm-glibc-2.36 \
  --build-arg PRDB_SDK_RELEASE=1 \
  --build-arg PRDB_SDK_ABIS=1 .
```

The same build arguments can be supplied when building the `server` target.
Keep the client and server SDK release, CPU baseline, runtime profile, and ABI
set compatible. The server includes its SDK at
`/opt/prdb/sdk/releases/linux-x86_64/<cpu-baseline>/<runtime-profile>/`.

## Run the client image

With no arguments, the client image prints its command help:

```sh
podman run --rm localhost/prdb-client:latest
```

The client accepts a numeric IP address and port. For a gateway published on
the same Linux host, use host networking and `127.0.0.1`:

```sh
podman run --rm --network=host \
  --userns=keep-id:uid=10001,gid=10001 \
  --volume "$PWD/prdb-data:/var/lib/prdb:ro" \
  localhost/prdb-client:latest discover \
    --address 127.0.0.1:8765 \
    --credentials /var/lib/prdb/admin.credentials
```

For a gateway on another host, replace `127.0.0.1` with its numeric IP address
reachable from the client container. `prdb-client` does not accept DNS names
for `--address`.

To run an SQL script, mount the script, requested scope, and any parameter file.
The interactive SQL client supports scripts and terminal sessions; the SDK
shared library and headers are already in the image:

```sh
podman run --rm --network=host \
  --userns=keep-id:uid=10001,gid=10001 \
  --volume "$PWD/prdb-data:/var/lib/prdb:ro" \
  --volume "$PWD/client-files:/work:rw" \
  localhost/prdb-client:latest interactive-sql \
    --address 127.0.0.1:8765 \
    --credentials /var/lib/prdb/admin.credentials \
    --default-user admin \
    --sql-file /work/query.sql --schema public \
    --scope /work/scope.psc1 \
    --sdk-library /opt/prdb/sdk/releases/linux-x86_64/x86_64-v1/debian-bookworm-glibc-2.36/sdk-1-abis1/libprdb_sql.so.1 \
    --include-dir /opt/prdb/include \
    --sdk-release 1 --cpu-baseline x86_64-v1 \
    --runtime-profile debian-bookworm-glibc-2.36 \
    --compiler-identity tcc-container-debian-bookworm
```

The SDK library path and profile arguments must match the image build and the
server's accepted compatibility profile. Interactive SQL uses the same SDK
options; see the client help for its full argument list.

## Start the server image

The server needs delegated cgroup v2 controllers for query worker limits. It
refuses to start unless `PRDB_CGROUP_PARENT` points to a writable cgroup v2
directory with `cpu`, `memory`, and `pids` available and enabled for child
cgroups. Mounting `/sys/fs/cgroup` read-write does not create delegation by
itself.

On a Linux host managed by systemd, the repository helper below moves the
container launcher into a child cgroup and passes the writable delegated
parent to the container. Create a private persistent data directory first:

```sh
install -d -m 0700 "$PWD/prdb-data"
```

Then start the server as a user service. The `keep-id` mapping makes the host
user own the files written by the image's UID 10001 process:

```sh
systemd-run --user --collect --unit=prdb-container.service --slice=prdb-tests.slice \
  --property='Delegate=cpu memory pids' \
  --property="WorkingDirectory=$PWD" \
  "$PWD/engine/tools/with_delegated_cgroup.sh" \
  env bash -c '
    exec podman run --replace --name prdb \
      --cgroupns=host \
      --userns=keep-id:uid=10001,gid=10001 \
      --volume /sys/fs/cgroup:/sys/fs/cgroup:rw \
      --volume "$PWD/prdb-data:/var/lib/prdb:rw" \
      --env "PRDB_CGROUP_PARENT=$PRDB_TEST_CGROUP_PARENT" \
      --publish 8765:8765 --publish 8766:8766 \
      localhost/prdb-server:latest
  '
```

The query gateway listens on port 8765 and the control gateway on 8766. The
protocol is plaintext TCP; publish these ports only on a trusted network. The
service stays active while the container runs. Inspect or stop it with:

```sh
systemctl --user status prdb-container.service
journalctl --user -u prdb-container.service
systemctl --user stop prdb-container.service
```

On the first start, the server initializes `/var/lib/prdb/instance` and writes
the initial administrator credential to
`/var/lib/prdb/admin.credentials` with mode 0600. The secret is not printed.
The bind-mounted host file is `$PWD/prdb-data/admin.credentials`; the client
example above mounts it read-only. Preserve the full `prdb-data` directory
across container replacement or upgrades.

If initialization was interrupted, do not delete the instance directory.
Remove any unacknowledged credential file from the incomplete attempt, then
resume bootstrap against the same persistent directory:

```sh
podman run --rm \
  --userns=keep-id:uid=10001,gid=10001 \
  --volume "$PWD/prdb-data:/var/lib/prdb:rw" \
  --entrypoint /usr/local/bin/prdb-init \
  localhost/prdb-server:latest \
    --instance-dir /var/lib/prdb/instance \
    --administrator prdb_admin \
    --credential-file /var/lib/prdb/admin.credentials \
    --expires-in-seconds 2592000 --resume
```

The existing credentials file must be removed before running this command; the
resume operation issues a replacement token. See the
[user-service bootstrap instructions](../contrib/systemd/README.md#per-login-user-service)
for the credential handoff contract.

## Configuration and limits

The server image runs as UID 10001 and persists database state, scratch files,
the immutable SDK repository, and administrator credentials under `/var/lib/prdb`. SDK releases are installed explicitly by an administrator after startup with `prdb-client sdk-library install`; the repository is not seeded from a client SDK directory. Useful
environment settings include:

- `PRDB_CGROUP_PARENT`: required writable delegated cgroup v2 parent.
- `PRDB_BIND` and `PRDB_CONTROL_BIND`: gateway listen addresses; defaults are
  `0.0.0.0:8765` and `0.0.0.0:8766`.
- `PRDB_ADMINISTRATOR` and `PRDB_CREDENTIAL_FILE`: first administrator name
  and credential path.
- `PRDB_CPU_BASELINE` and `PRDB_RUNTIME_PROFILE`: define the instance SDK
  compatibility profile.
- `PRDB_SDK_REPOSITORY_DIR` and `PRDB_SDK_REPOSITORY_MAX_BYTES`: optional
  repository location and quota; defaults are under the instance directory.
- `PRDB_MAX_ROWS`, `PRDB_MAX_BYTES`, `PRDB_CPU_QUOTA_US`,
  `PRDB_CPU_PERIOD_US`, `PRDB_MEMORY_MAX_BYTES`, and `PRDB_MAX_PROCESSES`:
  worker resource limits.

Use a runtime profile validated for the host. The server image does not bypass
the executor's sandbox requirements when the host lacks required features.
For the host's broader runtime and confinement setup, see
[the system service deployment guide](../contrib/systemd/README.md).

## Docker Compose demo

`docker/compose.yaml` runs two separate containers (`server` and `client`)
connected over a shared Compose network and sharing the `prdb-data` volume for
instance state and `admin.credentials`. The `client` container runs
`docker/client-repl-loop.sh`, which resolves the `server` service IP, fetches a
scope if needed, and runs `prdb-client interactive-sql` in a loop that
automatically reconnects on disconnect.

Start the demo inside a delegated cgroup v2 user service:

```sh
systemd-run --user --wait --pipe --collect \
  --unit=prdb-compose-demo.service --slice=prdb-tests.slice \
  --property='Delegate=cpu memory pids' \
  --property="WorkingDirectory=$PWD" \
  "$PWD/engine/tools/with_delegated_cgroup.sh" \
  env bash -c '
    export PRDB_CGROUP_PARENT="$PRDB_TEST_CGROUP_PARENT"
    export PRDB_UID=$(id -u) PRDB_GID=$(id -g)
    exec docker compose -f docker/compose.yaml up --build
  '
```

Attach to the interactive SQL REPL in another terminal:

```sh
PRDB_CGROUP_PARENT=/dummy docker compose -f docker/compose.yaml attach client
```


Teardown Demo:

```sh
systemctl --user stop prdb-compose-demo.service 2>/dev/null || true
systemctl --user reset-failed prdb-compose-demo.service
PRDB_CGROUP_PARENT=/dummy docker compose -f docker/compose.yaml down -v
```