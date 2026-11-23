# systemd units

## Install a named per-user instance

`../install-user-instance.sh` installs the release executables under
`~/.local/libexec/prdb/`, initializes one database, writes per-instance
systemd user units and mode-0600 configuration, then enables and starts the
gateway. Each instance has separate named supervisor/gateway units, database
and scratch directories, runtime socket, audit log, and TCP listener. The
gateway and control listener bind to loopback; by default the control listener
uses the next port after the ordinary listener. Pick distinct, unused ports
for every instance.

Build the release binaries first. Supply the host-validated runtime profile
and its actual absolute runtime file paths; the installer cannot safely infer
those from the build machine or select a runtime profile for another host.
For example, replace all profile and file values below with the values
validated for the target host:

```sh
contrib/install-user-instance.sh \
  --name analytics --port 8765 --administrator first_admin \
  --runtime-profile nixos-x86_64-glibc-2.42 --cpu-baseline x86_64-v1 \
  --runtime-read-execute /usr/lib64/ld-linux-x86-64.so.2 \
  --runtime-read-execute /usr/lib64/libc.so.6 \
  --allowed-dependency libc.so.6
```

The script prints the path to the initial administrator credential file; it
does not print the secret. The client connects to `127.0.0.1:8765`. The
per-instance units are named `prdb-analytics-supervisor.service` and
`prdb-analytics-gateway.service`. Their profile and Landlock paths are saved
in `~/.config/prdb/instances/`. Paths containing whitespace are not supported.

Remove an instance and permanently delete its database, audit log, and
bootstrap credential with:

```sh
contrib/remove-user-instance.sh --name analytics
```

The removal script prompts before deleting. Pass `--yes` for a scripted
removal, or `--keep-data` to stop and remove the services/configuration while
preserving the instance data directory. Shared executables remain available
for other instances.

The unit files in `user/` and `system/` run the supervisor and TCP gateway as
separate services. Both supervisors request delegation of `cpu`, `memory`, and
`pids`; the launcher moves itself into a child cgroup, enables those
controllers, and exports the service cgroup path. Use `--cgroup-parent
systemd` in `PRDB_SUPERVISOR_ARGS` to consume that path. The supervisor rejects
startup if delegation is unavailable.

These units expect a READY instance and a host-specific, validated runtime
profile. They do not initialize a database or manufacture a compatibility
profile. See the supervisor and sandbox plans for the current operational
limitations.

## Per-login user service

Install the supervisor, gateway, executor, optimizer, `prdb-init`,
`prdb-maintain`, `prdb-landlock-run`, and `with_delegated_cgroup.sh` executables under
`~/.local/libexec/prdb/`, and copy both unit files to
`~/.config/systemd/user/`. For a release build from this checkout:

```sh
install -Dm755 target/release/prdb-supervisor ~/.local/libexec/prdb/prdb-supervisor
install -Dm755 target/release/prdb-gateway ~/.local/libexec/prdb/prdb-gateway
install -Dm755 target/release/prdb-executor ~/.local/libexec/prdb/prdb-executor
install -Dm755 target/release/prdb-optimizer ~/.local/libexec/prdb/prdb-optimizer
install -Dm755 target/release/prdb-init ~/.local/libexec/prdb/prdb-init
install -Dm755 target/release/prdb-maintain ~/.local/libexec/prdb/prdb-maintain
install -Dm755 target/release/prdb-landlock-run ~/.local/libexec/prdb/prdb-landlock-run
install -Dm755 engine/tools/with_delegated_cgroup.sh ~/.local/libexec/prdb/with_delegated_cgroup.sh
install -d ~/.local/lib/prdb
cp -R target/release-profile/sdk/releases/linux-x86_64/x86_64-v1/nixos-x86_64-glibc-2.42/sdk-1-abis1,2,3,4,5,6 ~/.local/lib/prdb/sql-sdk
install -Dm644 contrib/systemd/user/prdb-supervisor.service ~/.config/systemd/user/prdb-supervisor.service
install -Dm644 contrib/systemd/user/prdb-gateway.service ~/.config/systemd/user/prdb-gateway.service
```

Create `~/.config/prdb/` and put `supervisor.env` and `gateway.env` there,
readable only by the login user. Example values (replace the binary profile
paths and IDs for the host):

Create and initialize the instance as the same login user that will own the
user service. The setup command writes the first administrator token only to
the selected mode-0600 credential file; it never prints the secret.

```sh
install -dm700 ~/.local/state/prdb ~/.config/prdb
~/.local/libexec/prdb/prdb-init \
  --instance-dir ~/.local/state/prdb/instance \
  --administrator first_admin \
  --credential-file ~/.config/prdb/admin.credentials \
  --expires-in-seconds 2592000
```

If a prior initialization was interrupted, remove any credential file from
that incomplete attempt and run the same command with `--resume`; resume
rotates the unacknowledged bootstrap token.

```ini
# ~/.config/prdb/supervisor.env
PRDB_SUPERVISOR_ARGS=--instance-dir\ /home/USER/.local/state/prdb/instance\ --service-socket\ /run/user/1000/prdb/supervisor.sock\ --gateway-uid\ 1000\ --cgroup-parent\ systemd\ --executor\ /home/USER/.local/libexec/prdb/prdb-executor\ --optimizer\ /home/USER/.local/libexec/prdb/prdb-optimizer\ --scratch-parent\ /home/USER/.local/state/prdb/scratch\ --runtime-read-execute\ /usr/lib64/ld-linux-x86-64.so.2\ --runtime-read-execute\ /usr/lib64/libc.so.6\ --runtime-profile\ nixos-x86_64-glibc-2.42\ --cpu-baseline\ x86_64-v1\ --sdk-repository-max-bytes\ 1073741824\ --allowed-dependency\ libc.so.6\ --retention-seconds\ 86400\ --max-rows\ 100000\ --max-bytes\ 67108864\ --cpu-quota-us\ 100000\ --cpu-period-us\ 100000\ --memory-max-bytes\ 536870912\ --max-processes\ 32
PRDB_AUDIT_JSONL_PATH=/home/USER/.local/state/prdb/audit.jsonl
PRDB_LANDLOCK_TRAVERSE_PATHS=/home\ /home/USER\ /home/USER/.local\ /home/USER/.local/lib\ /home/USER/.local/lib/prdb\ /run\ /run/user\ /run/user/1000\ /sys\ /sys/fs\ /sys/fs/cgroup\ /nix\ /nix/store
PRDB_SUPERVISOR_LANDLOCK_PATHS=/home/USER/.local/libexec/prdb/prdb-executor\ /home/USER/.local/libexec/prdb/prdb-optimizer\ /home/USER/.local/lib/prdb/sql-sdk\ /nix/store/HASH-glibc\ /nix/store/HASH-gcc-lib
```

`EnvironmentFile` does not process shell quoting; systemd expands the unquoted
`$PRDB_SUPERVISOR_ARGS` as whitespace-separated arguments. Keep configured
paths free of whitespace. Add any runtime libraries required by the selected
profile using repeated `--runtime-read-execute` values. Administrators install
SDK releases into the instance-local repository through the authenticated control
API; clients upload query DLLs only. There are no release-directory, cache,
uploaded-SDK, or static-SDK fallback options. Add the SDK's non-SDK dependencies
to `--allowed-dependency`. Set the gateway UID to
the login UID (use `id -u`). The user service uses the default 0600 local
socket.

To enable the optional local audit sink, set `PRDB_AUDIT_JSONL_PATH` to an
absolute path whose parent already exists. The example above keeps the JSONL
file in the supervisor's existing state directory, which the user unit already
allows for writes through both systemd and Landlock. The sink is created with
mode `0600`. For a sink elsewhere, add its path to the supervisor Landlock
policy with `--writable-env PRDB_AUDIT_JSONL_PATH` in the unit's
`prdb-landlock-run` command and grant the path through `ReadWritePaths=` (or a
systemd drop-in); also include its ancestors in
`PRDB_LANDLOCK_TRAVERSE_PATHS` when they are not already traversable.

```ini
# ~/.config/prdb/gateway.env
PRDB_GATEWAY_ARGS=--bind\ 127.0.0.1:8765\ --service-socket\ /run/user/1000/prdb/supervisor.sock\ --service-uid\ 1000
PRDB_GATEWAY_LANDLOCK_TRAVERSE_PATHS=/home\ /run\ /run/user\ /run/user/1000\ /nix\ /nix/store
PRDB_GATEWAY_LANDLOCK_PATHS=/nix/store/HASH-glibc\ /nix/store/HASH-gcc-lib
```

The user units apply separate Landlock policies before starting each service.
The gateway can read its executable/runtime files and the supervisor socket
directory. The supervisor can write its instance, scratch, and runtime
directories, `/dev/null`, and the delegated cgroup subtree; its additional
allowlist covers the executor, optimizer, and runtime libraries. Child workers
receive their own narrower Landlock layer before untrusted query code starts.
`prdb-landlock-run` fails closed unless Landlock ABI 3 or newer is available.
Keep the two `*_LANDLOCK_PATHS` lists minimal and machine-specific. On NixOS,
use the store output directories containing the needed loader and libraries
(for example, find them with `ldd` and `readlink -f`). Each traversal list
grants directory traversal but not file-content reads for path ancestors; the
gateway list deliberately omits cgroup paths. All lists are whitespace-
separated, so configured paths must not contain whitespace.

Enable and start the pair:

```sh
chmod 600 ~/.config/prdb/supervisor.env ~/.config/prdb/gateway.env
systemctl --user daemon-reload
systemctl --user enable --now prdb-gateway.service
systemctl --user status prdb-supervisor.service prdb-gateway.service
```

For services that must start before the user logs in, enable lingering with
`loginctl enable-linger USER` as an administrator. This keeps the database
owned by that account and its state under the user's systemd state directory.

## System manager installation

Install all seven executables root-owned under `/usr/libexec/prdb/`, then create
a dedicated `prdb-gateway` system group plus `prdb` and `prdb-gateway` system
accounts. On systems with `groupadd` and `useradd`, the account setup is:

```sh
groupadd --system prdb-gateway
useradd --system --no-create-home --home-dir /var/lib/prdb --shell /usr/sbin/nologin --gid prdb-gateway prdb
useradd --system --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin --gid prdb-gateway prdb-gateway
```

Install executables and units, then create `/etc/prdb/supervisor.env` and
`gateway.env` as root with mode 0600:

```sh
install -d -o root -g root -m 0755 /usr/libexec/prdb /etc/prdb
install -m 0755 target/release/prdb-supervisor target/release/prdb-gateway target/release/prdb-executor target/release/prdb-optimizer /usr/libexec/prdb/
install -m 0755 target/release/prdb-init target/release/prdb-landlock-run /usr/libexec/prdb/
install -m 0755 target/release/prdb-maintain /usr/libexec/prdb/
install -m 0755 engine/tools/with_delegated_cgroup.sh /usr/libexec/prdb/
install -m 0644 contrib/systemd/system/prdb-supervisor.service contrib/systemd/system/prdb-gateway.service /etc/systemd/system/
```

Keep `/etc/prdb` root-owned and the two environment files mode 0600.
The supervisor runs as `prdb`; its database directory is mode 0700. The
gateway runs as `prdb-gateway`, and can reach only the 0660 supervisor socket
through the 0750 runtime directory. The socket inherits the supervisor's
primary group (`prdb-gateway`).

Create the state directory and initialize the database as its service owner
before starting systemd. The one-time admin credential is written with mode
0600 under the database service account; arrange its delivery to the intended
database administrator through your operator secret-distribution process.

```sh
install -d -o prdb -g prdb-gateway -m 0700 /var/lib/prdb
runuser -u prdb -- /usr/libexec/prdb/prdb-init \
  --instance-dir /var/lib/prdb/instance \
  --administrator first_admin \
  --credential-file /var/lib/prdb/admin.credentials \
  --expires-in-seconds 2592000
```

For an interrupted attempt, remove its credential file as the service owner,
then repeat with `--resume` so a replacement token is issued.

## Offline storage maintenance

`prdb-maintain compact` makes one quiescent redb compaction attempt. The
instance lock prevents it from running while the supervisor owns the database;
stop both services before maintenance, then run it as the instance owner:

```sh
systemctl --user stop prdb-gateway.service prdb-supervisor.service
~/.local/libexec/prdb/prdb-maintain compact \
  --instance-dir ~/.local/state/prdb/instance
systemctl --user start prdb-supervisor.service prdb-gateway.service
```

For the system services, stop the gateway and supervisor, run the command as
`prdb` with `--instance-dir /var/lib/prdb/instance`, then start both services.
The command validates the READY image while holding the instance lock and
prints whether compaction ran and how long it took. It does not wait for active
transactions or compact online.

Configure `PRDB_SUPERVISOR_ARGS` in `/etc/prdb/supervisor.env` with the same
required supervisor options as above, using these paths and values:

- `--instance-dir /var/lib/prdb/instance`
- `--service-socket /run/prdb/supervisor.sock`
- `--gateway-uid` set to `id -u prdb-gateway`
- `--service-socket-mode 0660`
- `--cgroup-parent systemd`
- `--scratch-parent /var/lib/prdb/scratch`

The optional JSONL audit sink can use `/var/lib/prdb/audit.jsonl`; the service
already has write access to `/var/lib/prdb` through its systemd and Landlock
policies. Set this in `/etc/prdb/supervisor.env`:

```ini
PRDB_AUDIT_JSONL_PATH=/var/lib/prdb/audit.jsonl
```

The sink parent directory must exist, and the sink is created with mode `0600`.
For a path outside `/var/lib/prdb` and `/run/prdb`, grant it through both
systemd `ReadWritePaths=` and the supervisor Landlock policy (for example,
`--writable-env PRDB_AUDIT_JSONL_PATH` in the unit's `prdb-landlock-run`
command); add missing ancestors to `PRDB_LANDLOCK_TRAVERSE_PATHS` as needed.

Configure `PRDB_GATEWAY_ARGS` in `/etc/prdb/gateway.env` with
`--service-socket /run/prdb/supervisor.sock` and `--service-uid` set to
`id -u prdb`. Bind loopback unless a trusted plaintext network is explicitly
intended. The system units also set a standard command search path (including
the NixOS system profile) for the delegated-cgroup helper and apply separate
Landlock policies to both processes. Add these host-specific allowlists to the
environment files, using systemd EnvironmentFile escaping for spaces:

```ini
# /etc/prdb/supervisor.env
PRDB_LANDLOCK_TRAVERSE_PATHS=/run\ /run/prdb\ /sys\ /sys/fs\ /sys/fs/cgroup\ /usr\ /usr/lib\ /usr/libexec\ /usr/libexec/prdb\ /usr/lib/prdb\ /var\ /var/lib\ /var/lib/prdb\ /nix\ /nix/store
PRDB_SUPERVISOR_LANDLOCK_PATHS=/usr/libexec/prdb/prdb-executor\ /usr/libexec/prdb/prdb-optimizer\ /usr/lib/prdb/sql-sdk\ /nix/store/HASH-glibc\ /nix/store/HASH-gcc-lib
```

The supervisor executable and `/dev` traversal are granted directly by its
unit. The environment-backed traversal list provides ancestors for its state,
runtime, delegated cgroup, executable, SDK repository, and runtime-library paths.
The repository is stored under the managed instance directory and query resolution
uses immutable installed releases. On systems
without Nix, omit `/nix` and add the actual loader/library directories used by
the validated runtime profile.

```ini
# /etc/prdb/gateway.env
PRDB_GATEWAY_LANDLOCK_TRAVERSE_PATHS=/run\ /run/prdb\ /usr\ /usr/lib\ /usr/libexec\ /usr/libexec/prdb\ /nix\ /nix/store
PRDB_GATEWAY_LANDLOCK_PATHS=/nix/store/HASH-glibc\ /nix/store/HASH-gcc-lib
```

The gateway policy allows its executable/runtime files and the supervisor
socket directory; it does not grant database-directory access. Then load and
enable the gateway (which pulls in the supervisor):

```sh
systemctl daemon-reload
systemctl enable --now prdb-gateway.service
systemctl status prdb-supervisor.service prdb-gateway.service
```

The service cgroup is delegated by systemd; do not manually mount a second
cgroup hierarchy or run these services as root. If a host truly lacks a
cgroup2 mount, follow the host setup instructions in the repository
`AGENTS.md`, then verify controller delegation with `prdb-sandbox-probe`.
