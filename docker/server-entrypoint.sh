#!/bin/bash
set -euo pipefail

if [[ "${1:-serve}" == "--help" || "${1:-serve}" == "-h" || "${1:-serve}" == "help" ]]; then
    cat <<'HELP'
PRDB server container

Default command: serve
Required environment: PRDB_CGROUP_PARENT must name an absolute writable cgroup v2
parent with cpu, memory, and pids controllers delegated to this container.

Useful settings:
  PRDB_INSTANCE_DIR       default /var/lib/prdb/instance
  PRDB_CGROUP_PARENT      required delegated cgroup v2 parent
  PRDB_BIND               default 0.0.0.0:8765
  PRDB_CONTROL_BIND       default 0.0.0.0:8766
  PRDB_RUNTIME_PROFILE    default debian-bookworm-glibc-2.36
  PRDB_CPU_BASELINE       default x86_64-v1
  PRDB_ADMINISTRATOR      default prdb_admin
  PRDB_CREDENTIAL_FILE    default /var/lib/prdb/admin.credentials

The gateway uses the protocol's configured plaintext TCP transport. Publish
its ports only on a trusted network. Persist /var/lib/prdb.
HELP
    exit 0
fi

if [[ "${1:-serve}" != "serve" ]]; then
    exec "$@"
fi
shift || true
if (($# != 0)); then
    echo "prdb-server-entrypoint: serve does not accept positional arguments; use environment settings" >&2
    exit 2
fi

instance_dir=${PRDB_INSTANCE_DIR:-/var/lib/prdb/instance}
scratch_dir=${PRDB_SCRATCH_DIR:-/var/lib/prdb/scratch}
credential_file=${PRDB_CREDENTIAL_FILE:-/var/lib/prdb/admin.credentials}
administrator=${PRDB_ADMINISTRATOR:-prdb_admin}
expires_in_seconds=${PRDB_ADMIN_EXPIRES_IN_SECONDS:-2592000}
service_dir=${PRDB_SERVICE_DIR:-/run/prdb}
service_socket=${PRDB_SERVICE_SOCKET:-$service_dir/supervisor.sock}
cgroup_parent=${PRDB_CGROUP_PARENT:-}
bind_address=${PRDB_BIND:-0.0.0.0:8765}
control_bind_address=${PRDB_CONTROL_BIND:-0.0.0.0:8766}
cpu_baseline=${PRDB_CPU_BASELINE:-x86_64-v1}
runtime_profile=${PRDB_RUNTIME_PROFILE:-debian-bookworm-glibc-2.36}
sdk_repository_dir=${PRDB_SDK_REPOSITORY_DIR:-$instance_dir/sdk-libraries}

if [[ -z "$cgroup_parent" || "$cgroup_parent" != /* ]]; then
    echo "prdb-server-entrypoint: set PRDB_CGROUP_PARENT to an absolute writable delegated cgroup v2 parent" >&2
    exit 2
fi
if [[ ! -d "$cgroup_parent" || ! -w "$cgroup_parent/cgroup.procs" || ! -r "$cgroup_parent/cgroup.controllers" ]]; then
    echo "prdb-server-entrypoint: cgroup parent is not writable or lacks cgroup v2 files: $cgroup_parent" >&2
    exit 2
fi
available=$(<"$cgroup_parent/cgroup.controllers")
enabled=$(<"$cgroup_parent/cgroup.subtree_control")
for controller in cpu memory pids; do
    if [[ " $available " != *" $controller "* ]]; then
        echo "prdb-server-entrypoint: cgroup parent lacks the $controller controller" >&2
        exit 2
    fi
    if [[ " $enabled " != *" $controller "* ]]; then
        echo "prdb-server-entrypoint: cgroup parent has not enabled $controller for child cgroups" >&2
        exit 2
    fi
done

mkdir -p "$(dirname "$instance_dir")" "$scratch_dir" "$service_dir"
chmod 0700 "$(dirname "$instance_dir")" "$scratch_dir" "$service_dir"
if [[ ! -e "$instance_dir/storage.redb" ]]; then
    /usr/local/bin/prdb-init \
        --instance-dir "$instance_dir" \
        --administrator "$administrator" \
        --credential-file "$credential_file" \
        --expires-in-seconds "$expires_in_seconds"
else
    /usr/local/bin/prdb-maintain inspect --instance-dir "$instance_dir"
fi
mkdir -p "$sdk_repository_dir"
chmod 0700 "$sdk_repository_dir"

supervisor_args=(
    --instance-dir "$instance_dir"
    --service-socket "$service_socket"
    --gateway-uid "$(id -u)"
    --cgroup-parent "$cgroup_parent"
    --executor /usr/local/bin/prdb-executor
    --optimizer /usr/local/bin/prdb-optimizer
    --scratch-parent "$scratch_dir"
    --runtime-read-execute /lib64/ld-linux-x86-64.so.2
    --runtime-read-execute /lib/x86_64-linux-gnu/libc.so.6
    --runtime-profile "$runtime_profile"
    --cpu-baseline "$cpu_baseline"
    --sdk-repository-dir "$sdk_repository_dir"
    --sdk-repository-max-bytes "${PRDB_SDK_REPOSITORY_MAX_BYTES:-1073741824}"
    --allowed-dependency libc.so.6
    --retention-seconds "${PRDB_RETENTION_SECONDS:-86400}"
    --max-rows "${PRDB_MAX_ROWS:-100000}"
    --max-bytes "${PRDB_MAX_BYTES:-67108864}"
    --cpu-quota-us "${PRDB_CPU_QUOTA_US:-100000}"
    --cpu-period-us "${PRDB_CPU_PERIOD_US:-100000}"
    --memory-max-bytes "${PRDB_MEMORY_MAX_BYTES:-536870912}"
    --max-processes "${PRDB_MAX_PROCESSES:-32}"
)

gateway_args=(
    --bind "$bind_address"
    --control-bind "$control_bind_address"
    --service-socket "$service_socket"
    --service-uid "$(id -u)"
    --allow-plaintext-non-loopback
)

/usr/local/bin/prdb-supervisor "${supervisor_args[@]}" &
supervisor_pid=$!
gateway_pid=
shutdown() {
    trap - TERM INT
    [[ -z "$gateway_pid" ]] || kill -TERM "$gateway_pid" 2>/dev/null || true
    kill -TERM "$supervisor_pid" 2>/dev/null || true
    [[ -z "$gateway_pid" ]] || wait "$gateway_pid" 2>/dev/null || true
    wait "$supervisor_pid" 2>/dev/null || true
}
trap shutdown TERM INT

for ((attempt = 0; attempt < 300; attempt++)); do
    if [[ -S "$service_socket" ]]; then
        break
    fi
    if ! kill -0 "$supervisor_pid" 2>/dev/null; then
        wait "$supervisor_pid"
        exit $?
    fi
    sleep 0.1
done
if [[ ! -S "$service_socket" ]]; then
    echo "prdb-server-entrypoint: supervisor did not create $service_socket" >&2
    kill -TERM "$supervisor_pid" 2>/dev/null || true
    wait "$supervisor_pid" 2>/dev/null || true
    exit 1
fi

/usr/local/bin/prdb-gateway "${gateway_args[@]}" &
gateway_pid=$!
set +e
wait -n "$supervisor_pid" "$gateway_pid"
status=$?
set -e
shutdown
exit "$status"
