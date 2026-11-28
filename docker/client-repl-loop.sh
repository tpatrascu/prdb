#!/bin/bash
set -euo pipefail

server_host=${PRDB_SERVER_HOST:-server}
server_port=${PRDB_SERVER_PORT:-8765}
credentials_file=${PRDB_CREDENTIAL_FILE:-/var/lib/prdb/admin.credentials}
default_user=${PRDB_DEFAULT_USER:-prdb_admin}
schema_name=${PRDB_SCHEMA:-public}
scope_file=${PRDB_SCOPE_FILE:-/work/scope.psc1}
compiler_identity=${PRDB_COMPILER_IDENTITY:-tcc-container-debian-bookworm}
reconnect_delay=${PRDB_RECONNECT_DELAY_SECONDS:-2}

stop_requested=0
trap 'stop_requested=1' TERM INT

resolve_server_ip() {
    getent ahostsv4 "$server_host" | awk 'NR==1 { print $1 }'
}

echo "Waiting for credentials at $credentials_file..."
while (( !stop_requested )) && [[ ! -r "$credentials_file" ]]; do
    sleep 1
done

while (( !stop_requested )); do
    server_ip=$(resolve_server_ip || true)
    if [[ -z "$server_ip" ]]; then
        echo "Waiting for DNS resolution of $server_host..." >&2
        sleep "$reconnect_delay"
        continue
    fi

    address="${server_ip}:${server_port}"

    if [[ ! -s "$scope_file" ]]; then
        printf 'CREATE SCHEMA IF NOT EXISTS %s;\n' "$schema_name" | /usr/local/bin/prdb-client admin-sql \
            --address "$address" \
            --credentials "$credentials_file" \
            --sql-file - >/dev/null 2>&1 || true
        if ! /usr/local/bin/prdb-client scope \
            --address "$address" \
            --credentials "$credentials_file" \
            --schema "$schema_name" \
            --output "$scope_file"; then
            echo "Failed to fetch scope from $address; retrying in ${reconnect_delay}s..." >&2
            sleep "$reconnect_delay"
            continue
        fi
    fi

    echo "Connecting interactive SQL REPL to $address as $default_user..."
    /usr/local/bin/prdb-client interactive-sql \
        --address "$address" \
        --credentials "$credentials_file" \
        --default-user "$default_user" \
        --schema "$schema_name" \
        --scope "$scope_file" \
        --sdk-library "$PRDB_SDK_LIBRARY" \
        --include-dir "$PRDB_INCLUDE_DIR" \
        --sdk-release "$PRDB_SDK_RELEASE" \
        --cpu-baseline "$PRDB_CPU_BASELINE" \
        --runtime-profile "$PRDB_RUNTIME_PROFILE" \
        --compiler-identity "$compiler_identity" || true

    if (( stop_requested )); then
        break
    fi

    echo "Disconnected from $address; reconnecting in ${reconnect_delay}s..." >&2
    sleep "$reconnect_delay"
done
