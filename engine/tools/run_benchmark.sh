#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

BENCH_ROOT="${PRDB_BENCH_ROOT:-$HOME/tmp/prdb-bench}"
RUN_DIR="$BENCH_ROOT/runs/r$(date +%s)"
mkdir -p "$BENCH_ROOT/t" "$RUN_DIR/spill" "$RUN_DIR/logs" "$RUN_DIR/queries" "$RUN_DIR/sdk-repo"
chmod 0700 "$RUN_DIR" "$RUN_DIR/spill" "$RUN_DIR/logs"

if [ ! -e "$HOME/t" ] && [ ! -L "$HOME/t" ]; then
    ln -s "$BENCH_ROOT/t" "$HOME/t"
fi
export TMPDIR="$HOME/t"

echo "Building release binaries..."
cargo build --release --workspace --locked

BIN_DIR="$REPO_ROOT/target/release"
PROFILE_DIR="$REPO_ROOT/target/release-profile"
ADMIN_CREDS="$RUN_DIR/admin.creds"
SERVICE_SOCK="$TMPDIR/prdb-bench-$$.sock"
GW_PORT="${PRDB_BENCH_PORT:-15432}"
GW_ADDR="127.0.0.1:$GW_PORT"

echo "Preparing SDK repository..."
"$BIN_DIR/prdb-bench" --prepare-sdk-repo "$RUN_DIR/sdk-repo" "$PROFILE_DIR/sdk/releases/linux-x86_64/x86_64-v1/nixos-x86_64-glibc-2.42/sdk-1-abis1"

echo "Initializing database instance in $RUN_DIR/data..."
rm -rf "$RUN_DIR/data"
"$BIN_DIR/prdb-init" \
    --instance-dir "$RUN_DIR/data" \
    --administrator admin \
    --credential-file "$ADMIN_CREDS" \
    --expires-in-seconds 86400

SUP_PID=""
GW_PID=""
cleanup() {
    if [ -n "$GW_PID" ]; then kill "$GW_PID" 2>/dev/null || true; fi
    if [ -n "$SUP_PID" ]; then kill "$SUP_PID" 2>/dev/null || true; fi
    rm -f "$SERVICE_SOCK"
}
trap cleanup EXIT

RUNTIME_ARGS=()
while IFS= read -r lib_path; do
    [ -n "$lib_path" ] && RUNTIME_ARGS+=(--runtime-read-execute "$lib_path")
done < <(ldd "$BIN_DIR/prdb-executor" | awk '{for(i=1;i<=NF;i++) if($i ~ /^\//) print $i}' | sort -u)

echo "Starting prdb-supervisor under delegated cgroup..."
systemd-run --user --pipe --collect \
    --unit="prdb-bench-sup-$$.service" --slice=prdb-tests.slice \
    --property='Delegate=cpu memory pids' --property="WorkingDirectory=$REPO_ROOT" \
    "$REPO_ROOT/engine/tools/with_delegated_cgroup.sh" \
    sh -c 'exec "$1" "${@:2}" --cgroup-parent "$PRDB_TEST_CGROUP_PARENT"' _ \
    "$BIN_DIR/prdb-supervisor" \
        --instance-dir "$RUN_DIR/data" \
        --service-socket "$SERVICE_SOCK" \
        --gateway-uid "$(id -u)" \
        --executor "$BIN_DIR/prdb-executor" \
        --optimizer "$BIN_DIR/prdb-optimizer" \
        --scratch-parent "$RUN_DIR/logs" \
        --spill-parent "$RUN_DIR/spill" \
        --spill-max-bytes 34359738368 \
        --spill-max-io-bytes 137438953472 \
        --spill-max-streams 128 \
        --spill-max-operations 1048576 \
        --spill-max-response-bytes 1048576 \
        --spill-max-service-bytes 68719476736 \
        --runtime-profile nixos-x86_64-glibc-2.42 \
        --cpu-baseline x86_64-v1 \
        --allowed-dependency libc.so.6 \
        --allowed-dependency libprdb_sql.so.1 \
        --sdk-repository-dir "$RUN_DIR/sdk-repo" \
        --sdk-repository-max-bytes 1073741824 \
        "${RUNTIME_ARGS[@]}" \
        --retention-seconds 3600 \
        --max-rows 1000000 \
        --max-bytes 268435456 \
        --max-total-result-bytes 2147483648 \
        --max-invocation-scan-work 5000000000 \
        --max-principal-reservations 64 \
        --max-pending-submissions 128 \
        --max-active-invocations 32 \
        --max-active-per-principal 32 \
        --max-storage-active-snapshots 32 \
        --cpu-quota-us 800000 \
        --cpu-period-us 100000 \
        --memory-max-bytes 8589934592 \
        --max-processes 64 &
SUP_PID=$!

for _ in $(seq 1 30); do
    if [ -S "$SERVICE_SOCK" ]; then break; fi
    sleep 0.5
done

echo "Starting prdb-gateway on $GW_ADDR..."
"$BIN_DIR/prdb-gateway" \
    --bind "$GW_ADDR" \
    --service-socket "$SERVICE_SOCK" \
    --service-uid "$(id -u)" \
    --connections-per-ip 10000 \
    --workers 64 \
    --queue-capacity 256 \
    --idle-timeout-ms 600000 \
    --max-total-result-bytes 2147483648 &
GW_PID=$!
sleep 1

echo "Running prdb-bench..."
"$BIN_DIR/prdb-bench" \
    --address "$GW_ADDR" \
    --admin-credentials "$ADMIN_CREDS" \
    --prdb-client "$BIN_DIR/prdb-client" \
    --sdk-library "$PROFILE_DIR/sdk/releases/linux-x86_64/x86_64-v1/nixos-x86_64-glibc-2.42/sdk-1-abis1/libprdb_sql.so.1" \
    --include-dir "$PROFILE_DIR/sdk/include" \
    --run-dir "$RUN_DIR" \
    "$@"
