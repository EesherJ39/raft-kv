#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-live.XXXXXX")
declare -A pids

cleanup() {
    for pid in "${pids[@]:-}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    rm -rf "$scratch"
}
trap cleanup EXIT

jar="$repo_root/java/build/raftkv.jar"
native_dir="$repo_root/build/native"
if [[ ! -f "$jar" || ! -f "$native_dir/libkvstore.so" ]]; then
    echo "Run scripts/build_all.sh first." >&2
    exit 2
fi

peers="node1=http://127.0.0.1:18081,node2=http://127.0.0.1:18082,node3=http://127.0.0.1:18083"

start_node() {
    local id=$1
    local port=$2
    local data_dir="$scratch/$id"
    mkdir -p "$data_dir"
    NODE_ID="$id" \
    PORT="$port" \
    DATA_DIR="$data_dir" \
    PEERS="$peers" \
    RAFT_SNAPSHOT_THRESHOLD=32 \
        java -Djava.library.path="$native_dir" -jar "$jar" >"$scratch/$id.log" 2>&1 &
    pids["$id"]=$!
}

wait_healthy() {
    local url=$1
    for _ in $(seq 1 100); do
        if curl -fsS "$url/healthz" >/dev/null 2>&1; then return 0; fi
        sleep 0.05
    done
    return 1
}

find_leader() {
    local excluded=${1:-}
    local id port status
    for id in node1 node2 node3; do
        [[ "$id" == "$excluded" ]] && continue
        case "$id" in
            node1) port=18081 ;;
            node2) port=18082 ;;
            node3) port=18083 ;;
        esac
        status=$(curl -fsS "http://127.0.0.1:$port/v1/status" 2>/dev/null || true)
        if [[ "$status" == *'"role":"LEADER"'* ]]; then
            printf '%s|http://127.0.0.1:%s\n' "$id" "$port"
            return 0
        fi
    done
    return 1
}

wait_for_leader() {
    local excluded=${1:-}
    local found
    for _ in $(seq 1 160); do
        if found=$(find_leader "$excluded"); then
            printf '%s\n' "$found"
            return 0
        fi
        sleep 0.05
    done
    return 1
}

start_node node1 18081
start_node node2 18082
start_node node3 18083
wait_healthy http://127.0.0.1:18081
wait_healthy http://127.0.0.1:18082
wait_healthy http://127.0.0.1:18083

leader=$(wait_for_leader)
leader_id=${leader%%|*}
leader_url=${leader#*|}
echo "initial_leader=$leader_id"

python3 "$repo_root/python/probe_cluster.py" \
    --endpoints http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083 \
    --workers 4 \
    --operations 60 \
    --history "$scratch/live-history.jsonl"

python3 "$repo_root/python/benchmark.py" \
    --endpoint "$leader_url" \
    --operations 120 \
    --concurrency 4 \
    --value-bytes 128

old_pid=${pids[$leader_id]}
kill -TERM "$old_pid"
wait "$old_pid" 2>/dev/null || true
unset 'pids[$leader_id]'

failover_started=$(date +%s%N)
replacement=$(wait_for_leader "$leader_id")
failover_ended=$(date +%s%N)
replacement_id=${replacement%%|*}
replacement_url=${replacement#*|}
failover_ms=$(((failover_ended - failover_started) / 1000000))

PYTHONPATH="$repo_root/python" python3 - "$replacement_url" <<'PY'
import sys
from client import KVClient

client = KVClient(sys.argv[1])
client.put("post-failover", "committed")
assert client.get("post-failover") == "committed"
PY

case "$leader_id" in
    node1) start_node node1 18081; old_url=http://127.0.0.1:18081 ;;
    node2) start_node node2 18082; old_url=http://127.0.0.1:18082 ;;
    node3) start_node node3 18083; old_url=http://127.0.0.1:18083 ;;
esac
wait_healthy "$old_url"

caught_up=false
for _ in $(seq 1 160); do
    old_status=$(curl -fsS "$old_url/v1/status" 2>/dev/null || true)
    replacement_status=$(curl -fsS "$replacement_url/v1/status" 2>/dev/null || true)
    old_commit=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["commitIndex"])' "$old_status" 2>/dev/null || true)
    replacement_commit=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["commitIndex"])' "$replacement_status" 2>/dev/null || true)
    if [[ -n "$old_commit" && -n "$replacement_commit" && "$old_commit" -ge "$replacement_commit" ]]; then
        caught_up=true
        break
    fi
    sleep 0.05
done

if [[ "$caught_up" != true ]]; then
    echo "restarted leader did not catch up" >&2
    exit 1
fi

if ! grep -q 'C/JNI WAL enabled' "$scratch/$leader_id.log"; then
    echo "native WAL was not enabled on restarted node" >&2
    exit 1
fi

printf '{"failover_ms":%s,"initial_leader":"%s","replacement_leader":"%s","restarted_node_caught_up":true,"native_wal":true}\n' \
    "$failover_ms" "$leader_id" "$replacement_id"
