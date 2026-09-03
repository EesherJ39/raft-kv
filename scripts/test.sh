#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-tests.XXXXXX")
trap 'rm -rf "$scratch"' EXIT

mapfile -d '' java_sources < <(
    find "$repo_root/java/src/main/java" "$repo_root/java/src/test/java" -name '*.java' -print0
)
javac --release 17 -Xlint:all -Werror -d "$scratch/classes" "${java_sources[@]}"
java -ea -cp "$scratch/classes" com.example.raftkv.RaftCoreTest
java -ea -cp "$scratch/classes" com.example.raftkv.ChaosHarness 1000
python3 -m unittest discover -s "$repo_root/python" -p 'test_*.py' -v
cmake -S "$repo_root/c" -B "$scratch/native" -DCMAKE_BUILD_TYPE=Release
cmake --build "$scratch/native" --config Release --parallel
