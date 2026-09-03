#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$repo_root/build/native" "$repo_root/java/build/classes"

cmake -S "$repo_root/c" -B "$repo_root/build/native" -DCMAKE_BUILD_TYPE=Release
cmake --build "$repo_root/build/native" --config Release --parallel

mapfile -d '' java_sources < <(find "$repo_root/java/src/main/java" -name '*.java' -print0)
javac --release 17 -Xlint:all -Werror \
    -d "$repo_root/java/build/classes" \
    "${java_sources[@]}"
jar --create --file "$repo_root/java/build/raftkv.jar" \
    --main-class com.example.raftkv.Main \
    -C "$repo_root/java/build/classes" .

echo "Built native library and java/build/raftkv.jar"
