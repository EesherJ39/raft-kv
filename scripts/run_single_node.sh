#!/usr/bin/env bash
set -euo pipefail


# Convenience runner for a single local node (no Docker).
ROOT=$(cd "$(dirname "$0")/.." && pwd)
LIBDIR="$ROOT/c/build"
JAR="$ROOT/java/build/libs/raftkv-all.jar"
DATA="$ROOT/data/node1"
mkdir -p "$DATA"


if [[ "$(uname)" == "Darwin" ]]; then
LIB=libkvstore.dylib
elif [[ "$(uname -o 2>/dev/null)" == "Msys" ]]; then
LIB=libkvstore.dll
else
LIB=libkvstore.so
fi


java \
-Djava.library.path="$LIBDIR" \
-cp "$JAR" \
com.example.raftkv.Main \
--port 8080 \
--data "$DATA" \
--id node1