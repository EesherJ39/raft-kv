---


## scripts/build_all.sh
```bash
#!/usr/bin/env bash
set -euo pipefail


# Build native lib + Java fat jar locally (use Dockerfile for containerized build).
ROOT=$(cd "$(dirname "$0")/.." && pwd)


pushd "$ROOT/c" >/dev/null
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --config Release
popd >/dev/null


pushd "$ROOT/java" >/dev/null
./gradlew clean shadowJar
popd >/dev/null


echo "Built native lib + Java fat jar."