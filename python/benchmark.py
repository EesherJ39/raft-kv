"""Small reproducible HTTP write benchmark for a running RaftKV cluster."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import platform
import statistics
import time

from client import KVClient


def percentile(sorted_values: list[float], probability: float) -> float:
    if not sorted_values:
        return math.nan
    position = min(len(sorted_values) - 1, math.ceil(probability * len(sorted_values)) - 1)
    return sorted_values[position]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:8081")
    parser.add_argument("--operations", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--value-bytes", type=int, default=128)
    args = parser.parse_args()

    client = KVClient(args.endpoint, timeout_seconds=10)
    value = "x" * args.value_bytes
    for index in range(20):
        client.put(f"warmup-{index}", value)

    def write(index: int) -> float:
        started = time.perf_counter_ns()
        client.put(f"benchmark-{index}", value)
        return (time.perf_counter_ns() - started) / 1_000_000

    wall_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        latencies = list(pool.map(write, range(args.operations)))
    elapsed = time.perf_counter() - wall_started
    ordered = sorted(latencies)
    result = {
        "endpoint": args.endpoint,
        "operations": args.operations,
        "concurrency": args.concurrency,
        "value_bytes": args.value_bytes,
        "throughput_ops_per_second": round(args.operations / elapsed, 2),
        "latency_ms": {
            "median": round(statistics.median(ordered), 3),
            "p95": round(percentile(ordered, 0.95), 3),
            "p99": round(percentile(ordered, 0.99), 3),
        },
        "environment": {
            "os": platform.platform(),
            "python": platform.python_version(),
            "processor": platform.processor(),
        },
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
