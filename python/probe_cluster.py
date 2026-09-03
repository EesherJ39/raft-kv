"""Run concurrent operations against a live cluster and check linearizability."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import random
import threading
import time
from urllib.error import HTTPError, URLError

from client import KVClient
from linearizability import Operation, check_history


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--endpoints",
        default="http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083",
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--operations", type=int, default=80)
    parser.add_argument("--history", type=Path, default=Path("reports/live-history.jsonl"))
    args = parser.parse_args()

    endpoints = [value.strip() for value in args.endpoints.split(",") if value.strip()]
    clients = [KVClient(endpoint) for endpoint in endpoints]
    history: list[Operation] = []
    lock = threading.Lock()
    sequence = 0

    def execute(worker: int, iteration: int) -> None:
        nonlocal sequence
        random_source = random.Random(worker * 1_000_003 + iteration)
        client = clients[random_source.randrange(len(clients))]
        kind = "put" if iteration % 3 != 0 else "get"
        value = f"worker-{worker}-op-{iteration}" if kind == "put" else None
        started = time.monotonic_ns()
        ok = True
        observed = value
        try:
            if kind == "put":
                client.put("register", value or "")
            else:
                observed = client.get("register")
        except (HTTPError, URLError, TimeoutError, OSError):
            ok = False
        ended = time.monotonic_ns()
        with lock:
            sequence += 1
            history.append(Operation(
                str(sequence),
                str(worker),
                kind,
                "register",
                observed,
                started,
                ended,
                ok,
            ))

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(execute, index % args.workers, index)
            for index in range(args.operations)
        ]
        for future in futures:
            future.result()

    args.history.parent.mkdir(parents=True, exist_ok=True)
    with args.history.open("w", encoding="utf-8") as output:
        for operation in sorted(history, key=lambda item: item.start_ns):
            output.write(json.dumps({
                "id": operation.operation_id,
                "process": operation.process,
                "kind": operation.kind,
                "key": operation.key,
                "value": operation.value,
                "start_ns": operation.start_ns,
                "end_ns": operation.end_ns,
                "ok": operation.ok,
            }, sort_keys=True) + "\n")

    result = check_history(history)
    completed = sum(operation.ok for operation in history)
    print(json.dumps({
        "linearizable": result.linearizable,
        "attempted_operations": len(history),
        "completed_operations": completed,
        "explored_states": result.explored_states,
        "history": str(args.history),
    }, sort_keys=True))
    return 0 if result.linearizable else 1


if __name__ == "__main__":
    raise SystemExit(main())
