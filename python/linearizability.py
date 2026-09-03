"""Backtracking linearizability checker for read/write register histories.

The checker respects real-time precedence and searches every legal ordering of
overlapping operations. Histories are decomposed by key, which is sound for
independent key/value registers and keeps the state space manageable.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable, Sequence


@dataclass(frozen=True, slots=True)
class Operation:
    operation_id: str
    process: str
    kind: str
    key: str
    value: str | None
    start_ns: int
    end_ns: int
    ok: bool = True

    @classmethod
    def from_mapping(cls, raw: dict[str, object]) -> "Operation":
        operation = cls(
            operation_id=str(raw["id"]),
            process=str(raw.get("process", "unknown")),
            kind=str(raw["kind"]),
            key=str(raw["key"]),
            value=None if raw.get("value") is None else str(raw["value"]),
            start_ns=int(raw["start_ns"]),
            end_ns=int(raw["end_ns"]),
            ok=bool(raw.get("ok", True)),
        )
        operation.validate()
        return operation

    def validate(self) -> None:
        if self.kind not in {"put", "get"}:
            raise ValueError(f"unsupported operation kind: {self.kind}")
        if self.end_ns < self.start_ns:
            raise ValueError(f"operation {self.operation_id} ends before it starts")
        if self.kind == "put" and self.value is None:
            raise ValueError(f"put {self.operation_id} has no value")


@dataclass(frozen=True, slots=True)
class CheckResult:
    linearizable: bool
    checked_operations: int
    explored_states: int
    failing_key: str | None = None


def check_history(history: Iterable[Operation]) -> CheckResult:
    successful = [operation for operation in history if operation.ok]
    by_key: dict[str, list[Operation]] = {}
    for operation in successful:
        operation.validate()
        by_key.setdefault(operation.key, []).append(operation)

    explored = 0
    for key, operations in sorted(by_key.items()):
        linearizable, states = _check_register(operations)
        explored += states
        if not linearizable:
            return CheckResult(False, len(successful), explored, key)
    return CheckResult(True, len(successful), explored)


def _check_register(operations: Sequence[Operation]) -> tuple[bool, int]:
    count = len(operations)
    if count == 0:
        return True, 1

    predecessors = [0] * count
    for later, candidate in enumerate(operations):
        mask = 0
        for earlier, other in enumerate(operations):
            if other.end_ns <= candidate.start_ns and earlier != later:
                mask |= 1 << earlier
        predecessors[later] = mask

    all_remaining = (1 << count) - 1
    memo: set[tuple[int, str | None]] = set()
    explored = 0

    def search(remaining: int, register: str | None) -> bool:
        nonlocal explored
        state = (remaining, register)
        if state in memo:
            return False
        explored += 1
        if remaining == 0:
            return True

        for index, operation in enumerate(operations):
            bit = 1 << index
            if remaining & bit == 0:
                continue
            if predecessors[index] & remaining:
                continue
            if operation.kind == "get" and operation.value != register:
                continue
            next_value = operation.value if operation.kind == "put" else register
            if search(remaining ^ bit, next_value):
                return True

        memo.add(state)
        return False

    return search(all_remaining, None), explored


def load_jsonl(path: Path) -> list[Operation]:
    operations: list[Operation] = []
    with path.open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
                if not isinstance(raw, dict):
                    raise ValueError("record is not an object")
                operations.append(Operation.from_mapping(raw))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                raise ValueError(f"invalid history record at line {line_number}: {error}") from error
    return operations


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("history", type=Path, help="JSON Lines operation history")
    args = parser.parse_args()
    result = check_history(load_jsonl(args.history))
    print(json.dumps({
        "linearizable": result.linearizable,
        "checked_operations": result.checked_operations,
        "explored_states": result.explored_states,
        "failing_key": result.failing_key,
    }, sort_keys=True))
    return 0 if result.linearizable else 1


if __name__ == "__main__":
    raise SystemExit(main())
