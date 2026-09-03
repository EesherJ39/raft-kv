from __future__ import annotations

import unittest

from linearizability import Operation, check_history


def op(
    operation_id: str,
    kind: str,
    value: str | None,
    start: int,
    end: int,
    *,
    key: str = "x",
    ok: bool = True,
) -> Operation:
    return Operation(operation_id, "p1", kind, key, value, start, end, ok)


class LinearizabilityTests(unittest.TestCase):
    def test_sequential_history(self) -> None:
        history = [
            op("w1", "put", "one", 0, 1),
            op("r1", "get", "one", 2, 3),
            op("w2", "put", "two", 4, 5),
            op("r2", "get", "two", 6, 7),
        ]
        self.assertTrue(check_history(history).linearizable)

    def test_stale_read_is_rejected(self) -> None:
        history = [
            op("w1", "put", "one", 0, 1),
            op("w2", "put", "two", 2, 3),
            op("r1", "get", "one", 4, 5),
        ]
        result = check_history(history)
        self.assertFalse(result.linearizable)
        self.assertEqual("x", result.failing_key)

    def test_overlapping_read_can_linearize_before_write(self) -> None:
        history = [
            op("w1", "put", "one", 0, 10),
            op("r1", "get", None, 1, 2),
            op("r2", "get", "one", 11, 12),
        ]
        self.assertTrue(check_history(history).linearizable)

    def test_failed_write_has_no_effect(self) -> None:
        history = [
            op("w1", "put", "one", 0, 1, ok=False),
            op("r1", "get", None, 2, 3),
        ]
        self.assertTrue(check_history(history).linearizable)

    def test_independent_keys_are_decomposed(self) -> None:
        history = [
            op("wx", "put", "one", 0, 2, key="x"),
            op("wy", "put", "two", 0, 2, key="y"),
            op("rx", "get", "one", 3, 4, key="x"),
            op("ry", "get", "two", 3, 4, key="y"),
        ]
        result = check_history(history)
        self.assertTrue(result.linearizable)
        self.assertEqual(4, result.checked_operations)


if __name__ == "__main__":
    unittest.main()
