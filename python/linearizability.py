"""Tiny, single-key linearizability sanity check for demos.


This is **not** a full checker; it just verifies that each GET observes the
latest completed PUT in a greedy, end-time-ordered pass. It catches common
regressions in simple pipelines.
"""
from __future__ import annotations


from typing import List, Tuple, Optional


# Operation tuple: (start_ms, end_ms, kind, key, value)
# kind: "put" | "get"
# key: bytes (ignored by this single-register model)
# value: bytes for gets (or None if 404), bytes for puts
Op = Tuple[int, int, str, bytes, Optional[bytes]]




def check_single_register(history: List[Op]) -> tuple[bool, list[tuple]]:
"""Greedy check: ensure each GET sees the most recent preceding PUT.


Returns: (ok, errors)
errors: list of tuples with (start_ms, end_ms, expected, observed)
"""
ops = sorted(history, key=lambda x: x[1]) # sort by end time
current: Optional[bytes] = None
ok = True
errs: list[tuple] = []


for start_ms, end_ms, kind, key, value in ops:
if kind == "put":
current = value
else: # get
if value is not None and value != current:
ok = False
errs.append((start_ms, end_ms, current, value))
return ok, errs