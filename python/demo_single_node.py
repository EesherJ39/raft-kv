"""Concurrent read/write demo + simple linearizability sanity check.


Run against a single node started locally:


python demo_single_node.py --endpoint http://localhost:8080
"""
from __future__ import annotations


import argparse
import random
import threading
import time
from typing import List, Tuple, Optional


from client import KVClient
from linearizability import check_single_register




def main() -> None:
ap = argparse.ArgumentParser()
ap.add_argument("--endpoint", required=True, help="Base URL of a node")
ap.add_argument("--ops", type=int, default=50, help="Ops per thread")
args = ap.parse_args()


client = KVClient(args.endpoint)


# Warmup value so early reads don't come back as None
client.put(b"x", b"0")


history: List[Tuple[int, int, str, bytes, Optional[bytes]]] = []


def writer() -> None:
for i in range(args.ops):
s = now_ms()
client.put(b"x", str(i).encode("utf-8"))
e = now_ms()
history.append((s, e, "put", b"x", str(i).encode("utf-8")))
time.sleep(random.uniform(0.0, 0.02))


def reader() -> None:
for _ in range(args.ops):
s = now_ms()
v = client.get(b"x")
e = now_ms()
history.append((s, e, "get", b"x", v))
time.sleep(random.uniform(0.0, 0.02))


threads = [threading.Thread(target=writer), threading.Thread(target=reader)]
for t in threads:
t.start()
for t in threads:
t.join()


ok, errs = check_single_register(history)
print("Linearizability (single-register heuristic):", "PASS" if ok else "FAIL")
if not ok:
for e in errs[:10]:
print("viol:", e)




def now_ms() -> int:
return int(time.time() * 1000)




if __name__ == "__main__":
main()