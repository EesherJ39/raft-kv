"""Lightweight HTTP client for the RaftKV service.


Usage:
c = KVClient("http://localhost:8080")
c.put(b"hello", b"world")
v = c.get(b"hello")
"""
from __future__ import annotations


from dataclasses import dataclass
from typing import Optional
import requests




@dataclass
class KVClient:
"""Simple wrapper around the service's /put and /get endpoints.


* Keys/values are bytes to avoid accidental encoding surprises.
* A small timeout is used so tests fail fast if a node is down.
"""


endpoint: str
timeout_sec: float = 2.0


def __post_init__(self) -> None:
self.endpoint = self.endpoint.rstrip("/")


def put(self, key: bytes, val: bytes) -> None:
"""PUT key/value via /put?key=..&val=.. . Raises on HTTP error."""
r = requests.post(
f"{self.endpoint}/put",
params={"key": key.decode("utf-8"), "val": val.decode("utf-8")},
timeout=self.timeout_sec,
)
r.raise_for_status()


def get(self, key: bytes) -> Optional[bytes]:
"""GET value for key, or None if not found (HTTP 404)."""
r = requests.get(
f"{self.endpoint}/get",
params={"key": key.decode("utf-8")},
timeout=self.timeout_sec,
)
if r.status_code == 404:
return None
r.raise_for_status()
return r.content