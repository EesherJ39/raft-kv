"""Dependency-free HTTP client for RaftKV's public API."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, urljoin
from urllib.request import Request, urlopen


@dataclass(slots=True)
class KVClient:
    endpoint: str
    timeout_seconds: float = 3.0

    def __post_init__(self) -> None:
        self.endpoint = self.endpoint.rstrip("/")

    def status(self) -> dict[str, Any]:
        return self._json("GET", "/v1/status")

    def trace(self) -> list[dict[str, Any]]:
        result = self._json("GET", "/v1/trace")
        if not isinstance(result, list):
            raise RuntimeError("trace response was not a list")
        return result

    def put(self, key: str, value: str) -> int:
        result = self._json(
            "PUT",
            "/v1/kv/" + quote(key, safe=""),
            value.encode("utf-8"),
        )
        return int(result["index"])

    def get(self, key: str) -> str | None:
        try:
            result = self._json("GET", "/v1/kv/" + quote(key, safe=""))
            return str(result["value"])
        except HTTPError as error:
            if error.code == 404:
                return None
            raise

    def _json(self, method: str, path: str, body: bytes | None = None) -> Any:
        url = self.endpoint + path
        for redirect_count in range(4):
            request = Request(
                url,
                data=body,
                method=method,
                headers={"Content-Type": "text/plain; charset=utf-8"},
            )
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    return json.loads(response.read().decode("utf-8"))
            except HTTPError as error:
                if error.code not in (307, 308) or redirect_count == 3:
                    raise
                location = error.headers.get("Location")
                if not location:
                    raise
                url = urljoin(url, location)
        raise RuntimeError("too many leader redirects")
