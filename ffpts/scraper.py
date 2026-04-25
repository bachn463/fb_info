"""HTTP client for Pro Football Reference with on-disk cache and throttle.

DORMANT MODULE. The project pivoted to nflverse (see ffpts.ingest)
after discovering PFR sits behind Cloudflare Turnstile that blocks
all programmatic clients. This module is preserved as ready-to-use
infrastructure if a future PFR backfill (1970-1998) ever becomes
feasible — nothing in the active pipeline imports it.

Single entry point: ``Scraper.get(path)`` returns HTML for a PFR path.
Cache hits are free; cache misses sleep at least ``min_interval`` seconds
since the last live fetch and retry 429 / 5xx with exponential backoff.

Cache layout mirrors the URL path under ``cache_dir/`` — e.g. a request
for ``/years/2023/passing.htm`` lands at
``data/cache/years/2023/passing.htm``. A full re-parse never re-fetches:
the cache is the primary defense against PFR rate limits and the only
sane way to iterate on parsing logic.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Callable, Optional

import httpx
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

DEFAULT_BASE_URL = "https://www.pro-football-reference.com"
DEFAULT_CACHE_DIR = Path("data/cache")
DEFAULT_USER_AGENT = "ffpts/0.1 (+https://github.com/; contact via repo issues)"
DEFAULT_MIN_INTERVAL_S = 5.0
DEFAULT_TIMEOUT_S = 30.0


class RetryableHTTPError(Exception):
    """Raised on 429 / 5xx so tenacity can back off and retry."""

    def __init__(self, status_code: int, url: str):
        super().__init__(f"retryable HTTP {status_code} from {url}")
        self.status_code = status_code
        self.url = url


class Scraper:
    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        cache_dir: Path | str = DEFAULT_CACHE_DIR,
        user_agent: str = DEFAULT_USER_AGENT,
        min_interval_s: float = DEFAULT_MIN_INTERVAL_S,
        max_retries: int = 5,
        client: Optional[httpx.Client] = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.cache_dir = Path(cache_dir)
        self.user_agent = user_agent
        self.min_interval_s = min_interval_s
        self.max_retries = max_retries
        self._client = client or httpx.Client(timeout=DEFAULT_TIMEOUT_S)
        self._owns_client = client is None
        self._clock = clock
        self._sleep = sleep
        self._last_fetch_at: float | None = None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "Scraper":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get(self, path: str) -> str:
        """Return HTML for ``path``. Serves from cache if present."""
        cache_file = self._cache_path_for(path)
        if cache_file.exists():
            return cache_file.read_text(encoding="utf-8")
        html = self._fetch_with_retry(path)
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(html, encoding="utf-8")
        return html

    def is_cached(self, path: str) -> bool:
        return self._cache_path_for(path).exists()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _cache_path_for(self, path: str) -> Path:
        rel = path.lstrip("/")
        if not (rel.endswith(".htm") or rel.endswith(".html")):
            rel = f"{rel}.html"
        return self.cache_dir / rel

    def _throttle(self) -> None:
        if self._last_fetch_at is None:
            return
        elapsed = self._clock() - self._last_fetch_at
        wait = self.min_interval_s - elapsed
        if wait > 0:
            self._sleep(wait)

    def _fetch_with_retry(self, path: str) -> str:
        retrying = Retrying(
            retry=retry_if_exception_type(
                (RetryableHTTPError, httpx.TransportError)
            ),
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            reraise=True,
        )
        for attempt in retrying:
            with attempt:
                return self._fetch_once(path)
        raise RuntimeError("unreachable: tenacity Retrying exited without returning")

    def _fetch_once(self, path: str) -> str:
        self._throttle()
        url = f"{self.base_url}{path if path.startswith('/') else '/' + path}"
        try:
            resp = self._client.get(url, headers={"User-Agent": self.user_agent})
        finally:
            # Always update the clock, even on transport errors, so the
            # throttle counts the failed attempt against the budget.
            self._last_fetch_at = self._clock()
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            raise RetryableHTTPError(resp.status_code, url)
        resp.raise_for_status()
        return resp.text
