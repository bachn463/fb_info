"""HTTP client for Pro Football Reference with on-disk cache and throttle.

Two operating modes:

1. **Default** (no session config) — sends a polite ffpts User-Agent.
   Sufficient for non-Cloudflare-protected sites; PFR currently rejects
   this with the Cloudflare Turnstile challenge.

2. **Browser-cookie session** — pass ``cookies`` and ``user_agent``
   (typically loaded via ``Scraper.from_session_file``) so requests
   carry a ``cf_clearance`` cookie acquired from a real browser
   session. This is what makes the pre-1999 PFR backfill possible:
   we continue an authenticated browser session via API rather than
   trying to defeat the JS challenge programmatically.

Single entry point: ``Scraper.get(path)`` returns HTML for a PFR path.
Cache hits are free; cache misses sleep at least ``min_interval`` seconds
since the last live fetch and retry 429 / 5xx with exponential backoff.

Cache layout mirrors the URL path under ``cache_dir/`` — e.g. a request
for ``/years/2023/passing.htm`` lands at
``data/cache/years/2023/passing.htm``. A full re-parse never re-fetches:
the cache is the primary defense against PFR rate limits and the only
sane way to iterate on parsing logic.

If Cloudflare returns a Turnstile challenge despite our session cookie
(rotation / expiry / IP shift), ``CloudflareSessionExpired`` is raised
with refresh instructions. The error is **not** retryable — retrying
with the same cookie hits the same wall.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
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
DEFAULT_SESSION_PATH = Path("data/pfr_session.json")

REFRESH_INSTRUCTIONS = (
    "PFR returned a Cloudflare Turnstile challenge despite the session cookie. "
    "Refresh the cookie:\n"
    "  1. Visit https://www.pro-football-reference.com/years/2023/passing.htm "
    "in a real browser and complete any challenge.\n"
    "  2. DevTools -> Application -> Cookies -> copy the cf_clearance value.\n"
    "  3. Console -> navigator.userAgent -> copy that string.\n"
    "  4. Update data/pfr_session.json with both fields.\n"
    "See README -> 'Pre-1999 PFR backfill' for details."
)


class RetryableHTTPError(Exception):
    """Raised on 429 / 5xx so tenacity can back off and retry."""

    def __init__(self, status_code: int, url: str):
        super().__init__(f"retryable HTTP {status_code} from {url}")
        self.status_code = status_code
        self.url = url


class CloudflareSessionExpired(Exception):
    """Raised when PFR returns a Turnstile challenge despite our session cookie.

    Indicates the cf_clearance cookie has rotated, the IP/UA changed, or
    the user never set up the session config. Includes refresh
    instructions in the message so it surfaces clearly in build output.
    """


@dataclass(frozen=True)
class PFRSession:
    """The two values copied out of the user's browser session."""

    cf_clearance: str
    user_agent: str

    @classmethod
    def from_file(cls, path: Path | str = DEFAULT_SESSION_PATH) -> "PFRSession":
        try:
            data = json.loads(Path(path).read_text(encoding="utf-8"))
        except FileNotFoundError as e:
            raise CloudflareSessionExpired(
                f"PFR session file not found at {path}. {REFRESH_INSTRUCTIONS}"
            ) from e
        except json.JSONDecodeError as e:
            raise CloudflareSessionExpired(
                f"PFR session file at {path} is not valid JSON: {e}. "
                f"{REFRESH_INSTRUCTIONS}"
            ) from e
        for key in ("cf_clearance", "user_agent"):
            if not data.get(key):
                raise CloudflareSessionExpired(
                    f"PFR session file at {path} is missing or has empty "
                    f"{key!r}. {REFRESH_INSTRUCTIONS}"
                )
        return cls(cf_clearance=data["cf_clearance"], user_agent=data["user_agent"])


def _looks_like_turnstile(resp: httpx.Response) -> bool:
    """Heuristic: 403 + a Cloudflare interstitial body."""
    if resp.status_code != 403:
        return False
    # Cloudflare's interstitial pages are short and have a stable title.
    head = resp.text[:2000] if resp.text else ""
    return (
        "Just a moment..." in head
        or "challenges.cloudflare.com" in head
        or resp.headers.get("cf-mitigated") == "challenge"
    )


class Scraper:
    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        cache_dir: Path | str = DEFAULT_CACHE_DIR,
        user_agent: str = DEFAULT_USER_AGENT,
        cookies: dict[str, str] | None = None,
        min_interval_s: float = DEFAULT_MIN_INTERVAL_S,
        max_retries: int = 5,
        client: Optional[httpx.Client] = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.cache_dir = Path(cache_dir)
        self.user_agent = user_agent
        self.cookies = dict(cookies) if cookies else {}
        self.min_interval_s = min_interval_s
        self.max_retries = max_retries
        self._client = client or httpx.Client(timeout=DEFAULT_TIMEOUT_S)
        self._owns_client = client is None
        # httpx prefers cookies on the client over per-request, so install
        # them up front regardless of who owns the client.
        for name, value in self.cookies.items():
            self._client.cookies.set(name, value)
        self._clock = clock
        self._sleep = sleep
        self._last_fetch_at: float | None = None

    @classmethod
    def from_session_file(
        cls, session_path: Path | str = DEFAULT_SESSION_PATH, **kwargs
    ) -> "Scraper":
        """Construct a Scraper using cf_clearance + UA from a session file.

        Loads the JSON config produced by the user's one-time browser
        setup (see ``PFRSession.from_file`` and the README). Any kwargs
        forwarded are passed through to ``__init__`` (cache_dir,
        min_interval_s, etc.).
        """
        session = PFRSession.from_file(session_path)
        kwargs.setdefault("user_agent", session.user_agent)
        cookies = kwargs.pop("cookies", {}) or {}
        cookies = {"cf_clearance": session.cf_clearance, **cookies}
        return cls(cookies=cookies, **kwargs)

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
            resp = self._client.get(
                url, headers={"User-Agent": self.user_agent}
            )
        finally:
            # Always update the clock, even on transport errors, so the
            # throttle counts the failed attempt against the budget.
            self._last_fetch_at = self._clock()
        # Cloudflare Turnstile interstitial — not retryable; fail fast
        # with refresh instructions so the user knows what to do.
        if _looks_like_turnstile(resp):
            raise CloudflareSessionExpired(REFRESH_INSTRUCTIONS)
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            raise RetryableHTTPError(resp.status_code, url)
        resp.raise_for_status()
        return resp.text
