import json
from pathlib import Path

import httpx
import pytest

from ffpts.scraper import (
    CloudflareSessionExpired,
    PFRSession,
    Scraper,
)


@pytest.fixture
def fake_clock():
    """Mutable monotonic clock + sleep recorder."""
    state = {"now": 0.0, "sleeps": []}

    def clock():
        return state["now"]

    def sleep(seconds):
        state["sleeps"].append(seconds)
        state["now"] += seconds

    state["clock"] = clock
    state["sleep"] = sleep
    return state


def make_scraper(tmp_path: Path, fake_clock, **kwargs):
    return Scraper(
        cache_dir=tmp_path / "cache",
        clock=fake_clock["clock"],
        sleep=fake_clock["sleep"],
        **kwargs,
    )


def test_cache_miss_fetches_and_writes(httpx_mock, tmp_path, fake_clock):
    httpx_mock.add_response(
        url="https://www.pro-football-reference.com/years/2023/passing.htm",
        text="<html>passing 2023</html>",
    )
    s = make_scraper(tmp_path, fake_clock)
    out = s.get("/years/2023/passing.htm")
    assert out == "<html>passing 2023</html>"
    cached = tmp_path / "cache" / "years" / "2023" / "passing.htm"
    assert cached.read_text() == "<html>passing 2023</html>"


def test_cache_hit_serves_without_network(tmp_path, fake_clock):
    cache_file = tmp_path / "cache" / "years" / "2023" / "passing.htm"
    cache_file.parent.mkdir(parents=True)
    cache_file.write_text("<html>cached</html>")
    s = make_scraper(tmp_path, fake_clock)
    # No httpx_mock responses registered — a network call would raise.
    assert s.get("/years/2023/passing.htm") == "<html>cached</html>"


def test_throttle_sleeps_min_interval_between_live_fetches(
    httpx_mock, tmp_path, fake_clock
):
    httpx_mock.add_response(
        url="https://www.pro-football-reference.com/years/2023/passing.htm",
        text="A",
    )
    httpx_mock.add_response(
        url="https://www.pro-football-reference.com/years/2023/rushing.htm",
        text="B",
    )
    s = make_scraper(tmp_path, fake_clock, min_interval_s=5.0)
    s.get("/years/2023/passing.htm")
    # First fetch: no throttle (first request).
    assert fake_clock["sleeps"] == []
    s.get("/years/2023/rushing.htm")
    # Second fetch: clock advanced 0 between calls, so we sleep ~5s.
    assert fake_clock["sleeps"] == [pytest.approx(5.0)]


def test_throttle_does_not_sleep_when_interval_already_elapsed(
    httpx_mock, tmp_path, fake_clock
):
    httpx_mock.add_response(
        url="https://www.pro-football-reference.com/a.htm", text="A"
    )
    httpx_mock.add_response(
        url="https://www.pro-football-reference.com/b.htm", text="B"
    )
    s = make_scraper(tmp_path, fake_clock, min_interval_s=5.0)
    s.get("/a.htm")
    fake_clock["now"] += 10.0  # 10s elapse externally
    s.get("/b.htm")
    assert fake_clock["sleeps"] == []  # second fetch needed no throttle


def test_429_is_retried_then_succeeds(httpx_mock, tmp_path, fake_clock):
    url = "https://www.pro-football-reference.com/years/2023/passing.htm"
    httpx_mock.add_response(url=url, status_code=429)
    httpx_mock.add_response(url=url, status_code=429)
    httpx_mock.add_response(url=url, text="<html>ok</html>")
    s = make_scraper(tmp_path, fake_clock, max_retries=5)
    assert s.get("/years/2023/passing.htm") == "<html>ok</html>"


def test_5xx_is_retried(httpx_mock, tmp_path, fake_clock):
    url = "https://www.pro-football-reference.com/years/2023/passing.htm"
    httpx_mock.add_response(url=url, status_code=503)
    httpx_mock.add_response(url=url, text="<html>ok</html>")
    s = make_scraper(tmp_path, fake_clock, max_retries=3)
    assert s.get("/years/2023/passing.htm") == "<html>ok</html>"


def test_404_fails_immediately_without_retry(httpx_mock, tmp_path, fake_clock):
    url = "https://www.pro-football-reference.com/years/2023/missing.htm"
    httpx_mock.add_response(url=url, status_code=404)
    s = make_scraper(tmp_path, fake_clock, max_retries=5)
    with pytest.raises(httpx.HTTPStatusError):
        s.get("/years/2023/missing.htm")
    # Only one request was made — no retry on 404.
    assert len(httpx_mock.get_requests()) == 1


def test_retries_exhausted_raises(httpx_mock, tmp_path, fake_clock):
    url = "https://www.pro-football-reference.com/years/2023/passing.htm"
    # max_retries=3 -> exactly three attempts, all 429.
    for _ in range(3):
        httpx_mock.add_response(url=url, status_code=429)
    s = make_scraper(tmp_path, fake_clock, max_retries=3)
    with pytest.raises(Exception):
        s.get("/years/2023/passing.htm")
    assert len(httpx_mock.get_requests()) == 3


def test_user_agent_header_sent(httpx_mock, tmp_path, fake_clock):
    httpx_mock.add_response(
        url="https://www.pro-football-reference.com/x.htm", text="ok"
    )
    s = make_scraper(tmp_path, fake_clock, user_agent="ffpts-test/1.0")
    s.get("/x.htm")
    sent = httpx_mock.get_requests()[0]
    assert sent.headers["User-Agent"] == "ffpts-test/1.0"


def test_is_cached_reflects_cache_state(tmp_path, fake_clock):
    s = make_scraper(tmp_path, fake_clock)
    assert s.is_cached("/years/2023/passing.htm") is False
    cache_file = tmp_path / "cache" / "years" / "2023" / "passing.htm"
    cache_file.parent.mkdir(parents=True)
    cache_file.write_text("x")
    assert s.is_cached("/years/2023/passing.htm") is True


# --- Browser-cookie session support ----------------------------------


def test_cookies_attached_to_request(httpx_mock, tmp_path, fake_clock):
    httpx_mock.add_response(
        url="https://www.pro-football-reference.com/x.htm", text="ok"
    )
    s = make_scraper(
        tmp_path, fake_clock,
        cookies={"cf_clearance": "CFTOKEN_ABC", "extra": "v"},
    )
    s.get("/x.htm")
    sent = httpx_mock.get_requests()[0]
    cookie_header = sent.headers.get("Cookie", "")
    assert "cf_clearance=CFTOKEN_ABC" in cookie_header
    assert "extra=v" in cookie_header


def test_no_cookies_means_no_cookie_header(httpx_mock, tmp_path, fake_clock):
    httpx_mock.add_response(
        url="https://www.pro-football-reference.com/x.htm", text="ok"
    )
    s = make_scraper(tmp_path, fake_clock)
    s.get("/x.htm")
    sent = httpx_mock.get_requests()[0]
    assert "Cookie" not in sent.headers


def test_turnstile_response_raises_session_expired(httpx_mock, tmp_path, fake_clock):
    """A 403 with a Cloudflare interstitial body raises with refresh
    instructions (not retried — would just hit the same wall)."""
    httpx_mock.add_response(
        url="https://www.pro-football-reference.com/years/1985/passing.htm",
        status_code=403,
        text="<!DOCTYPE html><html><head><title>Just a moment...</title></head></html>",
    )
    s = make_scraper(
        tmp_path, fake_clock,
        cookies={"cf_clearance": "stale_token"},
        max_retries=5,
    )
    with pytest.raises(CloudflareSessionExpired) as ei:
        s.get("/years/1985/passing.htm")
    assert "cf_clearance" in str(ei.value)
    # Exactly one request — not retried.
    assert len(httpx_mock.get_requests()) == 1


def test_turnstile_detected_via_cf_mitigated_header(httpx_mock, tmp_path, fake_clock):
    httpx_mock.add_response(
        url="https://www.pro-football-reference.com/x.htm",
        status_code=403,
        text="<html>some other 403 body</html>",
        headers={"cf-mitigated": "challenge"},
    )
    s = make_scraper(tmp_path, fake_clock)
    with pytest.raises(CloudflareSessionExpired):
        s.get("/x.htm")


def test_pfr_session_loads_from_file(tmp_path):
    cfg = tmp_path / "session.json"
    cfg.write_text(json.dumps({"cf_clearance": "ABC", "user_agent": "Mozilla/5.0 ..."}))
    sess = PFRSession.from_file(cfg)
    assert sess.cf_clearance == "ABC"
    assert sess.user_agent == "Mozilla/5.0 ..."


def test_pfr_session_missing_file_raises_with_instructions(tmp_path):
    with pytest.raises(CloudflareSessionExpired) as ei:
        PFRSession.from_file(tmp_path / "nope.json")
    assert "session file not found" in str(ei.value).lower()
    assert "cf_clearance" in str(ei.value)


def test_pfr_session_missing_keys_raises(tmp_path):
    cfg = tmp_path / "session.json"
    cfg.write_text(json.dumps({"cf_clearance": "ABC"}))  # no user_agent
    with pytest.raises(CloudflareSessionExpired) as ei:
        PFRSession.from_file(cfg)
    assert "user_agent" in str(ei.value)


def test_pfr_session_invalid_json_raises(tmp_path):
    cfg = tmp_path / "session.json"
    cfg.write_text("{not json")
    with pytest.raises(CloudflareSessionExpired):
        PFRSession.from_file(cfg)


def test_scraper_from_session_file_constructs_with_cookie_and_ua(
    httpx_mock, tmp_path, fake_clock
):
    cfg = tmp_path / "session.json"
    cfg.write_text(json.dumps({
        "cf_clearance": "TOKEN_FROM_FILE",
        "user_agent": "Mozilla/5.0 (real browser UA)",
    }))
    httpx_mock.add_response(
        url="https://www.pro-football-reference.com/x.htm", text="ok"
    )
    s = Scraper.from_session_file(
        cfg,
        cache_dir=tmp_path / "cache",
        clock=fake_clock["clock"],
        sleep=fake_clock["sleep"],
    )
    s.get("/x.htm")
    sent = httpx_mock.get_requests()[0]
    assert sent.headers["User-Agent"] == "Mozilla/5.0 (real browser UA)"
    assert "cf_clearance=TOKEN_FROM_FILE" in sent.headers.get("Cookie", "")
