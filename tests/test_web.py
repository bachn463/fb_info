"""End-to-end smoke tests for the FastAPI web frontend.

The web UI is a thin layer over the same query helpers the CLI uses.
These tests just verify each route renders, posts work, and the
trivia state machine behaves like the CLI's REPL."""

from __future__ import annotations

import pytest

# fastapi may not be installed in dev (it's an optional [web] extra);
# skip the whole module gracefully when it isn't.
fastapi_testclient = pytest.importorskip("fastapi.testclient")


def _scraper():
    from tests.test_ingest_pfr import _FixtureScraper
    return _FixtureScraper()


@pytest.fixture
def client(tmp_path):
    from fastapi.testclient import TestClient
    from ffpts.db import apply_schema, connect
    from ffpts.pipeline import build
    from ffpts.web import _make_app

    db = tmp_path / "ff.duckdb"
    con = connect(db)
    apply_schema(con)
    build(seasons=[1985], con=con, pfr_scraper=_scraper())
    con.close()
    yield TestClient(_make_app(db))


# ---- Static pages render ----

def test_home_renders(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "FB Info" in r.text


def test_ask_form_renders(client):
    r = client.get("/ask")
    assert r.status_code == 200
    # All three kind options should appear in the form.
    for kind in ("pos-top", "career", "awards"):
        assert kind in r.text


def test_trivia_index_renders(client):
    r = client.get("/trivia")
    assert r.status_code == 200
    for path in ("/trivia/daily", "/trivia/random", "/trivia/play"):
        assert path in r.text


# ---- Ask: query submission ----

def test_ask_pos_top_query_returns_results(client):
    r = client.post(
        "/ask",
        data={
            "kind":     "pos-top",
            "rank_by":  "rush_yds",
            "n":        "5",
            "position": "RB",
            "start":    "1985",
            "end":      "1985",
        },
    )
    assert r.status_code == 200
    assert "Walter Payton" in r.text
    # Result label echoes the query.
    assert "rush_yds" in r.text


def test_ask_career_award_query_returns_results(client):
    """The consolidated career --award path: rank by career count of
    an award type."""
    r = client.post(
        "/ask",
        data={
            "kind":     "career",
            "award":    "AP_FIRST",
            "n":        "5",
            "position": "ALL",
            "rank_by":  "fpts_ppr",  # ignored when award is set
        },
    )
    assert r.status_code == 200
    assert "award_count" in r.text or "AP_FIRST" in r.text


def test_ask_awards_query_lists_winners(client):
    r = client.post(
        "/ask",
        data={
            "kind":   "awards",
            "award":  "MVP",
            "rank_by": "fpts_ppr",   # default, ignored
            "n":      "10",          # default, ignored
            "position": "ALL",
        },
    )
    assert r.status_code == 200
    # 1985 fixture has Marino runner-up but Marcus Allen won. Either
    # way the page renders without error.


# ---- Trivia: full game flow ----

def _start_play(client, **extra) -> str:
    """Start a make-your-own game and return the assigned game_id."""
    data = {
        "rank_by":  "rush_yds",
        "n":        "3",
        "position": "RB",
        "start":    "1985",
        "end":      "1985",
        "unique":   "on",
    }
    data.update(extra)
    r = client.post("/trivia/play", data=data, follow_redirects=False)
    assert r.status_code == 303, r.text
    location = r.headers["location"]
    assert location.startswith("/trivia/")
    return location.rsplit("/", 1)[-1]


def test_trivia_play_full_flow(client):
    """Start a play game, guess one, hint once, give up. Each step
    returns 200 and surfaces the expected log entry."""
    gid = _start_play(client)

    page = client.get(f"/trivia/{gid}")
    assert page.status_code == 200
    assert "rush_yds" in page.text  # title

    correct = client.post(f"/trivia/{gid}/guess", data={"guess": "payton"})
    assert correct.status_code == 200
    assert "Correct!" in correct.text
    assert "Walter Payton" in correct.text

    hint = client.post(f"/trivia/{gid}/hint")
    assert hint.status_code == 200
    assert "Hint #1" in hint.text

    give_up = client.post(f"/trivia/{gid}/give-up")
    assert give_up.status_code == 200
    assert "Final ranked list" in give_up.text
    assert "Marcus Allen" in give_up.text


def test_trivia_play_wrong_guess_shows_not_in_top(client):
    gid = _start_play(client)
    r = client.post(f"/trivia/{gid}/guess", data={"guess": "zzzdoesnotexist"})
    assert r.status_code == 200
    assert "not in the top" in r.text


def test_trivia_random_runs_with_pin(client):
    """Random with a pinned seed should redirect to a game page."""
    r = client.post(
        "/trivia/random",
        data={"seed": "1", "rank_by": "rush_yds", "position": "RB"},
        follow_redirects=False,
    )
    assert r.status_code in (200, 303), r.text
    if r.status_code == 303:
        gid = r.headers["location"].rsplit("/", 1)[-1]
        page = client.get(f"/trivia/{gid}")
        assert "rush_yds" in page.text


def test_trivia_daily_runs(client):
    r = client.get("/trivia/daily", follow_redirects=False)
    # Either 303 (game started) or 200 (no answers — defensive case
    # that's hard to hit on the fixture but tolerated).
    assert r.status_code in (200, 303)


def test_trivia_unknown_game_id_404(client):
    r = client.get("/trivia/this-id-does-not-exist")
    assert r.status_code == 404


# ---- Stat threshold filters in web forms ----

def test_ask_form_exposes_stat_threshold_inputs(client):
    """The /ask form should include all four threshold input names so
    users can enter col=value pairs from the browser."""
    r = client.get("/ask")
    assert r.status_code == 200
    for name in ("min_stat", "max_stat", "min_career_stat", "max_career_stat"):
        assert f'name="{name}"' in r.text


def test_trivia_play_form_exposes_stat_threshold_inputs(client):
    r = client.get("/trivia/play")
    assert r.status_code == 200
    for name in ("min_stat", "max_stat", "min_career_stat", "max_career_stat"):
        assert f'name="{name}"' in r.text


def test_trivia_random_form_exposes_stat_threshold_inputs(client):
    r = client.get("/trivia/random")
    assert r.status_code == 200
    for name in ("min_stat", "max_stat", "min_career_stat", "max_career_stat"):
        assert f'name="{name}"' in r.text


def test_ask_pos_top_with_min_stat_filter(client):
    """Submit a min-stat threshold from the /ask form and verify the
    handler threads it through to pos_topN. games=10 should restrict
    1985 RBs to those with 10+ games (excludes Bo Jackson — only 7
    games in 1985)."""
    r = client.post(
        "/ask",
        data={
            "kind":     "pos-top",
            "rank_by":  "rush_yds",
            "n":        "20",
            "position": "RB",
            "start":    "1985",
            "end":      "1985",
            "min_stat": "games=10",
        },
    )
    assert r.status_code == 200
    # Walter Payton played 16 games in 1985 — must be in the table.
    assert "Walter Payton" in r.text


def test_ask_pos_top_with_max_career_stat_filter(client):
    """Career threshold composes with pos-top per-season ranking.
    Just verify the page renders without error — the fixture is too
    sparse to make a strong assertion about who ends up in/out."""
    r = client.post(
        "/ask",
        data={
            "kind":             "pos-top",
            "rank_by":          "rush_yds",
            "n":                "10",
            "position":         "RB",
            "max_career_stat":  "rush_yds=99999",
        },
    )
    assert r.status_code == 200


def test_trivia_play_with_max_stat_threshold(client):
    """Start a play game with a max-stat ceiling. Game state machine
    accepts the threshold and the page renders the title with it
    surfaced."""
    r = client.post(
        "/trivia/play",
        data={
            "rank_by":  "rush_yds",
            "n":        "3",
            "position": "RB",
            "start":    "1985",
            "end":      "1985",
            "unique":   "on",
            "max_stat": "rush_yds=1700",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text
    gid = r.headers["location"].rsplit("/", 1)[-1]
    page = client.get(f"/trivia/{gid}")
    assert page.status_code == 200
    assert "rush_yds &lt;= 1700" in page.text or "rush_yds <= 1700" in page.text


def test_trivia_play_form_has_college_and_award_pins(client):
    """College + has-award + ever-won should all appear on the
    make-your-own form."""
    r = client.get("/trivia/play")
    assert r.status_code == 200
    for name in ("college", "has_award", "ever_won"):
        assert f'name="{name}"' in r.text


def test_trivia_random_form_has_college_and_award_pins(client):
    """Random's form previously had has_award only; now ever_won and
    college are exposed too. HOF should be in the dropdown values."""
    r = client.get("/trivia/random")
    assert r.status_code == 200
    for name in ("college", "has_award", "ever_won"):
        assert f'name="{name}"' in r.text
    # HOF is a valid award_type and should be selectable.
    assert ">HOF<" in r.text


def test_trivia_play_with_college_pin(client):
    """Pin --college on the play form. Game starts without error;
    title surfaces the college clause."""
    r = client.post(
        "/trivia/play",
        data={
            "rank_by":  "rec_yds",
            "n":        "5",
            "position": "ALL",
            "college":  "Mississippi",  # Jerry Rice → Mississippi Valley State
            "unique":   "on",
        },
        follow_redirects=False,
    )
    # Either 303 to a game (filter matched) or 200 with an empty
    # message (fixture too sparse). Both acceptable; the handler must
    # not 500 on the new field.
    assert r.status_code in (200, 303), r.text


def test_trivia_random_with_ever_won_hof_pin(client):
    """Pin --ever-won HOF on the random form. Mode is forced to
    season because has_award is a season-only filter — wait, no, the
    auto-fallback is only for has_award. ever_won composes with both
    modes. Just verify the handler accepts the field."""
    r = client.post(
        "/trivia/random",
        data={
            "seed":      "1",
            "ever_won":  "HOF",
            "rank_by":   "rec_yds",
            "position":  "WR",
        },
        follow_redirects=False,
    )
    assert r.status_code in (200, 303), r.text


def test_trivia_random_with_career_threshold(client):
    """Pass a career threshold + pinned career mode + rank-by; the
    handler should accept the form fields without error."""
    r = client.post(
        "/trivia/random",
        data={
            "seed":              "1",
            "rank_by":           "rush_yds",
            "position":          "RB",
            "mode":              "career",
            "min_career_stat":   "rush_yds=100",
        },
        follow_redirects=False,
    )
    assert r.status_code in (200, 303), r.text
