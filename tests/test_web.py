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
    # Both top-level kinds should appear in the radio selector.
    for kind in ("pos-top", "career"):
        assert kind in r.text
    # The dropped `awards` kind should NOT be in the form.
    assert ' value="awards"' not in r.text


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
            "kind":         "career",
            "career_mode":  "award",
            "career_award": "AP_FIRST",
            "n":            "5",
            "position":     "ALL",
        },
    )
    assert r.status_code == 200
    assert "award_count" in r.text or "AP_FIRST" in r.text


def test_ask_career_award_with_ever_won_composes(client):
    """The user's motivating example: list of CPOY winners who also
    won MVP. award_topN composes ever_won with the count rank, so
    --career_mode=award + ever_won=MVP narrows to that intersection.
    Just verify the handler runs cleanly — fixture is too sparse for
    a content assertion."""
    r = client.post(
        "/ask",
        data={
            "kind":         "career",
            "career_mode":  "award",
            "career_award": "CPOY",
            "ever_won":     "MVP",
            "n":            "10",
            "position":     "ALL",
        },
    )
    assert r.status_code == 200


def test_ask_career_award_with_year_range_composes(client):
    """start/end on award-mode counts only wins inside that range."""
    r = client.post(
        "/ask",
        data={
            "kind":         "career",
            "career_mode":  "award",
            "career_award": "MVP",
            "start":        "2010",
            "end":          "2024",
            "n":            "5",
            "position":     "ALL",
        },
    )
    assert r.status_code == 200


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


def test_ask_form_has_radio_kind_selector(client):
    """The kind toggle should be radio buttons, not a dropdown."""
    r = client.get("/ask")
    assert r.status_code == 200
    assert 'type="radio" name="kind" value="pos-top"' in r.text
    assert 'type="radio" name="kind" value="career"' in r.text
    # Career sub-mode is also a radio.
    assert 'type="radio" name="career_mode" value="rank_by"' in r.text
    assert 'type="radio" name="career_mode" value="award"' in r.text


def test_ask_form_exposes_full_cli_option_set(client):
    """Every CLI option that was missing from the web should now be
    in the /ask form."""
    r = client.get("/ask")
    assert r.status_code == 200
    for name in (
        "drafted_by", "draft_start", "draft_end",
        "tiebreak_by", "show_awards", "show_context",
        "career_mode", "career_award", "career_rank_by",
        "min_seasons",
    ):
        assert f'name="{name}"' in r.text, f"missing on /ask: {name}"


def test_ask_pos_top_show_awards_appends_column(client):
    r = client.post(
        "/ask",
        data={
            "kind":         "pos-top",
            "rank_by":      "rush_yds",
            "n":            "3",
            "position":     "RB",
            "start":        "1985",
            "end":          "1985",
            "show_awards":  "1",
            "career_rank_by": "fpts_ppr",   # form default; ignored
        },
    )
    assert r.status_code == 200
    # The augment-display path adds an "awards" column header.
    assert "<th>awards</th>" in r.text or ">awards<" in r.text


def test_trivia_forms_expose_draft_filters(client):
    """drafted-by / draft-start / draft-end should appear on both
    trivia forms (parity with the CLI flags)."""
    for path in ("/trivia/play", "/trivia/random"):
        r = client.get(path)
        assert r.status_code == 200
        for name in ("drafted_by", "draft_start", "draft_end"):
            assert f'name="{name}"' in r.text, f"missing on {path}: {name}"


def test_trivia_play_with_drafted_by(client):
    """Submit `--drafted-by PIT` on the play form. Game starts (or
    surfaces 'no matching' on a sparse fixture) without erroring."""
    r = client.post(
        "/trivia/play",
        data={
            "rank_by":     "rush_yds",
            "n":           "5",
            "position":    "RB",
            "drafted_by":  "PIT",
            "unique":      "on",
        },
        follow_redirects=False,
    )
    assert r.status_code in (200, 303), r.text


def test_trivia_random_with_draft_year_range(client):
    r = client.post(
        "/trivia/random",
        data={
            "seed":         "1",
            "draft_start":  "1990",
            "draft_end":    "2000",
        },
        follow_redirects=False,
    )
    assert r.status_code in (200, 303), r.text


def test_all_three_forms_expose_draft_rounds(client):
    """draft_rounds input should appear on /ask, /trivia/play, and
    /trivia/random with the same name attribute."""
    for path in ("/ask", "/trivia/play", "/trivia/random"):
        r = client.get(path)
        assert r.status_code == 200
        assert 'name="draft_rounds"' in r.text, f"missing on {path}"


def test_ask_pos_top_with_draft_rounds_filter(client):
    r = client.post(
        "/ask",
        data={
            "kind":         "pos-top",
            "rank_by":      "rush_yds",
            "n":            "20",
            "position":     "RB",
            "start":        "1985",
            "end":          "1985",
            "draft_rounds": "1,2",
        },
    )
    assert r.status_code == 200


def test_trivia_play_with_undrafted_rounds_filter(client):
    """Pass `--draft-rounds undrafted` via the form. Game starts (or
    surfaces "no matching" if the fixture is too sparse) without
    erroring on the new field."""
    r = client.post(
        "/trivia/play",
        data={
            "rank_by":      "rush_yds",
            "n":            "5",
            "position":     "RB",
            "start":        "1985",
            "end":          "1985",
            "unique":       "on",
            "draft_rounds": "undrafted",
        },
        follow_redirects=False,
    )
    assert r.status_code in (200, 303), r.text


def test_trivia_random_with_draft_rounds_filter(client):
    r = client.post(
        "/trivia/random",
        data={
            "seed":         "1",
            "draft_rounds": "1",
        },
        follow_redirects=False,
    )
    assert r.status_code in (200, 303), r.text


def test_trivia_random_position_default_is_blank(client):
    """The random-trivia form's position dropdown must lead with a
    blank option — otherwise the first listed position (FLEX in
    POSITION_ALIASES dict order) becomes a silent default pin and
    the user thinks they're getting a random position when they're
    not."""
    r = client.get("/trivia/random")
    assert r.status_code == 200
    # The position select's first <option> should be the empty value
    # (renders as "(any)"), not FLEX or any concrete position.
    pos_section = r.text.split('name="position"', 1)[1]
    # Look at the first option after the select opens.
    first_option = pos_section.split("<option", 2)[1]
    assert 'value=""' in first_option, f"first option was: {first_option!r}"


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
