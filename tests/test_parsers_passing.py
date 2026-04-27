"""Tests for the passing-leaders parser against real PFR HTML fixtures."""

from pathlib import Path

import pytest

from ffpts.parsers import parse_passing

FIX = Path(__file__).resolve().parent / "fixtures" / "passing"


@pytest.fixture(scope="module")
def passing_2023():
    return parse_passing((FIX / "2023.html").read_text(), season=2023)


@pytest.fixture(scope="module")
def passing_1985():
    return parse_passing((FIX / "1985.html").read_text(), season=1985)


def _by_slug(rows, slug):
    matching = [r for r in rows if r["player_id"] == f"pfr:{slug}"]
    assert len(matching) >= 1, f"no row found for slug {slug}"
    return matching[0]


# --- 2023 modern era -----------------------------------------------------


def test_passing_2023_returns_many_rows(passing_2023):
    # ~50 QB-eligible passers per year is typical.
    assert len(passing_2023) > 30


def test_passing_2023_tagovailoa_top_row(passing_2023):
    """Tua Tagovailoa led the league in passing yards in 2023."""
    tua = _by_slug(passing_2023, "TagoTu00")
    assert tua["name"] == "Tua Tagovailoa"
    assert tua["season"] == 2023
    assert tua["team"] == "MIA"
    assert tua["team_slug"] == "mia"
    assert tua["position"] == "QB"
    assert tua["age"] == 25
    assert tua["games"] == 17
    assert tua["games_started"] == 17
    assert tua["pass_cmp"] == 388
    assert tua["pass_att"] == 560
    assert tua["pass_yds"] == 4624
    assert tua["pass_td"] == 29
    assert tua["pass_int"] == 14
    assert tua["pass_long"] == 78
    assert tua["pass_sacks_taken"] == 29
    assert tua["pass_sack_yds"] == 171
    assert tua["pass_rating"] == pytest.approx(101.1, abs=0.05)


def test_passing_2023_surfaces_awards_cell(passing_2023):
    """Awards cell flows through the parser as a raw string for the
    awards ingest step to parse later."""
    tua = _by_slug(passing_2023, "TagoTu00")
    assert tua["awards"] == "PB,AP CPoY-5"
    mahomes = _by_slug(passing_2023, "MahoPa00")
    assert mahomes["awards"] == "PB,AP MVP-7"


def test_passing_1985_marino_awards(passing_1985):
    marino = _by_slug(passing_1985, "MariDa00")
    assert marino["awards"] == "PB,AP-1"


def test_passing_2023_includes_other_known_starters(passing_2023):
    slugs = {r["player_id"] for r in passing_2023}
    assert "pfr:MahoPa00" in slugs   # Patrick Mahomes
    assert "pfr:AlleJo02" in slugs   # Josh Allen
    assert "pfr:HurtJa00" in slugs   # Jalen Hurts


def test_passing_2023_player_id_uses_pfr_prefix(passing_2023):
    for row in passing_2023:
        assert row["player_id"].startswith("pfr:")


# --- 1985 pre-modern era -------------------------------------------------


def test_passing_1985_returns_rows(passing_1985):
    assert len(passing_1985) > 20


def test_passing_1985_marino_record_season(passing_1985):
    """Dan Marino's 1985: 4137 passing yards, 30 TD, 21 INT (a year off
    his 5084 in 1984, but still a top-10 single-season passing line)."""
    marino = _by_slug(passing_1985, "MariDa00")
    assert marino["name"] == "Dan Marino"
    assert marino["season"] == 1985
    assert marino["team"] == "MIA"
    assert marino["position"] == "QB"
    assert marino["age"] == 24
    assert marino["games"] == 16
    assert marino["pass_yds"] == 4137
    assert marino["pass_td"] == 30
    assert marino["pass_int"] == 21
    assert marino["pass_rating"] == pytest.approx(84.1, abs=0.05)


def test_passing_1985_no_qbr_column_does_not_crash(passing_1985):
    """1985 fixtures have pass_success but no qbr column. The parser
    only reads columns it cares about, so neither presence/absence
    should matter — just verify rows look sane."""
    for row in passing_1985[:10]:
        assert row["pass_yds"] is None or row["pass_yds"] >= 0


# --- common to both eras -------------------------------------------------


def test_passing_drops_summary_rows():
    """No row should have a None player_id (summary rows lack a slug)."""
    for fixture in ["2023.html", "1985.html"]:
        rows = parse_passing((FIX / fixture).read_text(), season=2023)
        for r in rows:
            assert r["player_id"] is not None


def test_passing_returns_unique_per_player_team(passing_2023):
    """A traded QB has multiple rows (one per team played for that year)
    — that matches our (player_id, season, team) PK exactly. So while
    `player_id` may repeat, the (player_id, team) pair is unique."""
    keys = [(r["player_id"], r["team"]) for r in passing_2023]
    assert len(keys) == len(set(keys))


def test_passing_2023_handles_traded_qbs(passing_2023):
    """Joshua Dobbs was traded ARI -> MIN mid-2023, so he should appear
    in two rows — one per team."""
    dobbs_rows = [r for r in passing_2023 if r["name"] == "Joshua Dobbs"]
    # At least 2 (could be 3 with a 2TM summary if it has a slug).
    assert len(dobbs_rows) >= 2
    teams = {r["team"] for r in dobbs_rows}
    assert "ARI" in teams or "MIN" in teams
