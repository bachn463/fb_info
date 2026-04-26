"""Integration test for ingest_pfr against the committed 1985 fixtures.

Uses a fixture-backed scraper shim (no network), runs the merge over
all 8 page parsers, and pins a handful of well-known 1985 player-
seasons against published numbers.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ffpts.ingest_pfr import (
    load_draft_picks,
    load_player_seasons,
    load_team_season_records,
)

FIX_ROOT = Path(__file__).resolve().parent / "fixtures"

# Map URL paths the orchestrator requests to fixture files on disk.
_PATH_TO_FIXTURE = {
    "/years/1985/passing.htm":   FIX_ROOT / "passing"   / "1985.html",
    "/years/1985/rushing.htm":   FIX_ROOT / "rushing"   / "1985.html",
    "/years/1985/receiving.htm": FIX_ROOT / "receiving" / "1985.html",
    "/years/1985/defense.htm":   FIX_ROOT / "defense"   / "1985.html",
    "/years/1985/kicking.htm":   FIX_ROOT / "kicking"   / "1985.html",
    "/years/1985/returns.htm":   FIX_ROOT / "returns"   / "1985.html",
    "/years/1985/draft.htm":     FIX_ROOT / "draft"     / "1985.html",
    "/years/1985/":              FIX_ROOT / "standings" / "1985.html",
    # 2023 too, in case a test wants the modern era.
    "/years/2023/passing.htm":   FIX_ROOT / "passing"   / "2023.html",
    "/years/2023/rushing.htm":   FIX_ROOT / "rushing"   / "2023.html",
    "/years/2023/receiving.htm": FIX_ROOT / "receiving" / "2023.html",
    "/years/2023/defense.htm":   FIX_ROOT / "defense"   / "2023.html",
    "/years/2023/kicking.htm":   FIX_ROOT / "kicking"   / "2023.html",
    "/years/2023/returns.htm":   FIX_ROOT / "returns"   / "2023.html",
    "/years/2023/draft.htm":     FIX_ROOT / "draft"     / "2023.html",
    "/years/2023/":              FIX_ROOT / "standings" / "2023.html",
}


class _FixtureScraper:
    """Duck-typed scraper backed by tests/fixtures/."""
    def __init__(self):
        self.calls: list[str] = []

    def get(self, path: str) -> str:
        self.calls.append(path)
        fixture = _PATH_TO_FIXTURE.get(path)
        if fixture is None:
            raise FileNotFoundError(f"no fixture for path {path!r}")
        return fixture.read_text()


@pytest.fixture(scope="module")
def scraper():
    return _FixtureScraper()


@pytest.fixture(scope="module")
def player_seasons_1985(scraper):
    return load_player_seasons([1985], scraper=scraper)


def _row_for(df, name, team):
    import polars as pl
    matching = df.filter((pl.col("name") == name) & (pl.col("team") == team))
    assert matching.height >= 1, f"no row for {name} ({team})"
    return matching.to_dicts()[0]


def test_player_seasons_1985_payton_full_line(player_seasons_1985):
    """Walter Payton 1985: 1551 rush yds, 9 rush TD, 49 rec, 483 rec yds, 2 rec TD."""
    wp = _row_for(player_seasons_1985, "Walter Payton", "CHI")
    assert wp["rush_yds"] == 1551
    assert wp["rush_td"] == 9
    assert wp["rec"] == 49
    assert wp["rec_yds"] == 483
    assert wp["rec_td"] == 2
    assert wp["position"] == "RB"


def test_player_seasons_1985_marino_passing(player_seasons_1985):
    marino = _row_for(player_seasons_1985, "Dan Marino", "MIA")
    assert marino["pass_yds"] == 4137
    assert marino["pass_td"] == 30
    assert marino["pass_int"] == 21
    assert marino["position"] == "QB"


def test_player_seasons_1985_lawrence_taylor_def(player_seasons_1985):
    lt = _row_for(player_seasons_1985, "Lawrence Taylor", "NYG")
    assert lt["def_sacks"] == pytest.approx(13.0)
    assert lt["def_fumbles_forced"] == 4
    # PFR records LT as a right-outside linebacker (ROLB).
    assert lt["position"] == "ROLB"


def test_player_seasons_1985_kicker_butler(player_seasons_1985):
    kb = _row_for(player_seasons_1985, "Kevin Butler", "CHI")
    assert kb["fg_made"] == 31
    assert kb["fg_att"] == 37
    assert kb["xp_made"] == 51
    # Kicker gets NULL fpts (not a skill position).
    assert kb["fpts_std"] is None


def test_fpts_computed_for_skill_positions(player_seasons_1985):
    """Payton 1985 had a 3/5/96/1/0 passing line on option plays in
    addition to his rushing+receiving lines, so the fpts is:
      pass: 96/25 + 1*4 - 0*2 = 3.84 + 4 = 7.84
      rush: 1551/10 + 9*6 = 155.1 + 54 = 209.1
      rec : 483/10 + 2*6 = 48.3 + 12 = 60.3
      pre-1994 fumbles_lost = 0 (column unavailable)
      std total = 277.24, PPR adds 49 receptions -> 326.24.
    """
    wp = _row_for(player_seasons_1985, "Walter Payton", "CHI")
    expected_std = (
        96 / 25 + 1 * 4
        + 1551 / 10 + 9 * 6
        + 483 / 10 + 2 * 6
    )
    assert wp["fpts_std"] == pytest.approx(expected_std, abs=0.01)
    assert wp["fpts_ppr"] == pytest.approx(expected_std + 49.0, abs=0.01)


def test_pre_1994_has_fumbles_lost_flag_false(player_seasons_1985):
    wp = _row_for(player_seasons_1985, "Walter Payton", "CHI")
    assert wp["has_fumbles_lost"] is False


def test_sources_marker_set_to_pfr(player_seasons_1985):
    wp = _row_for(player_seasons_1985, "Walter Payton", "CHI")
    assert wp["sources"] == "pfr"


def test_player_seasons_returns_many_rows(player_seasons_1985):
    # 1985: ~1500 distinct (player, team) seasons across all 8 pages.
    assert player_seasons_1985.height > 800


def test_player_id_uses_pfr_namespace(player_seasons_1985):
    for pid in player_seasons_1985["player_id"].to_list():
        assert pid.startswith("pfr:")


# --- Draft + standings ---------------------------------------------------


def test_load_draft_picks_returns_full_class(scraper):
    df = load_draft_picks([1985], scraper=scraper)
    assert df.height > 200
    # First overall pick.
    first = df.filter(df["overall_pick"] == 1).to_dicts()[0]
    assert first["round"] == 1
    assert first["team"] == "BUF"
    assert first["name"] == "Bruce Smith"


def test_load_team_season_records_returns_28_for_1985(scraper):
    df = load_team_season_records([1985], scraper=scraper)
    assert df.height == 28
    # Bears were 15-1 that year.
    bears = df.filter(df["franchise"] == "bears").to_dicts()[0]
    assert bears["wins"] == 15
    assert bears["losses"] == 1
