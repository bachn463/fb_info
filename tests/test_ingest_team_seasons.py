import polars as pl
import pytest

from ffpts.ingest import (
    TEAM_SEASONS_MIN_YEAR,
    _divisions_for_season,
    build_team_seasons,
)


def _row(df, *, team, season):
    return df.filter(
        (pl.col("team") == team) & (pl.col("season") == season)
    ).to_dicts()[0]


def test_build_returns_31_teams_for_1999_2001():
    # Pre-2002 layout: Cleveland Browns returned in 1999 -> 31 teams,
    # but Houston Texans don't exist yet.
    for season in (1999, 2000, 2001):
        df = build_team_seasons([season])
        assert df.height == 31, f"season {season} should have 31 teams"
        assert "HOU" not in df["team"].to_list()


def test_build_returns_32_teams_for_2002_onwards():
    for season in (2002, 2010, 2020, 2025):
        df = build_team_seasons([season])
        assert df.height == 32, f"season {season} should have 32 teams"


def test_2001_nfc_central_includes_modern_nfc_north_plus_tampa():
    df = build_team_seasons([2001])
    central = df.filter(pl.col("division") == "NFC Central")["team"].to_list()
    assert sorted(central) == sorted(["CHI", "DET", "GB", "MIN", "TB"])


def test_2002_realignment_creates_nfc_north_without_tampa():
    df = build_team_seasons([2002])
    north = df.filter(pl.col("division") == "NFC North")["team"].to_list()
    assert sorted(north) == sorted(["CHI", "DET", "GB", "MIN"])
    # Tampa is now NFC South.
    tb_div = _row(df, team="TB", season=2002)["division"]
    assert tb_div == "NFC South"


def test_seattle_moved_to_nfc_west_in_2002():
    assert _row(build_team_seasons([2001]), team="SEA", season=2001)["division"] == "AFC West"
    assert _row(build_team_seasons([2002]), team="SEA", season=2002)["division"] == "NFC West"


def test_relocated_team_codes_use_period_appropriate_code():
    # Rams: STL through 2015, LAR from 2016
    assert "STL" in build_team_seasons([2015])["team"].to_list()
    assert "LAR" in build_team_seasons([2016])["team"].to_list()
    assert "STL" not in build_team_seasons([2016])["team"].to_list()
    # Chargers: SD through 2016, LAC from 2017
    assert "SD" in build_team_seasons([2016])["team"].to_list()
    assert "LAC" in build_team_seasons([2017])["team"].to_list()
    # Raiders: OAK through 2019, LV from 2020
    assert "OAK" in build_team_seasons([2019])["team"].to_list()
    assert "LV" in build_team_seasons([2020])["team"].to_list()


def test_franchise_key_stable_across_relocation():
    # The Rams franchise key is the same whether the team code is STL or LAR.
    stl = _row(build_team_seasons([2015]), team="STL", season=2015)
    lar = _row(build_team_seasons([2016]), team="LAR", season=2016)
    assert stl["franchise"] == lar["franchise"] == "rams"
    # Same for Raiders (OAK -> LV).
    oak = _row(build_team_seasons([2019]), team="OAK", season=2019)
    lv = _row(build_team_seasons([2020]), team="LV", season=2020)
    assert oak["franchise"] == lv["franchise"] == "raiders"


def test_motivating_query_q2_division_filter_returns_expected_franchises():
    # The plan's Q2 example: "NFC North" 1999-2005. With the historical
    # filter, 1999-2001 rows belong to NFC Central, 2002-2005 to NFC North.
    df = build_team_seasons(range(1999, 2006))

    nfc_central = df.filter(
        (pl.col("division") == "NFC Central")
        & (pl.col("season").is_between(1999, 2005))
    )["team"].unique().to_list()
    nfc_north = df.filter(
        (pl.col("division") == "NFC North")
        & (pl.col("season").is_between(1999, 2005))
    )["team"].unique().to_list()

    assert sorted(nfc_central) == sorted(["CHI", "DET", "GB", "MIN", "TB"])
    assert sorted(nfc_north) == sorted(["CHI", "DET", "GB", "MIN"])

    # Franchise-grouped: union of CHI/DET/GB/MIN modern franchises across
    # the whole window is consistent (Tampa is "buccaneers", not in the
    # modern NFC North franchises).
    nfcn_franchises = (
        df.filter(pl.col("division").is_in(["NFC North", "NFC Central"]))
          .filter(pl.col("team").is_in(["CHI", "DET", "GB", "MIN"]))
          ["franchise"].unique().to_list()
    )
    assert sorted(nfcn_franchises) == sorted(["bears", "lions", "packers", "vikings"])


def test_unsupported_season_raises():
    with pytest.raises(ValueError):
        _divisions_for_season(1998)


def test_min_year_constant_matches_era_table():
    assert TEAM_SEASONS_MIN_YEAR == 1999
    # Sanity: 1999 actually resolves
    _divisions_for_season(1999)


def test_build_for_multi_year_range_concatenates_correctly():
    df = build_team_seasons(range(1999, 2003))
    # 31 + 31 + 31 + 32 = 125 rows.
    assert df.height == 125
    # No (team, season) duplicates.
    assert df.unique(subset=["team", "season"]).height == 125
