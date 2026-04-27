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
    assert sorted(central) == sorted(["CHI", "DET", "GNB", "MIN", "TAM"])


def test_2002_realignment_creates_nfc_north_without_tampa():
    df = build_team_seasons([2002])
    north = df.filter(pl.col("division") == "NFC North")["team"].to_list()
    assert sorted(north) == sorted(["CHI", "DET", "GNB", "MIN"])
    # Tampa is now NFC South.
    tb_div = _row(df, team="TAM", season=2002)["division"]
    assert tb_div == "NFC South"


def test_seattle_moved_to_nfc_west_in_2002():
    assert _row(build_team_seasons([2001]), team="SEA", season=2001)["division"] == "AFC West"
    assert _row(build_team_seasons([2002]), team="SEA", season=2002)["division"] == "NFC West"


def test_relocated_team_codes_use_period_appropriate_code():
    # PFR uses 3-letter codes throughout; team codes change with the
    # relocation/rename year.
    # Rams: STL through 2015, LAR from 2016
    assert "STL" in build_team_seasons([2015])["team"].to_list()
    assert "LAR" in build_team_seasons([2016])["team"].to_list()
    assert "STL" not in build_team_seasons([2016])["team"].to_list()
    # Chargers: SDG through 2016, LAC from 2017
    assert "SDG" in build_team_seasons([2016])["team"].to_list()
    assert "LAC" in build_team_seasons([2017])["team"].to_list()
    # Raiders: OAK through 2019, LVR from 2020
    assert "OAK" in build_team_seasons([2019])["team"].to_list()
    assert "LVR" in build_team_seasons([2020])["team"].to_list()


def test_franchise_key_stable_across_relocation():
    # The Rams franchise key is the same whether the team code is STL or LAR.
    stl = _row(build_team_seasons([2015]), team="STL", season=2015)
    lar = _row(build_team_seasons([2016]), team="LAR", season=2016)
    assert stl["franchise"] == lar["franchise"] == "rams"
    # Same for Raiders (OAK -> LVR).
    oak = _row(build_team_seasons([2019]), team="OAK", season=2019)
    lv = _row(build_team_seasons([2020]), team="LVR", season=2020)
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

    # PFR 3-letter codes throughout.
    assert sorted(nfc_central) == sorted(["CHI", "DET", "GNB", "MIN", "TAM"])
    assert sorted(nfc_north) == sorted(["CHI", "DET", "GNB", "MIN"])

    # Franchise-grouped: union of CHI/DET/GNB/MIN modern franchises across
    # the whole window is consistent (Tampa is "buccaneers", not in the
    # modern NFC North franchises).
    nfcn_franchises = (
        df.filter(pl.col("division").is_in(["NFC North", "NFC Central"]))
          .filter(pl.col("team").is_in(["CHI", "DET", "GNB", "MIN"]))
          ["franchise"].unique().to_list()
    )
    assert sorted(nfcn_franchises) == sorted(["bears", "lions", "packers", "vikings"])


def test_unsupported_season_raises():
    # Pre-merger years are out of scope.
    with pytest.raises(ValueError):
        _divisions_for_season(1969)


def test_min_year_constant_matches_era_table():
    assert TEAM_SEASONS_MIN_YEAR == 1970
    # Sanity: the boundary year actually resolves.
    _divisions_for_season(1970)


def test_build_for_multi_year_range_concatenates_correctly():
    df = build_team_seasons(range(1999, 2003))
    # 31 + 31 + 31 + 32 = 125 rows.
    assert df.height == 125
    # No (team, season) duplicates.
    assert df.unique(subset=["team", "season"]).height == 125


# --- Pre-1999 eras --------------------------------------------------------


def test_1970_has_26_teams_and_correct_pre_merger_alignment():
    df = build_team_seasons([1970])
    assert df.height == 26
    teams = set(df["team"].to_list())
    # Patriots are still Boston in 1970.
    assert "BOS" in teams
    assert "NWE" not in teams
    # Colts in Baltimore.
    assert _row(df, team="BAL", season=1970)["franchise"] == "colts"
    # No Tampa Bay or Seattle yet.
    assert "TAM" not in teams
    assert "SEA" not in teams


def test_patriots_renamed_to_nwe_in_1971():
    df = build_team_seasons([1971])
    teams = set(df["team"].to_list())
    assert "BOS" not in teams
    assert "NWE" in teams
    assert _row(df, team="NWE", season=1971)["franchise"] == "patriots"


def test_1976_one_year_swap_tb_and_sea():
    df = build_team_seasons([1976])
    # 26 + 2 = 28 teams.
    assert df.height == 28
    # TB joins AFC West (one-year stint).
    assert _row(df, team="TAM", season=1976)["division"] == "AFC West"
    # SEA joins NFC West (one-year stint).
    assert _row(df, team="SEA", season=1976)["division"] == "NFC West"


def test_1977_swap_to_permanent_homes():
    df = build_team_seasons([1977])
    # SEA settled in AFC West, TB in NFC Central.
    assert _row(df, team="SEA", season=1977)["division"] == "AFC West"
    assert _row(df, team="TAM", season=1977)["division"] == "NFC Central"


def test_raiders_la_move_in_1982_uses_rai_code():
    df = build_team_seasons([1981, 1982])
    teams_1981 = set(df.filter(pl.col("season") == 1981)["team"].to_list())
    teams_1982 = set(df.filter(pl.col("season") == 1982)["team"].to_list())
    assert "OAK" in teams_1981
    assert "RAI" not in teams_1981
    assert "RAI" in teams_1982
    assert "OAK" not in teams_1982
    # Same franchise either way.
    assert _row(df, team="OAK", season=1981)["franchise"] == "raiders"
    assert _row(df, team="RAI", season=1982)["franchise"] == "raiders"


def test_colts_move_to_indianapolis_in_1984():
    df = build_team_seasons([1983, 1984])
    teams_1983 = set(df.filter(pl.col("season") == 1983)["team"].to_list())
    teams_1984 = set(df.filter(pl.col("season") == 1984)["team"].to_list())
    assert "BAL" in teams_1983
    assert "IND" not in teams_1983
    assert "IND" in teams_1984
    assert "BAL" not in teams_1984
    # The 1983 BAL is the Colts (not Ravens — Ravens don't exist until 1996).
    assert _row(df, team="BAL", season=1983)["franchise"] == "colts"


def test_cardinals_eras_resolve_to_cardinals_franchise():
    """STL = Cardinals 1970-1987, PHO = 1988-1993, ARI = 1994+."""
    df = build_team_seasons([1980, 1990, 1995])
    assert _row(df, team="STL", season=1980)["franchise"] == "cardinals"
    assert _row(df, team="PHO", season=1990)["franchise"] == "cardinals"
    assert _row(df, team="ARI", season=1995)["franchise"] == "cardinals"


def test_stl_means_cardinals_pre_1988_but_rams_in_1995_2015():
    """The motivating ambiguity: STL is two different franchises."""
    df_1985 = build_team_seasons([1985])
    df_2010 = build_team_seasons([2010])
    assert _row(df_1985, team="STL", season=1985)["franchise"] == "cardinals"
    assert _row(df_2010, team="STL", season=2010)["franchise"] == "rams"


def test_bal_means_colts_pre_1984_but_ravens_post_1996():
    """The other motivating ambiguity: BAL is also two franchises."""
    df_1980 = build_team_seasons([1980])
    df_2000 = build_team_seasons([2000])
    assert _row(df_1980, team="BAL", season=1980)["franchise"] == "colts"
    assert _row(df_2000, team="BAL", season=2000)["franchise"] == "ravens"


def test_hou_means_oilers_pre_1997_but_texans_post_2002():
    df_1990 = build_team_seasons([1990])
    df_2010 = build_team_seasons([2010])
    # Oilers franchise key is "titans" (same franchise as Tennessee Titans today).
    assert _row(df_1990, team="HOU", season=1990)["franchise"] == "titans"
    assert _row(df_2010, team="HOU", season=2010)["franchise"] == "texans"


def test_oilers_become_oti_in_1997_then_ten_in_1999():
    df = build_team_seasons([1996, 1997, 1999])
    assert _row(df, team="HOU", season=1996)["franchise"] == "titans"
    assert _row(df, team="OTI", season=1997)["franchise"] == "titans"
    assert _row(df, team="TEN", season=1999)["franchise"] == "titans"


def test_1995_expansion_carolina_jacksonville():
    df = build_team_seasons([1995])
    teams = set(df["team"].to_list())
    assert "CAR" in teams
    assert "JAX" in teams
    # 30 teams total in 1995.
    assert df.height == 30
    # Rams now in St. Louis as Rams (different franchise from Cardinals).
    assert _row(df, team="STL", season=1995)["franchise"] == "rams"
    # Raiders back in Oakland.
    assert _row(df, team="OAK", season=1995)["franchise"] == "raiders"


def test_1996_browns_suspended_ravens_added():
    df = build_team_seasons([1996])
    teams = set(df["team"].to_list())
    assert "CLE" not in teams
    assert "BAL" in teams
    # BAL is Ravens now, not Colts (Colts are IND).
    assert _row(df, team="BAL", season=1996)["franchise"] == "ravens"


def test_pre_1999_year_range_total_count():
    # 1970 (26) + 1971-1975 (26 ea x 5 = 130) + 1976 (28) + 1977-1981 (28 x 5 = 140)
    # + 1982-1983 (28 x 2 = 56) + 1984-1987 (28 x 4 = 112) + 1988-1993 (28 x 6 = 168)
    # + 1994 (28) + 1995 (30) + 1996 (30) + 1997-1998 (30 x 2 = 60) = 808
    df = build_team_seasons(range(1970, 1999))
    assert df.height == 26 + 130 + 28 + 140 + 56 + 112 + 168 + 28 + 30 + 30 + 60


def test_q2_motivating_query_full_window_now_returns_pre_1999_rows():
    """With pre-1999 backfill the era table covers, NFC Central rows for
    1990-1998 now resolve. The Q2 query 'NFC North 1990-2005 historical'
    will surface them via division IN ('NFC Central','NFC North') in
    queries.py — verify the team_seasons rows exist."""
    df = build_team_seasons(range(1990, 2006))
    nfc_central_modern = (
        df.filter(pl.col("division") == "NFC Central")
          .filter(pl.col("season").is_between(1990, 1998))
          ["team"].unique().to_list()
    )
    assert sorted(nfc_central_modern) == sorted(["CHI", "DET", "GNB", "MIN", "TAM"])
