import polars as pl
import pytest

from ffpts.ingest import load_player_seasons, transform_player_seasons
from ffpts.scoring import StatLine, fantasy_points


# Real CMC 2023 numbers — used in test_scoring's golden test too.
CMC_2023 = {
    "player_id": "00-0033280",
    "player_display_name": "Christian McCaffrey",
    "position": "RB",
    "season": 2023,
    "recent_team": "SF",
    "games": 16,
    "completions": 0,
    "attempts": 0,
    "passing_yards": 0,
    "passing_tds": 0,
    "passing_interceptions": 0,
    "sacks_suffered": 0,
    "sack_yards_lost": 0,
    "passing_2pt_conversions": 0,
    "carries": 272,
    "rushing_yards": 1459,
    "rushing_tds": 14,
    "rushing_2pt_conversions": 0,
    "targets": 83,
    "receptions": 67,
    "receiving_yards": 564,
    "receiving_tds": 7,
    "receiving_2pt_conversions": 0,
    # fumbles split across categories: total = 0 sack + 1 rush + 0 rec = 1
    "sack_fumbles": 0,
    "sack_fumbles_lost": 0,
    "rushing_fumbles": 1,
    "rushing_fumbles_lost": 1,
    "receiving_fumbles": 0,
    "receiving_fumbles_lost": 0,
    # defensive columns -- all zero for an offensive player
    "def_tackles_solo": 0,
    "def_tackle_assists": 0,
    "def_sacks": 0.0,
    "def_interceptions": 0,
    "def_interception_yards": 0,
    "def_pass_defended": 0,
    "def_fumbles_forced": 0,
    "def_safeties": 0,
    "fumble_recovery_opp": 0,
    "fumble_recovery_yards_opp": 0,
    # kicking / returns
    "fg_made": 0, "fg_att": 0, "fg_long": 0,
    "pat_made": 0, "pat_att": 0,
    "punt_returns": 0, "punt_return_yards": 0,
    "kickoff_returns": 0, "kickoff_return_yards": 0,
}


# A defender (CB) with INTs — exercises NULL fpts for non-skill positions.
SAUCE_2023 = {
    "player_id": "00-0037243",
    "player_display_name": "Sauce Gardner",
    "position": "CB",
    "season": 2023,
    "recent_team": "NYJ",
    "games": 17,
    # offensive cols all 0
    "completions": 0, "attempts": 0, "passing_yards": 0, "passing_tds": 0,
    "passing_interceptions": 0, "sacks_suffered": 0, "sack_yards_lost": 0,
    "passing_2pt_conversions": 0,
    "carries": 0, "rushing_yards": 0, "rushing_tds": 0, "rushing_2pt_conversions": 0,
    "targets": 0, "receptions": 0, "receiving_yards": 0, "receiving_tds": 0,
    "receiving_2pt_conversions": 0,
    "sack_fumbles": 0, "sack_fumbles_lost": 0,
    "rushing_fumbles": 0, "rushing_fumbles_lost": 0,
    "receiving_fumbles": 0, "receiving_fumbles_lost": 0,
    # defense
    "def_tackles_solo": 39, "def_tackle_assists": 22,
    "def_sacks": 0.0,
    "def_interceptions": 2,
    "def_interception_yards": 53,
    "def_pass_defended": 14,
    "def_fumbles_forced": 1,
    "def_safeties": 0,
    "fumble_recovery_opp": 0, "fumble_recovery_yards_opp": 0,
    "fg_made": 0, "fg_att": 0, "fg_long": 0,
    "pat_made": 0, "pat_att": 0,
    "punt_returns": 0, "punt_return_yards": 0,
    "kickoff_returns": 0, "kickoff_return_yards": 0,
}


def _sample_df(*rows):
    return pl.DataFrame(list(rows))


def test_transform_renames_known_columns_and_keeps_player_id():
    df = transform_player_seasons(_sample_df(CMC_2023))
    assert "player_id" in df.columns
    assert "team" in df.columns
    assert "rec_yds" in df.columns
    assert "def_int" in df.columns
    assert df["player_id"][0] == "00-0033280"
    assert df["team"][0] == "SF"
    assert df["rec_yds"][0] == 564


def test_transform_aggregates_fumbles_across_categories():
    df = transform_player_seasons(_sample_df(CMC_2023))
    # CMC: 0 sack + 1 rush + 0 rec = 1 fumble; same for fumbles_lost.
    assert df["fumbles"][0] == 1
    assert df["fumbles_lost"][0] == 1


def test_transform_computes_combined_tackles():
    df = transform_player_seasons(_sample_df(SAUCE_2023))
    # Solo 39 + assists 22 = 61 combined.
    assert df["def_tackles_combined"][0] == 61


def test_transform_fpts_match_scoring_module_for_skill_position():
    df = transform_player_seasons(_sample_df(CMC_2023))
    expected = fantasy_points(
        StatLine(
            rush_yds=1459, rush_td=14,
            rec=67, rec_yds=564, rec_td=7,
            fumbles_lost=1,
        ),
        scoring="ppr",
    )
    assert df["fpts_ppr"][0] == pytest.approx(expected, abs=0.01)
    # Half-PPR sanity: PPR - 0.5 * receptions
    assert df["fpts_half"][0] == pytest.approx(expected - 0.5 * 67, abs=0.01)
    # Std: PPR - 1.0 * receptions
    assert df["fpts_std"][0] == pytest.approx(expected - 1.0 * 67, abs=0.01)


def test_transform_leaves_fpts_null_for_non_skill_positions():
    df = transform_player_seasons(_sample_df(SAUCE_2023))
    assert df["fpts_std"][0] is None
    assert df["fpts_half"][0] is None
    assert df["fpts_ppr"][0] is None


def test_transform_preserves_defensive_int_for_defender():
    df = transform_player_seasons(_sample_df(SAUCE_2023))
    assert df["def_int"][0] == 2
    assert df["def_int_yds"][0] == 53


def test_transform_handles_missing_optional_columns():
    # Drop a defensive column entirely; transform should still work and
    # emit NULL for that schema column.
    minimal = {k: v for k, v in CMC_2023.items() if k != "def_pass_defended"}
    df = transform_player_seasons(_sample_df(minimal))
    assert df["def_pass_def"][0] is None


def test_transform_marks_provenance_flags():
    df = transform_player_seasons(_sample_df(CMC_2023))
    assert df["sources"][0] == "nflverse"
    assert df["has_fumbles_lost"][0] is True


def test_load_player_seasons_uses_injected_loader():
    seen = {}

    def fake_loader(seasons):
        seen["seasons"] = list(seasons)
        return _sample_df(CMC_2023)

    df = load_player_seasons([2023], loader=fake_loader)
    assert seen["seasons"] == [2023]
    assert df.height == 1
    assert df["name"][0] == "Christian McCaffrey"


def test_transform_handles_multiple_rows():
    df = transform_player_seasons(_sample_df(CMC_2023, SAUCE_2023))
    assert df.height == 2
    assert sorted(df["position"].to_list()) == ["CB", "RB"]
