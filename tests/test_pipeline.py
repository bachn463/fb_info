"""End-to-end pipeline test: synthetic nflverse rows -> DuckDB.

Uses injected loaders so no network is touched. Asserts row counts,
key joins, and the two motivating queries land sensible answers.
"""

from __future__ import annotations

import polars as pl
import pytest

from ffpts.db import apply_schema, connect
from ffpts.pipeline import build


# Two players + two seasons of stats + matching draft + nothing else.
# The fixtures are intentionally tiny but cover the table joins.

CMC_2017 = {
    "player_id": "00-0033280",
    "player_display_name": "Christian McCaffrey",
    "position": "RB",
    "season": 2017, "recent_team": "CAR", "games": 16,
    "completions": 0, "attempts": 0, "passing_yards": 0, "passing_tds": 0,
    "passing_interceptions": 0, "sacks_suffered": 0, "sack_yards_lost": 0,
    "passing_2pt_conversions": 0,
    "carries": 117, "rushing_yards": 435, "rushing_tds": 2,
    "rushing_2pt_conversions": 0,
    "targets": 113, "receptions": 80, "receiving_yards": 651,
    "receiving_tds": 5, "receiving_2pt_conversions": 0,
    "sack_fumbles": 0, "sack_fumbles_lost": 0,
    "rushing_fumbles": 1, "rushing_fumbles_lost": 0,
    "receiving_fumbles": 0, "receiving_fumbles_lost": 0,
    "def_tackles_solo": 0, "def_tackle_assists": 0, "def_sacks": 0.0,
    "def_interceptions": 0, "def_interception_yards": 0,
    "def_pass_defended": 0, "def_fumbles_forced": 0, "def_safeties": 0,
    "fumble_recovery_opp": 0, "fumble_recovery_yards_opp": 0,
    "fg_made": 0, "fg_att": 0, "fg_long": 0,
    "pat_made": 0, "pat_att": 0,
    "punt_returns": 0, "punt_return_yards": 0,
    "kickoff_returns": 0, "kickoff_return_yards": 0,
}

URLACHER_2002 = {
    # Brian Urlacher 2002, NFC Central -> NFC North in the 2002 realignment.
    # Note: 2002 *is* the first year of the new alignment, so CHI shows
    # division="NFC North" in the team_seasons table.
    "player_id": "00-0019596",
    "player_display_name": "Brian Urlacher",
    "position": "MLB",
    "season": 2002, "recent_team": "CHI", "games": 16,
    "completions": 0, "attempts": 0, "passing_yards": 0, "passing_tds": 0,
    "passing_interceptions": 0, "sacks_suffered": 0, "sack_yards_lost": 0,
    "passing_2pt_conversions": 0,
    "carries": 0, "rushing_yards": 0, "rushing_tds": 0,
    "rushing_2pt_conversions": 0,
    "targets": 0, "receptions": 0, "receiving_yards": 0, "receiving_tds": 0,
    "receiving_2pt_conversions": 0,
    "sack_fumbles": 0, "sack_fumbles_lost": 0,
    "rushing_fumbles": 0, "rushing_fumbles_lost": 0,
    "receiving_fumbles": 0, "receiving_fumbles_lost": 0,
    "def_tackles_solo": 151, "def_tackle_assists": 63, "def_sacks": 4.5,
    "def_interceptions": 1, "def_interception_yards": 13,
    "def_pass_defended": 11, "def_fumbles_forced": 1, "def_safeties": 0,
    "fumble_recovery_opp": 1, "fumble_recovery_yards_opp": 0,
    "fg_made": 0, "fg_att": 0, "fg_long": 0,
    "pat_made": 0, "pat_att": 0,
    "punt_returns": 0, "punt_return_yards": 0,
    "kickoff_returns": 0, "kickoff_return_yards": 0,
}


def fake_player_stats_loader(seasons):
    seasons = list(seasons)
    bag = []
    if 2017 in seasons:
        bag.append(CMC_2017)
    if 2002 in seasons:
        bag.append(URLACHER_2002)
    if not bag:
        # Empty rows of the right shape so transform_player_seasons works.
        bag = [{**CMC_2017, "season": seasons[0], "player_id": "__empty__"}]
    return pl.DataFrame(bag)


def fake_draft_loader():
    return pl.DataFrame(
        [
            {
                "season": 2017, "round": 1, "pick": 8, "team": "CAR",
                "gsis_id": "00-0033280", "pfr_player_id": "McCaCh01",
                "pfr_player_name": "Christian McCaffrey", "position": "RB",
            },
            {
                "season": 2000, "round": 1, "pick": 9, "team": "CHI",
                "gsis_id": "00-0019596", "pfr_player_id": "UrlaBr00",
                "pfr_player_name": "Brian Urlacher", "position": "MLB",
            },
        ]
    )


@pytest.fixture
def populated_db():
    con = connect(None)
    apply_schema(con)
    summary = build(
        seasons=[2002, 2017],
        con=con,
        player_loader=fake_player_stats_loader,
        draft_loader=fake_draft_loader,
    )
    yield con, summary
    con.close()


def test_build_returns_summary_with_expected_counts(populated_db):
    _, summary = populated_db
    assert summary["seasons"] == [2002, 2017]
    # 2002 -> 32 teams; 2017 -> 32 teams.
    assert summary["team_seasons_rows"] == 64
    # Two draft picks, both with valid gsis_id.
    assert summary["draft_picks_rows"] == 2
    assert summary["player_season_stats_rows"] == {2002: 1, 2017: 1}


def test_players_table_has_first_and_last_season_widened(populated_db):
    con, _ = populated_db
    cmc = con.execute(
        "SELECT name, first_season, last_season FROM players WHERE player_id = '00-0033280'"
    ).fetchone()
    assert cmc[0] == "Christian McCaffrey"
    # CMC drafted 2017, played 2017 → both first_season and last_season = 2017.
    assert cmc[1] == 2017
    assert cmc[2] == 2017
    # Urlacher drafted 2000, played 2002 → first_season widens to 2000.
    bu = con.execute(
        "SELECT first_season, last_season FROM players WHERE player_id = '00-0019596'"
    ).fetchone()
    assert bu[0] == 2000
    assert bu[1] == 2002


def test_v_player_season_full_joins_division_and_draft(populated_db):
    con, _ = populated_db
    rows = con.execute(
        """
        SELECT name, season, team, position, division, conference,
               draft_round, draft_overall_pick
        FROM v_player_season_full
        ORDER BY season, name
        """
    ).fetchall()
    # 2 rows: Urlacher 2002 + CMC 2017.
    assert len(rows) == 2
    urlacher = rows[0]
    cmc = rows[1]
    assert urlacher == ("Brian Urlacher", 2002, "CHI", "MLB", "NFC North", "NFC", 1, 9)
    assert cmc == ("Christian McCaffrey", 2017, "CAR", "RB", "NFC South", "NFC", 1, 8)


def test_v_flex_seasons_includes_cmc_excludes_urlacher(populated_db):
    con, _ = populated_db
    rows = con.execute(
        "SELECT name FROM v_flex_seasons ORDER BY name"
    ).fetchall()
    assert rows == [("Christian McCaffrey",)]


def test_pipeline_is_idempotent(populated_db):
    """Re-running build for the same seasons replaces rows, no PK conflict."""
    con, _ = populated_db
    summary2 = build(
        seasons=[2002, 2017],
        con=con,
        player_loader=fake_player_stats_loader,
        draft_loader=fake_draft_loader,
    )
    assert summary2["player_season_stats_rows"] == {2002: 1, 2017: 1}
    # Still only one row each — DELETE-then-INSERT replaced cleanly.
    n = con.execute("SELECT COUNT(*) FROM player_season_stats").fetchone()[0]
    assert n == 2


def test_q2_motivating_query_finds_urlacher_in_nfc_north_2002(populated_db):
    """Q2 in plan: NFC North single-season INTs."""
    con, _ = populated_db
    rows = con.execute(
        """
        SELECT name, season, team, def_int
        FROM   v_player_season_full
        WHERE  division = 'NFC North'
          AND  def_int IS NOT NULL
          AND  def_int > 0
        ORDER BY def_int DESC
        """
    ).fetchall()
    assert rows == [("Brian Urlacher", 2002, "CHI", 1)]


def test_pipeline_tolerates_null_player_display_name():
    """Regression: nflverse rows with NULL player_display_name (and draft
    rows with NULL pfr_player_name) used to crash the players upsert
    because players.name is NOT NULL. The upsert now falls back to the
    player_id when no name is available anywhere in the source."""

    NAMELESS_2023 = {**CMC_2017,
        "player_id": "00-9999999",
        "player_display_name": None,
        "season": 2023,
        "recent_team": "SF",
    }

    def loader_with_null_name(seasons):
        s = list(seasons)[0]
        if s == 2023:
            return pl.DataFrame([NAMELESS_2023])
        return pl.DataFrame([{**CMC_2017, "season": s}])

    def draft_with_null_name():
        return pl.DataFrame([
            {
                "season": 2017, "round": 1, "pick": 8, "team": "CAR",
                "gsis_id": "00-0033280", "pfr_player_id": "McCaCh01",
                "pfr_player_name": "Christian McCaffrey", "position": "RB",
            },
            {
                # Drafted player with NULL pfr_player_name.
                "season": 2018, "round": 6, "pick": 200, "team": "ATL",
                "gsis_id": "00-9999998", "pfr_player_id": None,
                "pfr_player_name": None, "position": "WR",
            },
        ])

    con = connect(None)
    apply_schema(con)
    try:
        # No exception = pass.
        build(seasons=[2023], con=con,
              player_loader=loader_with_null_name,
              draft_loader=draft_with_null_name)
        # Both nameless players ended up with their player_id as the name.
        rows = dict(con.execute(
            "SELECT player_id, name FROM players WHERE name = player_id"
        ).fetchall())
        assert rows == {
            "00-9999999": "00-9999999",
            "00-9999998": "00-9999998",
        }
    finally:
        con.close()


def test_q1_motivating_query_finds_cmc_round_1(populated_db):
    """Q1 in plan: FLEX top-N by single-season fpts, drafted in given round.

    Our fixture has only one R1 FLEX (CMC 2017). The query shape returns
    a player-season row.
    """
    con, _ = populated_db
    rows = con.execute(
        """
        SELECT name, team, season, fpts_ppr, draft_round
        FROM   v_flex_seasons
        WHERE  draft_round = 1
          AND  fpts_ppr IS NOT NULL
        ORDER BY fpts_ppr DESC
        LIMIT 10
        """
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "Christian McCaffrey"
    assert rows[0][1] == "CAR"
    assert rows[0][2] == 2017
    assert rows[0][4] == 1
