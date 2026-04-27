"""End-to-end pipeline test against the committed 1985 PFR fixtures.

Uses the fixture-backed scraper from test_ingest_pfr (no network).
Verifies the full pipeline: era table -> draft picks -> standings W/L
-> player_season_stats -> supplemental drafts -> the motivating
queries.
"""

from __future__ import annotations

import pytest

from ffpts.db import apply_schema, connect
from ffpts.pipeline import build


def _scraper():
    """Fresh fixture-backed scraper for each build() invocation."""
    from tests.test_ingest_pfr import _FixtureScraper
    return _FixtureScraper()


@pytest.fixture
def populated_db():
    con = connect(None)
    apply_schema(con)
    summary = build(seasons=[1985], con=con, pfr_scraper=_scraper())
    yield con, summary
    con.close()


# --- Summary + counts ----------------------------------------------------


def test_build_returns_summary_with_expected_counts(populated_db):
    _, summary = populated_db
    assert summary["seasons"] == [1985]
    # 1985 = 28 teams (pre-1995 expansion).
    assert summary["team_seasons_rows"] == 28
    # 1985 draft fixture has ~240 picks with player slugs.
    assert summary["draft_picks_rows"] > 200
    # 1985 PFR data merges to >800 player-season rows.
    assert summary["player_season_stats_rows"][1985] > 800


def test_player_id_namespace_uniformly_pfr(populated_db):
    """The full-PFR pivot means every players row uses pfr:<slug>."""
    con, _ = populated_db
    non_pfr = con.execute(
        "SELECT COUNT(*) FROM players WHERE player_id NOT LIKE 'pfr:%'"
    ).fetchone()[0]
    assert non_pfr == 0


# --- Real player-seasons ------------------------------------------------


def test_walter_payton_1985_in_v_player_season_full(populated_db):
    con, _ = populated_db
    row = con.execute(
        """
        SELECT name, season, team, position, division, conference, franchise
        FROM   v_player_season_full
        WHERE  name = 'Walter Payton' AND season = 1985 AND team = 'CHI'
        """
    ).fetchone()
    assert row == (
        "Walter Payton", 1985, "CHI", "RB",
        "NFC Central", "NFC", "bears",
    )


def test_v_flex_seasons_includes_walter_payton(populated_db):
    con, _ = populated_db
    rows = con.execute(
        "SELECT name FROM v_flex_seasons "
        "WHERE name = 'Walter Payton' AND season = 1985"
    ).fetchall()
    assert rows == [("Walter Payton",)]


def test_v_flex_seasons_excludes_qb_marino(populated_db):
    """QBs are not FLEX."""
    con, _ = populated_db
    rows = con.execute(
        "SELECT COUNT(*) FROM v_flex_seasons WHERE name = 'Dan Marino'"
    ).fetchone()
    assert rows == (0,)


# --- Q1: FLEX drafted in round 1 (motivating query) ---------------------


def test_q1_jerry_rice_1985_round_1_flex(populated_db):
    """1985 draft R1 P16 SFO → Jerry Rice (WR). 1985 has him as a
    rookie — he should be the only FLEX (RB/WR/TE) drafted R1 in the
    fixture year (other 1985 R1 picks were DE/G/DT/etc.)."""
    con, _ = populated_db
    rows = con.execute(
        """
        SELECT name, team, season, draft_round
        FROM   v_flex_seasons
        WHERE  draft_round = 1
          AND  fpts_ppr IS NOT NULL
        ORDER BY fpts_ppr DESC
        """
    ).fetchall()
    names = [r[0] for r in rows]
    assert "Jerry Rice" in names


# --- Q2: NFC Central INTs (motivating query, 1985 era) ------------------


def test_q2_nfc_central_int_leaders_1985(populated_db):
    con, _ = populated_db
    rows = con.execute(
        """
        SELECT name, team, season, def_int
        FROM   v_player_season_full
        WHERE  division = 'NFC Central'
          AND  def_int IS NOT NULL
          AND  def_int > 0
          AND  season = 1985
        ORDER BY def_int DESC
        LIMIT 10
        """
    ).fetchall()
    assert len(rows) > 0
    # Every row is from a 1985 NFC Central team.
    for name, team, season, def_int in rows:
        assert season == 1985
        assert team in {"CHI", "DET", "GNB", "MIN", "TAM"}
        assert def_int > 0


# --- Idempotency --------------------------------------------------------


def test_pipeline_is_idempotent():
    """Re-running build for the same season replaces rows cleanly —
    no PK collisions, identical row counts."""
    con = connect(None)
    apply_schema(con)
    try:
        build(seasons=[1985], con=con, pfr_scraper=_scraper())
        n_stats_before = con.execute(
            "SELECT COUNT(*) FROM player_season_stats"
        ).fetchone()[0]
        n_drafts_before = con.execute(
            "SELECT COUNT(*) FROM draft_picks"
        ).fetchone()[0]

        build(seasons=[1985], con=con, pfr_scraper=_scraper())
        n_stats_after = con.execute(
            "SELECT COUNT(*) FROM player_season_stats"
        ).fetchone()[0]
        n_drafts_after = con.execute(
            "SELECT COUNT(*) FROM draft_picks"
        ).fetchone()[0]

        assert n_stats_before == n_stats_after
        assert n_drafts_before == n_drafts_after
    finally:
        con.close()


# --- Standings W/L ------------------------------------------------------


def test_pipeline_attaches_team_records_from_standings(populated_db):
    """The Bears went 15-1 in 1985 (Super Bowl champs); Dolphins 12-4."""
    con, _ = populated_db
    bears = con.execute(
        "SELECT wins, losses FROM team_seasons "
        "WHERE franchise = 'bears' AND season = 1985"
    ).fetchone()
    assert bears == (15, 1)
    dolphins = con.execute(
        "SELECT wins, losses FROM team_seasons "
        "WHERE franchise = 'dolphins' AND season = 1985"
    ).fetchone()
    assert dolphins == (12, 4)


# --- Supplemental drafts -----------------------------------------------


def test_supplemental_drafts_inserted_for_1985_players(populated_db):
    """Several supp-draft picks played in 1985:
    - Steve Young (1984 supp R1 TAM, USFL until 1985)
    - Reggie White (1984 supp R1 PHI)
    - Bernie Kosar (1985 supp R1 CLE)
    Each should produce a draft_picks row whose player_id matches the
    1985 PFR slug."""
    con, summary = populated_db
    assert summary["supplemental_draft_rows"] >= 1

    # Steve Young's supp draft entry (1984 R1 TAM).
    young = con.execute(
        """
        SELECT dp.year, dp.round, dp.team
        FROM   draft_picks dp
        JOIN   players p USING (player_id)
        WHERE  p.name = 'Steve Young' AND dp.year = 1984
        """
    ).fetchall()
    assert (1984, 1, "TAM") in young


def test_supplemental_drafts_idempotent():
    """Re-running build doesn't duplicate supp draft rows."""
    con = connect(None)
    apply_schema(con)
    try:
        for _ in range(2):
            build(seasons=[1985], con=con, pfr_scraper=_scraper())
        # Steve Young still has exactly one draft row.
        n = con.execute(
            """
            SELECT COUNT(*) FROM draft_picks dp
            JOIN   players p USING (player_id)
            WHERE  p.name = 'Steve Young' AND dp.year = 1984
            """
        ).fetchone()[0]
        assert n == 1
    finally:
        con.close()
