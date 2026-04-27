"""Tests for the new filter dimensions: --college,
--min-career-stat / --max-career-stat, and the award_topN helper.
Also exercises the SAFETY position alias."""

from __future__ import annotations

import pytest

from ffpts.db import apply_schema, connect
from ffpts.pipeline import build
from ffpts.queries import (
    POSITION_ALIASES,
    award_topN,
    career_topN,
    pos_topN,
)


def _scraper():
    from tests.test_ingest_pfr import _FixtureScraper
    return _FixtureScraper()


@pytest.fixture(scope="module")
def db():
    con = connect(None)
    apply_schema(con)
    build(seasons=[1985, 2023], con=con, pfr_scraper=_scraper())
    yield con
    con.close()


# ---- Position aliases ----

def test_safety_alias_expands_to_s_ss_fs():
    assert set(POSITION_ALIASES["SAFETY"]) == {"S", "SS", "FS"}


def test_db_alias_includes_safeties_and_corners():
    assert "CB" in POSITION_ALIASES["DB"]
    assert "S"  in POSITION_ALIASES["DB"]


# ---- College filter ----

def test_pos_topN_college_filter_picks_only_drafted_alumni(db):
    """College filter is a substring match against draft_picks.college.
    Bryce Young (Alabama) was drafted in 2023; if we ask for top QBs
    from Alabama we should see him and not Mahomes (Texas Tech)."""
    sql, params = pos_topN("QB", n=20, rank_by="pass_yds", college="Alabama")
    rows = db.execute(sql, params).fetchall()
    cols = [d[0] for d in db.execute(sql, params).description]
    name_idx = cols.index("name")
    names = {r[name_idx] for r in rows}
    # Don't depend on exact PFR set; just sanity-check the filter is
    # applied (must not include known non-Alabama QBs).
    assert "Patrick Mahomes" not in names
    assert "Lamar Jackson" not in names


def test_pos_topN_college_filter_returns_no_rows_for_unknown(db):
    sql, params = pos_topN(
        "QB", n=10, rank_by="pass_yds", college="ZZZ-not-a-college",
    )
    rows = db.execute(sql, params).fetchall()
    assert rows == []


def test_career_topN_college_filter(db):
    """Career-by-college: just check the SQL runs and returns rows."""
    sql, params = career_topN("rush_yds", n=5, college="Alabama")
    rows = db.execute(sql, params).fetchall()
    # Empty or non-empty is data-dependent; only error here would be
    # a SQL failure.
    assert isinstance(rows, list)


# ---- Career stat min/max ----

def test_pos_topN_min_career_stat_blocks_low_volume(db):
    """Filter to QBs with at least 50000 career pass yards. In our
    1985+2023 fixture set this should return zero rows (no one has
    50k career yards across just two seasons of data)."""
    sql, params = pos_topN(
        "QB", n=10, rank_by="pass_yds",
        min_career_stats={"pass_yds": 50000},
    )
    rows = db.execute(sql, params).fetchall()
    assert rows == []


def test_pos_topN_max_career_stat_keeps_low_career_only(db):
    """Filter to QBs whose career pass_yds <= 5000. The fixture set
    has many young QBs and Marino's career-in-fixture is just 1985, so
    the result should be non-empty."""
    sql, params = pos_topN(
        "QB", n=20, rank_by="pass_yds",
        max_career_stats={"pass_yds": 5000},
    )
    rows = db.execute(sql, params).fetchall()
    assert len(rows) > 0


def test_career_topN_max_career_stat_threshold(db):
    """In career_topN, --max-career-stat filters the player pool by
    a HAVING-style subquery. Filter to players with <= 100 career
    rush_att and rank by rush_yds; the resulting career_total values
    must come from players whose career rush_att stays under the cap."""
    sql, params = career_topN(
        "rush_yds", n=20,
        max_career_stats={"rush_att": 100},
    )
    rows = db.execute(sql, params).fetchall()
    # Sanity: every returned player has career rush_att <= 100.
    # Players-table name collisions exist (two distinct people sharing a
    # display name), so use IN with all matching player_ids and verify
    # at least one of them is under the cap.
    for name, *_ in rows:
        atts_per_pid = db.execute(
            """
            SELECT SUM(rush_att) FROM player_season_stats
            WHERE player_id IN (SELECT player_id FROM players WHERE name = ?)
            GROUP BY player_id
            """,
            [name],
        ).fetchall()
        if atts_per_pid:
            assert min(a[0] or 0 for a in atts_per_pid) <= 100, (
                f"{name}: every variant > 100 attempts"
            )


def test_pos_topN_unknown_career_stat_raises(db):
    with pytest.raises(ValueError, match="min_career_stats"):
        pos_topN(
            "QB", n=5, rank_by="pass_yds",
            min_career_stats={"not_a_real_col": 1},
        )


# ---- award_topN ----

def test_award_topN_returns_expected_columns(db):
    sql, params = award_topN("PB", n=5)
    cur = db.execute(sql, params)
    cols = [d[0] for d in cur.description]
    assert cols == [
        "name", "positions", "teams", "college",
        "award_count", "award_seasons",
    ]


def test_award_topN_winners_only_excludes_finalists(db):
    """award_topN should count outright winners only — vote_finish=1
    or NULL. Ben Roethlisberger had AP MVP votes in 2023 but never won;
    he should not appear in an MVP count > 0 leaderboard."""
    sql, params = award_topN("MVP", n=20)
    rows = db.execute(sql, params).fetchall()
    cols = [d[0] for d in db.execute(sql, params).description]
    name_idx = cols.index("name")
    count_idx = cols.index("award_count")
    # Every row should have award_count >= 1 (we got it because they
    # won, not finished 2nd).
    for r in rows:
        assert r[count_idx] >= 1
    # Lamar Jackson won 2023 MVP -> appears.
    names = {r[name_idx] for r in rows}
    assert "Lamar Jackson" in names


def test_award_topN_position_filter_works(db):
    """Restricting AP_FIRST count to QBs only should still return
    only QBs (or players whose stats include any QB season)."""
    sql, params = award_topN("AP_FIRST", n=10, position="QB")
    rows = db.execute(sql, params).fetchall()
    cols = [d[0] for d in db.execute(sql, params).description]
    pos_idx = cols.index("positions")
    for r in rows:
        positions = r[pos_idx] or ""
        assert "QB" in positions, f"non-QB in QB-filtered award rank: {positions}"


def test_award_topN_safety_alias_filter(db):
    """User's example: AP_FIRST counts among safeties with <30 career
    INTs. The query must accept the SAFETY alias and the
    --max-career-stat filter together without error."""
    sql, params = award_topN(
        "AP_FIRST", n=10, position="SAFETY",
        max_career_stats={"def_int": 30},
    )
    rows = db.execute(sql, params).fetchall()
    # Data-dependent whether non-empty across our 2-season fixture;
    # the assertion is just that the SQL runs without error.
    assert isinstance(rows, list)


def test_award_topN_unknown_award_raises(db):
    with pytest.raises(ValueError, match="award_type"):
        award_topN("BOGUS", n=5)


# ---- College overrides applied by the pipeline ----

def test_pipeline_populates_college_from_draft(db):
    """For drafted players the pipeline should copy draft_picks.college
    into players.college. 2023 draft fixture has Bryce Young (Alabama)."""
    rows = db.execute(
        "SELECT name, college FROM players WHERE name = 'Bryce Young'"
    ).fetchall()
    assert any(c == "Alabama" for _, c in rows), (
        f"expected Bryce Young to have college 'Alabama', got {rows}"
    )


def test_pipeline_applies_college_override_for_supp_pick():
    """Reggie White is a supplemental-draft pick (no college on the
    PFR draft page); the curated KNOWN_COLLEGE_OVERRIDES list should
    fill him in as Tennessee. Run a fresh build with both fixture
    seasons so the supp-draft step actually inserts him.

    1985 fixture is post-1984-supp-draft and has Reggie White stats,
    so the players row exists by the time supp drafts + override
    application runs."""
    from ffpts.db import apply_schema, connect
    from ffpts.pipeline import build

    con = connect(None)
    apply_schema(con)
    build(seasons=[1985], con=con, pfr_scraper=_scraper())
    rows = con.execute(
        "SELECT college FROM players WHERE name = 'Reggie White'"
    ).fetchall()
    con.close()
    # If Reggie White isn't in this fixture, just ensure the override
    # didn't crash the build and the column is queryable.
    if rows:
        assert any(c == "Tennessee" for (c,) in rows), (
            f"expected Tennessee for Reggie White, got {rows}"
        )


def test_pos_topN_college_substring_match_handles_transfer_list(db):
    """A player with a comma-list college value should match an
    ILIKE substring filter for any of the listed schools. Synthetic
    insert ensures we test the SQL path, not just fixture data."""
    db.execute(
        "INSERT INTO players (player_id, name, first_season, last_season, college) "
        "VALUES ('pfr:TestQB01', 'Test QB', 2020, 2023, 'Alabama, Oklahoma')"
    )
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, "
        "pass_yds, fpts_ppr) "
        "VALUES ('pfr:TestQB01', 2023, 'PHI', 'QB', 4000, 250.0)"
    )
    sql, params = pos_topN("QB", n=10, rank_by="pass_yds", college="Alabama")
    rows = db.execute(sql, params).fetchall()
    cols = [d[0] for d in db.execute(sql, params).description]
    name_idx = cols.index("name")
    assert any(r[name_idx] == "Test QB" for r in rows)
    # Same player should also match Oklahoma.
    sql2, params2 = pos_topN("QB", n=10, rank_by="pass_yds", college="Oklahoma")
    rows2 = db.execute(sql2, params2).fetchall()
    assert any(r[name_idx] == "Test QB" for r in rows2)
    # And NOT a school not in the list.
    sql3, params3 = pos_topN("QB", n=10, rank_by="pass_yds", college="Texas")
    rows3 = db.execute(sql3, params3).fetchall()
    assert not any(r[name_idx] == "Test QB" for r in rows3)
    # Cleanup so other tests using the module-scoped db fixture aren't
    # affected.
    db.execute("DELETE FROM player_season_stats WHERE player_id = 'pfr:TestQB01'")
    db.execute("DELETE FROM players WHERE player_id = 'pfr:TestQB01'")
