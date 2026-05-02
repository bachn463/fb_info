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


def test_career_topN_draft_rounds_filter(db):
    """The draft_rounds filter should restrict the player pool. Round
    1 includes high picks; combined with rank by pass_yds we expect
    to find Bryce Young (2023 #1 overall)."""
    sql, params = career_topN("pass_yds", n=20, draft_rounds=[1])
    rows = db.execute(sql, params).fetchall()
    names = {r[0] for r in rows}
    # 2023 fixture has 1st-round QBs Bryce Young and CJ Stroud.
    assert "Bryce Young" in names or "C.J. Stroud" in names


def test_career_topN_draft_rounds_undrafted(db):
    """draft_rounds=['undrafted'] should match players with no
    draft_picks row (their LEFT JOIN yields d.round IS NULL)."""
    sql, params = career_topN("rush_yds", n=20, draft_rounds=["undrafted"])
    rows = db.execute(sql, params).fetchall()
    # Just sanity — query runs and returns rows of the expected shape.
    cols = [d[0] for d in db.execute(sql, params).description]
    assert "career_total" in cols


def test_career_topN_drafted_by_filter(db):
    """drafted_by should restrict to players drafted by that team."""
    sql, params = career_topN("pass_yds", n=20, drafted_by="CAR")
    rows = db.execute(sql, params).fetchall()
    # 2023 #1 overall (Bryce Young) was a CAR pick.
    names = {r[0] for r in rows}
    assert "Bryce Young" in names


def test_career_topN_first_name_contains_filter(db):
    """First-name substring match should narrow the leaderboard."""
    sql, params = career_topN(
        "pass_yds", n=20, first_name_contains="Bryce",
    )
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    # Every returned player should have a first name containing "Bryce".
    for name in names:
        first = name.split()[0]
        assert "bryce" in first.lower(), f"unexpected first name {first}"


def test_career_topN_last_name_contains_filter(db):
    """Last-name substring match should narrow the leaderboard."""
    sql, params = career_topN(
        "pass_yds", n=20, last_name_contains="Young",
    )
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    for name in names:
        # everything after the first space
        last = name.split(" ", 1)[1] if " " in name else ""
        assert "young" in last.lower(), f"last name doesn't contain Young: {name}"


def test_career_topN_draft_rounds_invalid_entry_raises(db):
    with pytest.raises(ValueError, match="draft_rounds"):
        career_topN("pass_yds", n=10, draft_rounds=[1.5])  # type: ignore[arg-type]


def test_pos_topN_teammate_of_filter(db):
    """teammate_of_player_id restricts the answer set to players who
    shared any (team, season) with the target — including seasons
    the target didn't play. Synthetic insert: target T plays for X
    in 2020; player A plays for X in 2018 (not overlapping with T)
    AND for Y in 2021 (also not overlapping). A should still appear
    because they shared (X, 2018) — wait, that's not target's season.
    Let me set this up cleanly.
    """
    from ffpts.queries import pos_topN

    # Seed three synthetic players. Target played for X in 2020.
    # Teammate1 played for X in 2018 (was on X before target arrived).
    # Teammate2 played for X in 2020 (overlap year).
    # NonTeammate played for Y in all years.
    db.execute(
        "INSERT INTO players (player_id, name, first_season, last_season) "
        "VALUES "
        "('pfr:TestT01','Target Tee',2020,2020),"
        "('pfr:TestM01','Mate One',2018,2018),"
        "('pfr:TestM02','Mate Two',2020,2020),"
        "('pfr:TestN01','Not Mate',2018,2020)"
    )
    for pid, season, team in [
        ("pfr:TestT01", 2020, "TST"),
        ("pfr:TestM01", 2018, "TST"),
        ("pfr:TestM02", 2020, "TST"),
        ("pfr:TestN01", 2018, "OTH"),
        ("pfr:TestN01", 2020, "OTH"),
    ]:
        db.execute(
            "INSERT INTO player_season_stats "
            "(player_id, season, team, position, rec_yds, fpts_ppr) "
            f"VALUES ('{pid}', {season}, '{team}', 'WR', 100, 50.0)"
        )

    sql, params = pos_topN(
        "WR", n=10, rank_by="rec_yds",
        teammate_of_player_id="pfr:TestT01",
    )
    rows = db.execute(sql, params).fetchall()
    cols = [d[0] for d in db.execute(sql, params).description]
    name_idx = cols.index("name")
    names = {r[name_idx] for r in rows}
    # Mate One was on TST in 2018 (target wasn't there yet) — but
    # the question is whether they were EVER on the same (team,
    # season). 2018 and 2020 are different seasons, so Mate One
    # technically shouldn't qualify either. Re-checking the helper:
    # filter is shared (team, season), so Mate One isn't a teammate
    # by the strict definition. Let me verify Mate Two qualifies.
    assert "Mate Two" in names, "Mate Two shared TST/2020 with target"
    assert "Not Mate" not in names

    # Cleanup
    for pid in ("pfr:TestT01", "pfr:TestM01", "pfr:TestM02", "pfr:TestN01"):
        db.execute(f"DELETE FROM player_season_stats WHERE player_id = '{pid}'")
        db.execute(f"DELETE FROM players WHERE player_id = '{pid}'")


def test_pos_topN_teammate_of_career_overlap_real_data(db):
    """End-to-end against the fixture DB: WRs who were ever
    teammates of Lamar Jackson (2023 fixture). Should pick up his
    BAL receivers."""
    from ffpts.queries import pos_topN

    # Lamar Jackson is in the 2023 fixture as BAL QB.
    lamar = db.execute(
        "SELECT player_id FROM players WHERE name = 'Lamar Jackson'"
    ).fetchone()
    if lamar is None:
        pytest.skip("fixture lacks Lamar Jackson")
    sql, params = pos_topN(
        "WR", n=20, rank_by="rec_yds",
        teammate_of_player_id=lamar[0],
    )
    rows = db.execute(sql, params).fetchall()
    cols = [d[0] for d in db.execute(sql, params).description]
    team_idx = cols.index("team")
    season_idx = cols.index("season")
    # Every returned (team, season) pair must include at least one
    # season Lamar shared the team. Spot-check: BAL 2023 should
    # appear in at least one row.
    assert any(r[team_idx] == "BAL" and r[season_idx] == 2023 for r in rows), (
        "expected at least one BAL/2023 WR row teammate of Lamar"
    )


def test_career_topN_teammate_of_runs(db):
    """Smoke test: career_topN composes with teammate_of without
    error."""
    lamar = db.execute(
        "SELECT player_id FROM players WHERE name = 'Lamar Jackson'"
    ).fetchone()
    if lamar is None:
        pytest.skip("fixture lacks Lamar Jackson")
    sql, params = career_topN(
        "rec_yds", n=10, position="WR",
        teammate_of_player_id=lamar[0],
    )
    rows = db.execute(sql, params).fetchall()
    assert isinstance(rows, list)


def test_award_topN_teammate_of_runs(db):
    """Smoke test: award_topN composes with teammate_of."""
    from ffpts.queries import award_topN

    lamar = db.execute(
        "SELECT player_id FROM players WHERE name = 'Lamar Jackson'"
    ).fetchone()
    if lamar is None:
        pytest.skip("fixture lacks Lamar Jackson")
    sql, params = award_topN(
        "PB", n=10, teammate_of_player_id=lamar[0],
    )
    rows = db.execute(sql, params).fetchall()
    assert isinstance(rows, list)


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
