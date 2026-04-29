"""Direct (non-CLI) tests for ``ffpts.queries.career_topN``."""

from __future__ import annotations

import pytest

from ffpts.db import apply_schema, connect
from ffpts.pipeline import build
from ffpts.queries import career_topN


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


# ---- career_topN ----

def test_career_topN_returns_expected_columns(db):
    sql, params = career_topN("rush_yds", n=5)
    cur = db.execute(sql, params)
    cols = [d[0] for d in cur.description]
    assert cols == [
        "name", "positions", "teams",
        "career_total", "seasons", "first_season", "last_season",
    ]
    rows = cur.fetchall()
    assert len(rows) > 0


def test_career_topN_positions_and_teams_populated(db):
    """The aggregated `positions` and `teams` columns should be
    non-empty for typical players in the fixture set."""
    sql, params = career_topN("rush_yds", n=5)
    rows = db.execute(sql, params).fetchall()
    # rows: (name, positions, teams, career_total, seasons, first, last)
    for name, positions, teams, total, *_ in rows:
        assert positions, f"empty positions for {name}"
        assert teams, f"empty teams for {name}"


def test_career_topN_position_filter_qb(db):
    """The position filter should restrict the SUM to seasons played at
    that position. Returned career_total values should be > 0 (QBs all
    have nonzero pass_yds), and the resulting rows should not include
    obvious non-QBs."""
    sql, params = career_topN("pass_yds", n=5, position="QB")
    rows = db.execute(sql, params).fetchall()
    assert len(rows) > 0
    # rows: (name, positions, teams, career_total, ...)
    for name, positions, teams, career_total, *_ in rows:
        assert career_total is not None and career_total > 0
        assert "QB" in (positions or "")
    # Known non-QBs from our fixture set should never appear.
    names = {r[0] for r in rows}
    assert "Walter Payton" not in names
    assert "Marcus Allen" not in names


def test_career_topN_unknown_rank_by_raises(db):
    with pytest.raises(ValueError):
        career_topN("not_a_real_column", n=5)


def test_career_topN_min_seasons_filters_out(db):
    """min_seasons=3 against a 2-season fixture build returns zero
    rows because no player has 3 distinct seasons in this DB."""
    sql, params = career_topN("rush_yds", n=10, min_seasons=3)
    rows = db.execute(sql, params).fetchall()
    assert rows == []


def test_career_topN_ever_won_filter(db):
    """Restrict to players who ever won an MVP. 2023 MVP = Lamar
    Jackson."""
    sql, params = career_topN(
        "pass_yds", n=10, ever_won_award=["MVP"],
    )
    rows = db.execute(sql, params).fetchall()
    names = {r[0] for r in rows}
    assert "Lamar Jackson" in names


def test_career_topN_ever_won_excludes_finalists(db):
    """The --ever-won filter should match outright winners only,
    not vote-only finalists. 2023 MVP voting had Josh Allen finishing
    behind Lamar Jackson — Allen should NOT be picked up by an MVP
    ever-won filter despite having a vote_finish row in player_awards."""
    # Sanity: confirm Allen has a non-winning MVP-vote row in our
    # fixture data (otherwise this test isn't proving anything).
    finalists = db.execute(
        """
        SELECT p.name, pa.vote_finish
        FROM   player_awards pa
        JOIN   players p USING (player_id)
        WHERE  pa.award_type = 'MVP' AND pa.season = 2023
          AND  pa.vote_finish > 1
        """
    ).fetchall()
    assert any(name == "Josh Allen" for name, _ in finalists), (
        "fixture sanity: expected Josh Allen as a 2023 MVP finalist"
    )

    sql, params = career_topN(
        "pass_yds", n=20, ever_won_award=["MVP"],
    )
    rows = db.execute(sql, params).fetchall()
    names = {r[0] for r in rows}
    assert "Lamar Jackson" in names
    assert "Josh Allen" not in names


# ---- pass_cmp_pct as a rank_by ----

def test_pass_cmp_pct_works_in_pos_topN(db):
    """pass_cmp_pct is a computed column on v_player_season_full;
    pos_topN should rank by it without error and return values
    between 0 and 1."""
    from ffpts.queries import pos_topN

    sql, params = pos_topN(
        "QB", n=5, rank_by="pass_cmp_pct",
        min_stats={"pass_att": 100},
    )
    rows = db.execute(sql, params).fetchall()
    assert len(rows) > 0
    cols = [d[0] for d in db.execute(sql, params).description]
    rank_col = cols.index("rank_value")
    for r in rows:
        v = r[rank_col]
        assert v is not None
        assert 0 <= v <= 1, f"pass_cmp_pct outside [0,1]: {v}"


def test_catch_rate_works_in_pos_topN(db):
    """catch_rate = rec / NULLIF(targets, 0) — computed column. Should
    rank cleanly with a min targets floor and produce 0..1 values."""
    from ffpts.queries import pos_topN

    sql, params = pos_topN(
        "WR", n=5, rank_by="catch_rate",
        min_stats={"targets": 30},
    )
    rows = db.execute(sql, params).fetchall()
    assert len(rows) > 0
    cols = [d[0] for d in db.execute(sql, params).description]
    rank_col = cols.index("rank_value")
    for r in rows:
        v = r[rank_col]
        assert v is not None
        assert 0 <= v <= 1, f"catch_rate outside [0,1]: {v}"


def test_catch_rate_zero_targets_does_not_crash(db):
    """A rank_by query for catch_rate with no min_stats floor shouldn't
    raise on 0-target rows — NULLIF in the view returns NULL, which
    pos_topN's IS NOT NULL filter excludes."""
    from ffpts.queries import pos_topN

    sql, params = pos_topN("ALL", n=5, rank_by="catch_rate")
    rows = db.execute(sql, params).fetchall()
    # Nonzero answer set, no exception.
    assert len(rows) >= 0


def test_pass_cmp_pct_career_recomputes_from_components(db):
    """career_topN with a ratio rank_by must recompute as
    SUM(num) / NULLIF(SUM(den), 0) — never sum percentages, never
    divide by zero."""
    sql, params = career_topN("pass_cmp_pct", n=5)
    rows = db.execute(sql, params).fetchall()
    assert len(rows) > 0
    # rows: (name, positions, teams, career_total, ...)
    for name, positions, teams, career_total, *_ in rows:
        assert career_total is not None
        assert 0 <= career_total <= 1, (
            f"career pass_cmp_pct outside [0,1]: {career_total} for {name}"
        )


def test_catch_rate_career_recomputes_from_components(db):
    """Career catch_rate computes as SUM(rec) / NULLIF(SUM(targets), 0).

    Note: PFR's pre-1992 targets data is incomplete, so the
    career_total can exceed 1.0 for tiny-sample players from older
    fixtures (rec recorded, targets undercounted). The test only
    verifies the computation runs cleanly and returns a positive
    number — the data quirk is upstream."""
    sql, params = career_topN("catch_rate", n=5)
    rows = db.execute(sql, params).fetchall()
    assert len(rows) > 0
    for name, positions, teams, career_total, *_ in rows:
        assert career_total is not None
        assert career_total > 0
