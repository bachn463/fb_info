"""Direct (non-CLI) tests for the new query helpers in
``ffpts.queries``: career_topN, awards_list."""

from __future__ import annotations

import pytest

from ffpts.db import apply_schema, connect
from ffpts.pipeline import build
from ffpts.queries import awards_list, career_topN


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


# ---- awards_list ----

def test_awards_list_returns_expected_columns(db):
    sql, params = awards_list(award_type="MVP")
    cur = db.execute(sql, params)
    cols = [d[0] for d in cur.description]
    assert cols == [
        "season", "award_type", "name", "position", "team", "vote_finish",
    ]


def test_awards_list_filters_by_award_type(db):
    sql, params = awards_list(award_type="MVP")
    rows = db.execute(sql, params).fetchall()
    # Should include the 2023 MVP.
    names = {r[2] for r in rows}
    assert "Lamar Jackson" in names


def test_awards_list_position_and_team_populated(db):
    """Award rows for players who have a stats row that season should
    have non-empty position + team. Lamar Jackson 2023 MVP is QB BAL."""
    sql, params = awards_list(award_type="MVP", season=2023)
    rows = db.execute(sql, params).fetchall()
    # rows: (season, award_type, name, position, team, vote_finish)
    lamar = next(r for r in rows if r[2] == "Lamar Jackson")
    assert lamar[3] == "QB"
    assert lamar[4] == "BAL"


def test_awards_list_filters_by_season(db):
    sql, params = awards_list(season=2023)
    rows = db.execute(sql, params).fetchall()
    seasons = {r[0] for r in rows}
    assert seasons == {2023}


def test_awards_list_winners_only_excludes_finalists(db):
    """winners_only=True should filter out vote_finish > 1."""
    sql, params = awards_list(award_type="MVP", winners_only=True)
    rows = db.execute(sql, params).fetchall()
    for season, award_type, name, position, team, vote_finish in rows:
        assert vote_finish == 1


def test_awards_list_include_finalists(db):
    """winners_only=False keeps the placings."""
    sql, params = awards_list(award_type="MVP", winners_only=False)
    rows = db.execute(sql, params).fetchall()
    finishes = {r[5] for r in rows}
    # 2023 MVP voting had multiple finishers; expect more than just 1.
    assert len(finishes) > 1


def test_awards_list_unknown_award_type_raises(db):
    with pytest.raises(ValueError):
        awards_list(award_type="BOGUS")
