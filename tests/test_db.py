import duckdb
import pytest


def _table_names(con):
    return {row[0] for row in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
    ).fetchall()}


def _view_names(con):
    return {row[0] for row in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'VIEW'"
    ).fetchall()}


def test_schema_creates_expected_tables(db):
    assert _table_names(db) == {"players", "draft_picks", "team_seasons", "player_season_stats"}


def test_schema_creates_expected_views(db):
    assert _view_names(db) == {"v_player_season_full", "v_flex_seasons"}


def test_apply_schema_is_idempotent(db):
    # Re-apply on top of an already-initialized connection — must not raise.
    from ffpts.db import apply_schema
    apply_schema(db)
    assert _table_names(db) == {"players", "draft_picks", "team_seasons", "player_season_stats"}


def test_player_season_stats_pk_rejects_duplicates(db):
    db.execute("INSERT INTO players (player_id, name) VALUES ('McCaCh01', 'CMC')")
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, fpts_ppr) "
        "VALUES ('McCaCh01', 2023, 'SFO', 393.3)"
    )
    with pytest.raises(duckdb.ConstraintException):
        db.execute(
            "INSERT INTO player_season_stats (player_id, season, team, fpts_ppr) "
            "VALUES ('McCaCh01', 2023, 'SFO', 999.0)"
        )


def test_v_flex_seasons_filters_to_skill_positions(db):
    db.execute("INSERT INTO players (player_id, name) VALUES ('a', 'A'), ('b', 'B'), ('c', 'C')")
    db.execute("""
        INSERT INTO player_season_stats (player_id, season, team, position) VALUES
            ('a', 2023, 'SFO', 'RB'),
            ('b', 2023, 'KAN', 'QB'),
            ('c', 2023, 'MIA', 'WR')
    """)
    rows = db.execute(
        "SELECT player_id, position FROM v_flex_seasons ORDER BY player_id"
    ).fetchall()
    # QB excluded, RB and WR included.
    assert rows == [("a", "RB"), ("c", "WR")]


def test_v_player_season_full_left_joins_draft_and_team(db):
    db.execute("INSERT INTO players (player_id, name) VALUES ('McCaCh01', 'CMC')")
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, fpts_ppr) "
        "VALUES ('McCaCh01', 2023, 'SFO', 'RB', 393.3)"
    )
    # Without draft/team_seasons rows, the LEFT JOINs still return one row.
    row = db.execute(
        "SELECT name, draft_round, division FROM v_player_season_full"
    ).fetchone()
    assert row[0] == "CMC"
    assert row[1] is None
    assert row[2] is None

    # Add draft + team_seasons rows; same view now returns the joined data.
    db.execute(
        "INSERT INTO draft_picks (player_id, year, round, overall_pick, team) "
        "VALUES ('McCaCh01', 2017, 1, 8, 'CAR')"
    )
    db.execute(
        "INSERT INTO team_seasons (team, season, franchise, conference, division) "
        "VALUES ('SFO', 2023, '49ers', 'NFC', 'NFC West')"
    )
    row = db.execute(
        "SELECT draft_round, conference, division, franchise FROM v_player_season_full"
    ).fetchone()
    assert row == (1, "NFC", "NFC West", "49ers")


def test_init_db_returns_ready_connection(tmp_path):
    from ffpts.db import init_db
    db_path = tmp_path / "test.duckdb"
    con = init_db(db_path)
    try:
        assert _table_names(con) == {"players", "draft_picks", "team_seasons", "player_season_stats"}
    finally:
        con.close()
    assert db_path.exists()
