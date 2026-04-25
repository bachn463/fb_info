import pytest

from ffpts.queries import flex_topN_by_draft_round


def _seed(db):
    """Seed a tiny set of player-seasons + draft picks across rounds and positions."""
    rows = [
        # (player_id, name, draft_round, position, fpts_ppr, fpts_half, fpts_std, season, team)
        ("p1", "RB R3 Big",   3, "RB", 350.0, 320.0, 290.0, 2019, "DAL"),
        ("p2", "RB R3 Mid",   3, "RB", 280.0, 260.0, 240.0, 2020, "NYG"),
        ("p3", "WR R3",       3, "WR", 240.0, 215.0, 190.0, 2021, "GB"),
        ("p4", "TE R3",       3, "TE", 220.0, 200.0, 180.0, 2022, "BAL"),
        ("p5", "RB R1 Best",  1, "RB", 400.0, 360.0, 320.0, 2023, "SF"),
        ("p6", "QB R3",       3, "QB", 380.0, 380.0, 380.0, 2023, "BUF"),  # FLEX excluded
        ("p7", "RB R3 Same Player", 3, "RB", 300.0, 270.0, 240.0, 2018, "DAL"),
        ("p1_dup_2018", None,  None, None, None, None, None, None, None),  # placeholder
    ]
    # Insert players + drafts + season stats.
    for pid, name, rnd, pos, ppr, half, std, season, team in rows:
        if name is None:  # skip placeholders
            continue
        db.execute(
            "INSERT INTO players (player_id, name) VALUES (?, ?)",
            [pid, name],
        )
        db.execute(
            "INSERT INTO draft_picks (player_id, year, round, overall_pick, team) VALUES (?, ?, ?, ?, ?)",
            [pid, season - 2, rnd, 99, team],
        )
        db.execute(
            """
            INSERT INTO player_season_stats
              (player_id, season, team, position, fpts_std, fpts_half, fpts_ppr)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [pid, season, team, pos, std, half, ppr],
        )


def test_flex_topN_returns_player_seasons_in_correct_order(db):
    _seed(db)
    sql, params = flex_topN_by_draft_round(3, n=10, scoring="ppr")
    rows = db.execute(sql, params).fetchall()
    # FLEX positions only (no QB), only round 3, ordered by ppr desc.
    names = [r[0] for r in rows]
    assert names == ["RB R3 Big", "RB R3 Same Player", "RB R3 Mid", "WR R3", "TE R3"]


def test_flex_topN_respects_limit(db):
    _seed(db)
    sql, params = flex_topN_by_draft_round(3, n=2, scoring="ppr")
    rows = db.execute(sql, params).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == "RB R3 Big"
    assert rows[1][0] == "RB R3 Same Player"


def test_flex_topN_excludes_qb(db):
    _seed(db)
    sql, params = flex_topN_by_draft_round(3, n=10, scoring="ppr")
    rows = db.execute(sql, params).fetchall()
    assert "QB R3" not in [r[0] for r in rows]


def test_flex_topN_excludes_other_rounds(db):
    _seed(db)
    sql, params = flex_topN_by_draft_round(1, n=10, scoring="ppr")
    rows = db.execute(sql, params).fetchall()
    assert [r[0] for r in rows] == ["RB R1 Best"]


def test_flex_topN_uses_correct_fpts_column_per_scoring_mode(db):
    _seed(db)
    for scoring, expected_top_value in [
        ("std", 290.0),    # RB R3 Big std = 290
        ("half", 320.0),   # RB R3 Big half = 320
        ("ppr", 350.0),    # RB R3 Big ppr = 350
    ]:
        sql, params = flex_topN_by_draft_round(3, n=1, scoring=scoring)
        row = db.execute(sql, params).fetchone()
        assert row[3] == pytest.approx(expected_top_value)


def test_flex_topN_skips_null_fpts(db):
    """A FLEX player-season with NULL fpts (e.g. failed scoring) shouldn't appear."""
    _seed(db)
    db.execute("INSERT INTO players (player_id, name) VALUES ('p_null', 'Null Fpts')")
    db.execute(
        "INSERT INTO draft_picks (player_id, year, round, overall_pick, team) "
        "VALUES ('p_null', 2018, 3, 75, 'DAL')"
    )
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, fpts_ppr) "
        "VALUES ('p_null', 2020, 'DAL', 'RB', NULL)"
    )
    sql, params = flex_topN_by_draft_round(3, n=20, scoring="ppr")
    rows = db.execute(sql, params).fetchall()
    assert "Null Fpts" not in [r[0] for r in rows]


def test_flex_topN_invalid_scoring_raises():
    with pytest.raises(ValueError):
        flex_topN_by_draft_round(3, scoring="superflex")  # type: ignore[arg-type]


def test_flex_topN_returns_player_season_columns(db):
    _seed(db)
    sql, params = flex_topN_by_draft_round(3, n=1, scoring="ppr")
    cols = [d[0] for d in db.execute(sql, params).description]
    assert cols == [
        "name", "team", "season", "fpts",
        "draft_round", "draft_year", "draft_overall_pick",
    ]
