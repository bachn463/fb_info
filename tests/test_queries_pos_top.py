import pytest

from ffpts.queries import POSITION_ALIASES, RANK_BY_ALLOWED, pos_topN


def _seed(db):
    """Seed a small set of player-seasons across positions and years."""
    rows = [
        # (player_id, name, season, team, position, draft_round,
        #  pass_yds, rec_yds, rec, fpts_ppr, def_int, def_sacks, fg_made)
        ("qb1", "Late Round QB", 2016, "DAL", "QB", 4, 3667, 0, 0, 270.0, 0, 0.0, 0),
        ("qb2", "5th Round QB",  2018, "WAS", "QB", 5, 3000, 0, 0, 230.0, 0, 0.0, 0),
        ("qb3", "Top QB",        2023, "BUF", "QB", 1, 4306, 0, 0, 392.0, 0, 0.0, 0),
        ("rb1", "Top RB",        2023, "SF",  "RB", 1, 0, 564, 67, 391.3, 0, 0.0, 0),
        ("wr1", "Top WR",        2023, "DAL", "WR", 1, 0, 1749, 135, 403.2, 0, 0.0, 0),
        ("te1", "Top TE",        2023, "KC",  "TE", 3, 0, 984, 93, 240.0, 0, 0.0, 0),
        ("cb1", "Pick CB",       2022, "NYJ", "CB", 1, 0, 0, 0, 0.0, 6, 0.0, 0),
        ("de1", "Sack DE",       2023, "PIT", "DE", 1, 0, 0, 0, 0.0, 0, 19.0, 0),
        ("k1",  "Top K",         2023, "BAL", "K",  6, 0, 0, 0, 0.0, 0, 0.0, 35),
    ]
    for pid, name, season, team, pos, rnd, py, ry, rec, ppr, di, ds, fg in rows:
        db.execute(
            "INSERT INTO players (player_id, name) VALUES (?, ?)", [pid, name]
        )
        db.execute(
            "INSERT INTO draft_picks (player_id, year, round, overall_pick, team) "
            "VALUES (?, ?, ?, ?, ?)",
            [pid, season - 1, rnd, 99, team],
        )
        db.execute(
            """
            INSERT INTO player_season_stats
              (player_id, season, team, position,
               pass_yds, rec_yds, rec, fpts_ppr, def_int, def_sacks, fg_made)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [pid, season, team, pos, py, ry, rec, ppr, di, ds, fg],
        )


def test_pos_top_qb_ranks_by_pass_yds(db):
    _seed(db)
    sql, params = pos_topN("QB", n=10, rank_by="pass_yds")
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    # Top QB (4306) > Late Round QB (3667) > 5th Round QB (3000)
    assert names == ["Top QB", "Late Round QB", "5th Round QB"]


def test_pos_top_qb_filtered_to_late_rounds_answers_user_question(db):
    """The 'best QBs drafted in round 4 and 5' query."""
    _seed(db)
    sql, params = pos_topN(
        "QB", n=10, rank_by="pass_yds", draft_rounds=[4, 5]
    )
    rows = db.execute(sql, params).fetchall()
    assert [r[0] for r in rows] == ["Late Round QB", "5th Round QB"]


def test_pos_top_flex_alias_expands_to_rb_wr_te(db):
    _seed(db)
    sql, params = pos_topN("FLEX", n=10, rank_by="fpts_ppr")
    rows = db.execute(sql, params).fetchall()
    positions = {r[3] for r in rows}
    assert positions == {"RB", "WR", "TE"}
    names = [r[0] for r in rows]
    # Top WR (403) > Top RB (391) > Top TE (240)
    assert names == ["Top WR", "Top RB", "Top TE"]


def test_pos_top_all_alias_includes_every_position(db):
    _seed(db)
    sql, params = pos_topN("ALL", n=10, rank_by="fpts_ppr")
    rows = db.execute(sql, params).fetchall()
    # Sack DE has fpts_ppr = 0 but it's NOT NULL, so it's included.
    # Order is by fpts_ppr desc.
    names = [r[0] for r in rows]
    assert names[0] == "Top WR"  # 403.2 PPR


def test_pos_top_cb_by_def_int(db):
    _seed(db)
    sql, params = pos_topN("CB", n=10, rank_by="def_int")
    rows = db.execute(sql, params).fetchall()
    assert [r[0] for r in rows] == ["Pick CB"]


def test_pos_top_de_by_def_sacks(db):
    _seed(db)
    sql, params = pos_topN("DE", n=10, rank_by="def_sacks")
    rows = db.execute(sql, params).fetchall()
    assert [r[0] for r in rows] == ["Sack DE"]


def test_pos_top_k_by_fg_made(db):
    _seed(db)
    sql, params = pos_topN("K", n=10, rank_by="fg_made")
    rows = db.execute(sql, params).fetchall()
    assert [r[0] for r in rows] == ["Top K"]


def test_pos_top_year_range_filter(db):
    _seed(db)
    # Only 2023 QB rows.
    sql, params = pos_topN("QB", n=10, rank_by="pass_yds", start=2023, end=2023)
    rows = db.execute(sql, params).fetchall()
    assert [r[0] for r in rows] == ["Top QB"]


def test_pos_top_skips_null_rank_value(db):
    _seed(db)
    db.execute("INSERT INTO players (player_id, name) VALUES ('null_p', 'Null Stat')")
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, pass_yds) "
        "VALUES ('null_p', 2023, 'DAL', 'QB', NULL)"
    )
    sql, params = pos_topN("QB", n=10, rank_by="pass_yds")
    names = [r[0] for r in db.execute(sql, params).fetchall()]
    assert "Null Stat" not in names


def test_pos_top_invalid_rank_by_raises():
    with pytest.raises(ValueError):
        pos_topN("QB", rank_by="DROP TABLE players;--")


def test_pos_top_position_is_case_insensitive(db):
    _seed(db)
    upper = db.execute(*pos_topN("QB", n=1, rank_by="pass_yds")).fetchall()
    lower = db.execute(*pos_topN("qb", n=1, rank_by="pass_yds")).fetchall()
    assert upper == lower


def test_rank_by_allowlist_includes_expected_columns():
    # Spot-check a few categories — guards against accidental removal.
    assert "fpts_ppr" in RANK_BY_ALLOWED
    assert "pass_yds" in RANK_BY_ALLOWED
    assert "def_int" in RANK_BY_ALLOWED
    assert "fg_made" in RANK_BY_ALLOWED


def test_position_aliases_table():
    assert POSITION_ALIASES["FLEX"] == ["RB", "WR", "TE"]
    assert POSITION_ALIASES["ALL"] is None


def test_pos_top_returns_expected_column_order(db):
    _seed(db)
    sql, params = pos_topN("QB", n=1, rank_by="pass_yds")
    cols = [d[0] for d in db.execute(sql, params).description]
    assert cols == [
        "name", "team", "season", "position", "rank_value",
        "draft_round", "draft_year", "draft_overall_pick",
    ]
