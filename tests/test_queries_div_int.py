import pytest

from ffpts.queries import most_def_int_by_division


def _seed(db):
    """Seed three NFC Central / North seasons + a non-NFC-N team to test scoping."""
    teams = [
        # (team, season, conference, division, franchise)
        ("CHI", 2001, "NFC", "NFC Central", "bears"),    # pre-realignment
        ("CHI", 2002, "NFC", "NFC North", "bears"),      # post-realignment
        ("GB",  2003, "NFC", "NFC North", "packers"),
        ("TB",  2001, "NFC", "NFC Central", "buccaneers"),  # NFC Central, then -> NFC South
        ("TB",  2002, "NFC", "NFC South", "buccaneers"),
        ("DAL", 2002, "NFC", "NFC East", "cowboys"),     # never NFC North
    ]
    for team, season, conf, div, frn in teams:
        db.execute(
            "INSERT INTO team_seasons (team, season, conference, division, franchise) "
            "VALUES (?, ?, ?, ?, ?)",
            [team, season, conf, div, frn],
        )

    rows = [
        # (player_id, name, team, season, def_int)
        ("p1", "Bears Pre",  "CHI", 2001, 5),
        ("p2", "Bears Post", "CHI", 2002, 8),
        ("p3", "Packers",    "GB",  2003, 7),
        ("p4", "Bucs Pre",   "TB",  2001, 6),  # NFC Central member at the time
        ("p5", "Bucs Post",  "TB",  2002, 4),  # now NFC South
        ("p6", "Cowboy",     "DAL", 2002, 9),  # never NFC North
    ]
    for pid, name, team, season, ints in rows:
        db.execute("INSERT INTO players (player_id, name) VALUES (?, ?)", [pid, name])
        db.execute(
            "INSERT INTO player_season_stats "
            "(player_id, season, team, position, def_int) VALUES (?, ?, ?, 'CB', ?)",
            [pid, season, team, ints],
        )


def test_historical_filter_nfc_north_2002_2005_excludes_pre_realignment(db):
    _seed(db)
    sql, params = most_def_int_by_division("NFC North", start=2002, end=2005, n=10)
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    # Only Bears 2002 (8) and Packers 2003 (7); pre-2002 NFC Central excluded.
    assert names == ["Bears Post", "Packers"]


def test_historical_filter_nfc_central_returns_only_pre_2002_central_rows(db):
    _seed(db)
    sql, params = most_def_int_by_division("NFC Central", start=1999, end=2005, n=10)
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    # Bears 2001 (5) and Bucs 2001 (6) — Bucs as NFC Central member in 2001.
    assert sorted(names) == sorted(["Bears Pre", "Bucs Pre"])


def test_franchise_mode_nfc_north_unions_all_eras_for_modern_franchises(db):
    _seed(db)
    sql, params = most_def_int_by_division(
        "NFC North", start=1999, end=2005, n=10, division_mode="franchise"
    )
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    # The franchise set for "NFC North" is bears + packers (the two
    # franchises that have appeared in NFC North in the seeded data).
    # Bucs (NFC Central pre-2002) is NOT in NFC North franchises.
    # Result includes Bears Pre + Bears Post + Packers across all years.
    assert sorted(names) == sorted(["Bears Pre", "Bears Post", "Packers"])


def test_franchise_mode_excludes_non_franchise_member(db):
    _seed(db)
    sql, params = most_def_int_by_division(
        "NFC North", start=1999, end=2005, n=10, division_mode="franchise"
    )
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    assert "Cowboy" not in names
    assert "Bucs Pre" not in names
    assert "Bucs Post" not in names


def test_year_range_inclusive_on_both_ends(db):
    _seed(db)
    sql, params = most_def_int_by_division(
        "NFC North", start=2003, end=2003, n=10, division_mode="historical"
    )
    rows = db.execute(sql, params).fetchall()
    assert [r[0] for r in rows] == ["Packers"]


def test_returns_player_season_columns_in_order(db):
    _seed(db)
    sql, params = most_def_int_by_division("NFC North", start=2002, end=2005, n=1)
    cols = [d[0] for d in db.execute(sql, params).description]
    assert cols == ["name", "team", "season", "def_int", "conference", "division", "franchise"]


def test_orders_by_def_int_desc_then_season_asc(db):
    """When two seasons tie on def_int, the earlier season ranks first
    (deterministic, matches the player-season default of treating each
    qualifying year on its own merit)."""
    _seed(db)
    # Add a second Bears 2003 row tied with the Packers' 7 INTs.
    db.execute(
        "INSERT INTO team_seasons (team, season, conference, division, franchise) "
        "VALUES ('CHI', 2003, 'NFC', 'NFC North', 'bears')"
    )
    db.execute("INSERT INTO players (player_id, name) VALUES ('p7', 'Bear 2003')")
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, def_int) "
        "VALUES ('p7', 2003, 'CHI', 'CB', 7)"
    )
    sql, params = most_def_int_by_division("NFC North", start=2002, end=2005, n=10)
    rows = db.execute(sql, params).fetchall()
    # Bears 2002 (8) > Packers 2003 (7) tied with Bears 2003 (7).
    assert [r[0] for r in rows][:1] == ["Bears Post"]


def test_invalid_mode_raises():
    with pytest.raises(ValueError):
        most_def_int_by_division("NFC North", start=2000, end=2005, division_mode="silly")  # type: ignore[arg-type]


def test_skips_null_def_int(db):
    _seed(db)
    db.execute("INSERT INTO players (player_id, name) VALUES ('p_null', 'Null INTs')")
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, def_int) "
        "VALUES ('p_null', 2003, 'CHI', 'CB', NULL)"
    )
    sql, params = most_def_int_by_division("NFC North", start=2002, end=2005, n=10)
    names = [r[0] for r in db.execute(sql, params).fetchall()]
    assert "Null INTs" not in names
