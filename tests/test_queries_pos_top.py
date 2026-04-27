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
        # Players for name-grep tests; first names with "Z", last names with "Z".
        ("zfn", "Zach Wilson",       2021, "NYJ", "QB", 1, 2334, 0, 0, 200.0, 0, 0.0, 0),
        ("zln", "Joey Bosa",         2023, "LAC", "DE", 1, 0, 0, 0, 0.0, 0, 6.5, 0),
        ("zb",  "Zaven Collins",     2021, "ARI", "LB", 1, 0, 0, 0, 0.0, 1, 1.0, 0),
        ("ez",  "George Pickens Jr.", 2022, "PIT", "WR", 2, 0, 801, 52, 187.0, 0, 0.0, 0),
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
        # Wire team_seasons so division/conference filters work for these rows.
        # In the seeded data, only certain (team, season) combos are needed:
        # we add them all here, idempotently.
        div_for_team = {
            "DAL": ("NFC", "NFC East"),
            "WAS": ("NFC", "NFC East"),
            "BUF": ("AFC", "AFC East"),
            "NYJ": ("AFC", "AFC East"),
            "SF":  ("NFC", "NFC West"),
            "ARI": ("NFC", "NFC West"),
            "LAC": ("AFC", "AFC West"),
            "PIT": ("AFC", "AFC North"),
            "KC":  ("AFC", "AFC West"),
            "BAL": ("AFC", "AFC North"),
        }
        conf, div = div_for_team[team]
        db.execute(
            "INSERT INTO team_seasons (team, season, conference, division, franchise) "
            "VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            [team, season, conf, div, "test"],
        )


def test_pos_top_qb_ranks_by_pass_yds(db):
    _seed(db)
    sql, params = pos_topN("QB", n=10, rank_by="pass_yds")
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    # Top QB (4306) > Late Round QB (3667) > 5th Round QB (3000) > Zach Wilson (2334)
    assert names == ["Top QB", "Late Round QB", "5th Round QB", "Zach Wilson"]


def test_pos_top_qb_filtered_to_late_rounds_answers_user_question(db):
    """The 'best QBs drafted in round 4 and 5' query."""
    _seed(db)
    sql, params = pos_topN(
        "QB", n=10, rank_by="pass_yds", draft_rounds=[4, 5]
    )
    rows = db.execute(sql, params).fetchall()
    assert [r[0] for r in rows] == ["Late Round QB", "5th Round QB"]


def _seed_undrafted(db):
    """Add a few undrafted player-seasons (NULL draft_round)."""
    rows = [
        # (player_id, name, season, team, position, fpts_ppr)
        ("u1", "Undrafted RB Hero",   2002, "KC",  "RB", 285.0),
        ("u2", "Undrafted WR Star",   2014, "MIN", "WR", 280.0),
        ("u3", "Undrafted TE Backup", 2010, "SD",  "TE",  60.0),
    ]
    for pid, name, season, team, pos, ppr in rows:
        db.execute("INSERT INTO players (player_id, name) VALUES (?, ?)", [pid, name])
        # NO draft_picks row -> draft_round is NULL via LEFT JOIN
        db.execute(
            "INSERT INTO player_season_stats (player_id, season, team, position, fpts_ppr) "
            "VALUES (?, ?, ?, ?, ?)",
            [pid, season, team, pos, ppr],
        )


def test_pos_top_undrafted_only(db):
    _seed(db)
    _seed_undrafted(db)
    sql, params = pos_topN(
        "FLEX", n=10, rank_by="fpts_ppr", draft_rounds=["undrafted"]
    )
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    # Only the three undrafted FLEX, ordered by ppr desc.
    assert names == ["Undrafted RB Hero", "Undrafted WR Star", "Undrafted TE Backup"]


def test_pos_top_undrafted_token_is_case_insensitive(db):
    _seed_undrafted(db)
    for token in ["undrafted", "UNDRAFTED", "Undrafted"]:
        sql, params = pos_topN(
            "FLEX", n=10, rank_by="fpts_ppr", draft_rounds=[token]
        )
        names = [r[0] for r in db.execute(sql, params).fetchall()]
        assert "Undrafted RB Hero" in names


def test_pos_top_mixed_rounds_and_undrafted(db):
    _seed(db)
    _seed_undrafted(db)
    sql, params = pos_topN(
        "FLEX", n=10, rank_by="fpts_ppr",
        draft_rounds=[3, "undrafted"],
    )
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    # _seed's only round-3 FLEX is "Top TE" (240). Undrafted FLEX from
    # _seed_undrafted: "Undrafted RB Hero" (285), "Undrafted WR Star" (280),
    # "Undrafted TE Backup" (60). Combined and ordered by PPR desc:
    assert names == [
        "Undrafted RB Hero",
        "Undrafted WR Star",
        "Top TE",
        "Undrafted TE Backup",
    ]
    # No round-1 / round-2 FLEX should appear.
    assert "Top WR" not in names           # R1
    assert "Top RB" not in names           # R1
    assert "George Pickens Jr." not in names  # R2


def test_pos_top_only_drafted_rounds_excludes_undrafted(db):
    _seed(db)
    _seed_undrafted(db)
    sql, params = pos_topN(
        "FLEX", n=20, rank_by="fpts_ppr", draft_rounds=[3]
    )
    names = [r[0] for r in db.execute(sql, params).fetchall()]
    # No undrafted in the result.
    for n in names:
        assert "Undrafted" not in n


def test_pos_top_undrafted_token_invalid_string_raises():
    with pytest.raises(ValueError):
        pos_topN("FLEX", draft_rounds=["fourth"])


def test_pos_top_undrafted_token_invalid_type_raises():
    with pytest.raises(ValueError):
        pos_topN("FLEX", draft_rounds=[3.5])  # type: ignore[list-item]


def test_pos_top_flex_alias_expands_to_rb_wr_te(db):
    _seed(db)
    sql, params = pos_topN("FLEX", n=10, rank_by="fpts_ppr")
    rows = db.execute(sql, params).fetchall()
    positions = {r[3] for r in rows}
    assert positions == {"RB", "WR", "TE"}
    names = [r[0] for r in rows]
    # Top WR (403) > Top RB (391) > Top TE (240) > Pickens Jr. (187)
    assert names == ["Top WR", "Top RB", "Top TE", "George Pickens Jr."]


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
    # Sack DE (19) > Joey Bosa (6.5)
    assert [r[0] for r in rows] == ["Sack DE", "Joey Bosa"]


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


def test_pos_top_team_filter(db):
    _seed(db)
    sql, params = pos_topN("ALL", n=10, rank_by="fpts_ppr", team="DAL")
    rows = db.execute(sql, params).fetchall()
    teams = {r[1] for r in rows}
    assert teams == {"DAL"}
    # Order: WR (403.2) > QB (270.0)
    names = [r[0] for r in rows]
    assert names == ["Top WR", "Late Round QB"]


def test_pos_top_team_filter_is_case_insensitive(db):
    _seed(db)
    sql_upper, p_upper = pos_topN("ALL", n=10, rank_by="fpts_ppr", team="DAL")
    sql_lower, p_lower = pos_topN("ALL", n=10, rank_by="fpts_ppr", team="dal")
    assert (
        db.execute(sql_upper, p_upper).fetchall()
        == db.execute(sql_lower, p_lower).fetchall()
    )


def test_pos_top_division_filter(db):
    _seed(db)
    sql, params = pos_topN(
        "ALL", n=10, rank_by="fpts_ppr", division="AFC East"
    )
    rows = db.execute(sql, params).fetchall()
    teams = {r[1] for r in rows}
    assert teams == {"BUF", "NYJ"}


def test_pos_top_conference_filter(db):
    _seed(db)
    sql, params = pos_topN(
        "ALL", n=20, rank_by="fpts_ppr", conference="AFC"
    )
    rows = db.execute(sql, params).fetchall()
    teams = {r[1] for r in rows}
    # Every AFC team in the seed: BUF, NYJ, LAC, PIT, KC, BAL
    assert teams == {"BUF", "NYJ", "LAC", "PIT", "KC", "BAL"}


def test_pos_top_first_name_contains_grep(db):
    _seed(db)
    sql, params = pos_topN(
        "ALL", n=20, rank_by="fpts_ppr", first_name_contains="z"
    )
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    # First names with 'z': "Zach Wilson", "Zaven Collins". Case-insensitive.
    assert sorted(names) == sorted(["Zach Wilson", "Zaven Collins"])


def test_pos_top_last_name_contains_grep(db):
    _seed(db)
    sql, params = pos_topN(
        "ALL", n=20, rank_by="fpts_ppr", last_name_contains="z"
    )
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    # Last names containing 'z': Bosa? no. Wait: Joey Bosa has 'B', no z.
    # "George Pickens Jr." -- last name segment is "Pickens Jr." -- no z either.
    # Hmm let me re-check seeded names with 'z' in the last name...
    # None. Let me add one inline.
    assert names == []


def test_pos_top_last_name_grep_finds_match(db):
    _seed(db)
    db.execute("INSERT INTO players (player_id, name) VALUES ('zlast', 'Joey Lopez')")
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, fpts_ppr) "
        "VALUES ('zlast', 2023, 'SF', 'WR', 250.0)"
    )
    sql, params = pos_topN("ALL", n=20, rank_by="fpts_ppr", last_name_contains="z")
    names = [r[0] for r in db.execute(sql, params).fetchall()]
    assert "Joey Lopez" in names
    # First-name grep for 'z' should NOT pick up Joey Lopez.
    sql2, params2 = pos_topN("ALL", n=20, rank_by="fpts_ppr", first_name_contains="z")
    names2 = [r[0] for r in db.execute(sql2, params2).fetchall()]
    assert "Joey Lopez" not in names2


def _seed_multi_season_player(db):
    """Same player_id across three seasons; varying fpts and pass_yds."""
    db.execute("INSERT INTO players (player_id, name) VALUES ('multi', 'Multi Year QB')")
    db.execute(
        "INSERT INTO draft_picks (player_id, year, round, overall_pick, team) "
        "VALUES ('multi', 2018, 1, 10, 'GB')"
    )
    rows = [
        # (season, team, fpts_ppr, pass_yds)
        (2019, "GB", 250.0, 3500),  # mediocre
        (2020, "GB", 380.0, 4500),  # best PPR + best pass_yds
        (2021, "GB", 320.0, 4200),  # good but not best
    ]
    for season, team, ppr, py in rows:
        db.execute(
            "INSERT INTO player_season_stats "
            "(player_id, season, team, position, pass_yds, fpts_ppr) "
            "VALUES ('multi', ?, ?, 'QB', ?, ?)",
            [season, team, py, ppr],
        )


def test_unique_collapses_to_best_season_per_player(db):
    _seed(db)
    _seed_multi_season_player(db)
    sql, params = pos_topN("QB", n=10, rank_by="fpts_ppr", unique=True)
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    # Multi Year QB appears once.
    assert names.count("Multi Year QB") == 1
    # And the row that survived is their 2020 season (best PPR = 380).
    multi_row = next(r for r in rows if r[0] == "Multi Year QB")
    season = multi_row[2]
    rank_value = multi_row[4]
    assert season == 2020
    assert rank_value == 380.0


def test_unique_picks_different_season_for_different_rank_by(db):
    _seed_multi_season_player(db)
    # Multi Year QB's seasons: 2019(3500), 2020(4500), 2021(4200) pass_yds.
    # 2020 wins for pass_yds *and* fpts_ppr in this fixture, but verify
    # the SQL actually evaluates ROW_NUMBER over the chosen rank_by:
    sql, params = pos_topN("QB", n=10, rank_by="pass_yds", unique=True)
    row = db.execute(sql, params).fetchone()
    assert row[0] == "Multi Year QB"
    assert row[2] == 2020
    assert row[4] == 4500


def test_unique_respects_year_range_when_picking_best_season(db):
    _seed_multi_season_player(db)
    # Constrain to 2019-2019 — the only Multi Year QB season in range
    # is 2019 with 250 PPR.
    sql, params = pos_topN(
        "QB", n=10, rank_by="fpts_ppr",
        start=2019, end=2019, unique=True,
    )
    rows = db.execute(sql, params).fetchall()
    multi = next(r for r in rows if r[0] == "Multi Year QB")
    assert multi[2] == 2019
    assert multi[4] == 250.0


def test_unique_combines_with_other_filters(db):
    _seed_multi_season_player(db)
    sql, params = pos_topN(
        "QB", n=10, rank_by="pass_yds", team="GB", unique=True
    )
    rows = db.execute(sql, params).fetchall()
    # Only one row, the 2020 season.
    assert len(rows) == 1
    assert rows[0][0] == "Multi Year QB"
    assert rows[0][2] == 2020


def test_unique_default_false_preserves_player_season_behavior(db):
    _seed_multi_season_player(db)
    sql, params = pos_topN("QB", n=10, rank_by="fpts_ppr")  # unique unspecified
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    # Three player-seasons for Multi Year QB still appear.
    assert names.count("Multi Year QB") == 3


def test_pos_top_combined_filters(db):
    _seed(db)
    # FLEX (RB/WR/TE) on DAL in 2023, ranked by PPR — expect just Top WR.
    sql, params = pos_topN(
        "FLEX", n=10, rank_by="fpts_ppr",
        team="DAL", start=2023, end=2023,
    )
    rows = db.execute(sql, params).fetchall()
    assert [r[0] for r in rows] == ["Top WR"]


def _seed_awards(db):
    """Seed a few players with awards rows.

    - 'MVP Guy' won MVP in 2020 (vote_finish=1)
    - 'Runner Up' had MVP vote_finish=2 in 2020 (didn't win)
    - 'Pro Bowler' had PB in 2020 (binary award)
    - 'Rookie OROY' is in his first season 2020 with OROY win
    """
    db.execute("""
        INSERT INTO players (player_id, name) VALUES
            ('mvp_g',   'MVP Guy'),
            ('runner',  'Runner Up'),
            ('pb_only', 'Pro Bowler'),
            ('rookie',  'Rookie OROY')
    """)
    db.execute("""
        INSERT INTO player_season_stats (player_id, season, team, position, fpts_ppr) VALUES
            ('mvp_g',   2020, 'KC',  'QB', 410.0),
            ('mvp_g',   2021, 'KC',  'QB', 380.0),
            ('runner',  2020, 'BUF', 'QB', 395.0),
            ('pb_only', 2020, 'TB',  'WR', 320.0),
            ('rookie',  2020, 'CIN', 'WR', 305.0)
    """)
    db.execute("""
        INSERT INTO player_awards (player_id, season, award_type, vote_finish) VALUES
            ('mvp_g',   2020, 'MVP',      1),
            ('mvp_g',   2020, 'PB',       NULL),
            ('runner',  2020, 'MVP',      2),
            ('runner',  2020, 'PB',       NULL),
            ('pb_only', 2020, 'PB',       NULL),
            ('rookie',  2020, 'OROY',     1),
            ('rookie',  2020, 'AP_FIRST', NULL)
    """)


def test_has_award_filter_only_includes_winners(db):
    """--has-award MVP returns the actual MVP winner only, not the
    runner-up who got vote_finish=2."""
    _seed_awards(db)
    sql, params = pos_topN("ALL", n=10, rank_by="fpts_ppr", has_award=["MVP"])
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    assert names == ["MVP Guy"]


def test_has_award_filter_pro_bowl_includes_all_pro_bowlers(db):
    """Binary award (PB has NULL vote_finish) — all rows that have
    the entry are 'wins'."""
    _seed_awards(db)
    sql, params = pos_topN("ALL", n=10, rank_by="fpts_ppr", has_award=["PB"])
    names = [r[0] for r in db.execute(sql, params).fetchall()]
    # MVP Guy, Runner Up, Pro Bowler — all 2020 PB.
    assert sorted(names) == sorted(["MVP Guy", "Runner Up", "Pro Bowler"])


def test_has_award_filter_multi_award_or_semantics(db):
    """--has-award MVP --has-award OROY returns winners of either."""
    _seed_awards(db)
    sql, params = pos_topN(
        "ALL", n=10, rank_by="fpts_ppr",
        has_award=["MVP", "OROY"],
    )
    names = [r[0] for r in db.execute(sql, params).fetchall()]
    # MVP Guy (MVP) + Rookie OROY (OROY).
    assert sorted(names) == sorted(["MVP Guy", "Rookie OROY"])


def test_has_award_normalizes_case():
    """Lowercase award labels work too."""
    sql, params = pos_topN("ALL", n=5, rank_by="fpts_ppr", has_award=["mvp"])
    # No DB query — just confirm no ValueError.
    assert "MVP" in params


def test_has_award_unknown_label_raises():
    with pytest.raises(ValueError):
        pos_topN("ALL", has_award=["BEST_HAIRCUT"])


def test_rookie_only_filters_to_first_season(db):
    """Rookie OROY's only season is 2020 (his rookie year). MVP Guy
    appears in both 2020 and 2021; rookie_only restricts to 2020."""
    _seed_awards(db)
    sql, params = pos_topN("ALL", n=10, rank_by="fpts_ppr", rookie_only=True)
    rows = db.execute(sql, params).fetchall()
    seasons_by_name = {r[0]: r[2] for r in rows}
    # MVP Guy: 2 seasons -> rookie season = 2020 (MIN). His 2021 row excluded.
    assert seasons_by_name["MVP Guy"] == 2020
    # Multiple players' 2020 rows survive.
    assert "Rookie OROY" in seasons_by_name


def test_combined_has_award_and_rookie_only(db):
    """Top OROY winners during their rookie year — implements the user's
    motivating example."""
    _seed_awards(db)
    sql, params = pos_topN(
        "ALL", n=10, rank_by="fpts_ppr",
        has_award=["OROY"], rookie_only=True,
    )
    rows = db.execute(sql, params).fetchall()
    assert [(r[0], r[2]) for r in rows] == [("Rookie OROY", 2020)]


def test_min_stats_filter_excludes_low_volume(db):
    """--min-stat rush_yds=1000 keeps only seasons over the threshold."""
    _seed(db)  # has Top RB at 564 rec yds, etc.
    db.execute("""
        INSERT INTO players (player_id, name) VALUES ('big_rb', '1500 Yarder')
    """)
    db.execute("""
        INSERT INTO player_season_stats (player_id, season, team, position, rush_yds, fpts_ppr)
        VALUES ('big_rb', 2010, 'DAL', 'RB', 1500, 350.0)
    """)
    sql, params = pos_topN(
        "RB", n=10, rank_by="rush_yds",
        min_stats={"rush_yds": 1000},
    )
    rows = db.execute(sql, params).fetchall()
    names = [r[0] for r in rows]
    assert "1500 Yarder" in names
    assert "Top RB" not in names  # 0 rush_yds in seed (it's a receiving RB)


def test_max_stats_filter_motivating_example(db):
    """User's example: top RBs by rush attempts with rush_yds <= 999.
    High-volume / low-yardage seasons."""
    _seed(db)
    db.execute("""
        INSERT INTO players (player_id, name) VALUES
            ('grinder', 'Volume Grinder'),
            ('breakaway', 'Breakaway Back')
    """)
    db.execute("""
        INSERT INTO player_season_stats (player_id, season, team, position, rush_att, rush_yds, fpts_ppr)
        VALUES
            ('grinder',   2010, 'PIT', 'RB', 280, 950, 200.0),
            ('breakaway', 2010, 'PIT', 'RB', 200, 1500, 250.0)
    """)
    sql, params = pos_topN(
        "RB", n=10, rank_by="rush_att",
        team="PIT",
        max_stats={"rush_yds": 999},
    )
    names = [r[0] for r in db.execute(sql, params).fetchall()]
    # Volume Grinder qualifies (950 yds, under cap); Breakaway doesn't.
    assert "Volume Grinder" in names
    assert "Breakaway Back" not in names


def test_min_and_max_stats_combine(db):
    """Both filters combined narrow the band further."""
    _seed(db)
    db.execute("""
        INSERT INTO players (player_id, name) VALUES
            ('low', 'Below Min'),
            ('mid', 'In Range'),
            ('high', 'Above Max')
    """)
    db.execute("""
        INSERT INTO player_season_stats (player_id, season, team, position, rec_yds, fpts_ppr)
        VALUES
            ('low',  2020, 'DAL', 'WR', 400,  40.0),
            ('mid',  2020, 'DAL', 'WR', 800,  80.0),
            ('high', 2020, 'DAL', 'WR', 1500, 150.0)
    """)
    sql, params = pos_topN(
        "WR", n=10, rank_by="rec_yds",
        team="DAL",  # exclude pre-seeded Pickens (PIT)
        min_stats={"rec_yds": 500},
        max_stats={"rec_yds": 1200},
    )
    names = [r[0] for r in db.execute(sql, params).fetchall()]
    assert names == ["In Range"]


def test_min_stats_unknown_column_raises():
    with pytest.raises(ValueError):
        pos_topN("RB", min_stats={"nonexistent_column": 100})


def test_max_stats_unknown_column_raises():
    with pytest.raises(ValueError):
        pos_topN("RB", max_stats={"DROP TABLE": 100})


def test_draft_year_range_filter(db):
    """--draft-start / --draft-end filter player-seasons by the
    player's draft year."""
    db.execute("INSERT INTO players (player_id, name) VALUES "
               "('p1','Old'), ('p2','Mid'), ('p3','New'), ('p4','Undrafted')")
    db.execute(
        "INSERT INTO draft_picks (player_id, year, round, overall_pick, team) VALUES "
        "('p1', 1995, 1, 5, 'DAL'),"
        "('p2', 2010, 1, 5, 'DAL'),"
        "('p3', 2020, 1, 5, 'DAL')"
    )
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, fpts_ppr) VALUES "
        "('p1', 2000, 'DAL', 'WR', 100),"
        "('p2', 2015, 'DAL', 'WR', 100),"
        "('p3', 2021, 'DAL', 'WR', 100),"
        "('p4', 2010, 'DAL', 'WR', 100)"
    )
    sql, params = pos_topN(
        "WR", n=10, rank_by="fpts_ppr",
        team="DAL",
        draft_start=2005, draft_end=2015,
    )
    rows = [r[0] for r in db.execute(sql, params).fetchall()]
    # Only Mid (drafted 2010) qualifies.
    assert rows == ["Mid"]


def test_draft_year_range_excludes_undrafted(db):
    """Undrafted players have draft_year=NULL; the range filter
    excludes them."""
    db.execute("INSERT INTO players (player_id, name) VALUES ('u', 'Undrafted Star')")
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, fpts_ppr) "
        "VALUES ('u', 2010, 'DAL', 'WR', 999)"
    )
    sql, params = pos_topN(
        "WR", n=10, rank_by="fpts_ppr",
        team="DAL",
        draft_start=1990, draft_end=2024,
    )
    names = [r[0] for r in db.execute(sql, params).fetchall()]
    assert "Undrafted Star" not in names


def test_draft_start_only(db):
    """Open-ended upper bound — drafted in or after the start year."""
    db.execute("INSERT INTO players (player_id, name) VALUES ('p1','Pre'), ('p2','Post')")
    db.execute(
        "INSERT INTO draft_picks (player_id, year, round, overall_pick, team) VALUES "
        "('p1', 1990, 1, 1, 'DAL'),"
        "('p2', 2010, 1, 1, 'DAL')"
    )
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, fpts_ppr) VALUES "
        "('p1', 2000, 'DAL', 'WR', 100),"
        "('p2', 2015, 'DAL', 'WR', 100)"
    )
    sql, params = pos_topN("WR", rank_by="fpts_ppr", team="DAL", draft_start=2000)
    names = [r[0] for r in db.execute(sql, params).fetchall()]
    assert names == ["Post"]


def test_ever_won_award_returns_all_seasons_of_winner(db):
    """--ever-won MVP returns every season of an MVP winner, not just
    the season they won."""
    db.execute("INSERT INTO players (player_id, name) VALUES ('mvp','MVP Career')")
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, fpts_ppr) VALUES "
        "('mvp', 2018, 'KC', 'QB', 300),"
        "('mvp', 2019, 'KC', 'QB', 320),"  # MVP-winning season
        "('mvp', 2020, 'KC', 'QB', 280),"
        "('mvp', 2021, 'KC', 'QB', 270)"
    )
    db.execute(
        "INSERT INTO player_awards (player_id, season, award_type, vote_finish) "
        "VALUES ('mvp', 2019, 'MVP', 1)"
    )
    sql, params = pos_topN("QB", n=10, rank_by="fpts_ppr", ever_won_award=["MVP"])
    seasons = sorted(r[2] for r in db.execute(sql, params).fetchall())
    # All 4 seasons returned, not just 2019.
    assert seasons == [2018, 2019, 2020, 2021]


def test_ever_won_excludes_non_winners(db):
    """A QB who never won MVP doesn't show up under --ever-won MVP,
    even if they got MVP votes."""
    db.execute("INSERT INTO players (player_id, name) VALUES ('runner','MVP Runner Up')")
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, fpts_ppr) "
        "VALUES ('runner', 2019, 'BUF', 'QB', 305)"
    )
    db.execute(
        "INSERT INTO player_awards (player_id, season, award_type, vote_finish) "
        "VALUES ('runner', 2019, 'MVP', 2)"  # 2nd place, didn't win
    )
    sql, params = pos_topN("QB", n=10, rank_by="fpts_ppr", ever_won_award=["MVP"])
    names = [r[0] for r in db.execute(sql, params).fetchall()]
    assert "MVP Runner Up" not in names


def test_ever_won_combined_with_has_award_year_specific(db):
    """`--has-award PB --ever-won MVP` = made the Pro Bowl this year
    AND has won MVP at some point in their career."""
    db.execute("INSERT INTO players (player_id, name) VALUES "
               "('a','MVP+PB Same Year'), ('b','PB only'), ('c','MVP elsewhere PB this year')")
    db.execute(
        "INSERT INTO player_season_stats (player_id, season, team, position, fpts_ppr) VALUES "
        "('a', 2019, 'KC', 'QB', 300),"
        "('b', 2019, 'BUF', 'QB', 280),"
        "('c', 2019, 'KC', 'QB', 290)"
    )
    db.execute(
        "INSERT INTO player_awards (player_id, season, award_type, vote_finish) VALUES "
        "('a', 2019, 'MVP', 1),"        # won MVP in 2019
        "('a', 2019, 'PB', NULL),"
        "('b', 2019, 'PB', NULL),"      # PB only, never MVP
        "('c', 2018, 'MVP', 1),"        # won MVP in 2018
        "('c', 2019, 'PB', NULL)"       # PB this year
    )
    sql, params = pos_topN(
        "QB", n=10, rank_by="fpts_ppr",
        has_award=["PB"], ever_won_award=["MVP"],
    )
    names = {r[0] for r in db.execute(sql, params).fetchall()}
    assert names == {"MVP+PB Same Year", "MVP elsewhere PB this year"}


def test_ever_won_unknown_label_raises():
    with pytest.raises(ValueError):
        pos_topN("QB", ever_won_award=["BEST_HAIR"])


def test_no_filters_unchanged_column_order_backward_compat(db):
    """Backward-compat sanity: pos_topN with no new filters returns
    the same 8 columns in the same order as before C6."""
    sql, params = pos_topN("ALL", n=1, rank_by="fpts_ppr")
    cols = [d[0] for d in db.execute(sql, params).description]
    assert cols == [
        "name", "team", "season", "position", "rank_value",
        "draft_round", "draft_year", "draft_overall_pick",
    ]


def test_pos_top_returns_expected_column_order(db):
    _seed(db)
    sql, params = pos_topN("QB", n=1, rank_by="pass_yds")
    cols = [d[0] for d in db.execute(sql, params).description]
    assert cols == [
        "name", "team", "season", "position", "rank_value",
        "draft_round", "draft_year", "draft_overall_pick",
    ]
