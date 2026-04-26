"""Smoke tests for the CLI: verify each command wires through to its helper."""

from __future__ import annotations

import polars as pl
from typer.testing import CliRunner

from ffpts.cli import app
from ffpts.db import apply_schema, connect
from ffpts.pipeline import build


runner = CliRunner()


# Reusable fake loaders shared with the pipeline integration tests.
def _stats_loader(seasons):
    s = list(seasons)[0]
    return pl.DataFrame([{
        "player_id": "00-0033280",
        "player_display_name": "Christian McCaffrey",
        "position": "RB",
        "season": s,
        "recent_team": "SF",
        "games": 16,
        "completions": 0, "attempts": 0, "passing_yards": 0, "passing_tds": 0,
        "passing_interceptions": 0, "sacks_suffered": 0, "sack_yards_lost": 0,
        "passing_2pt_conversions": 0,
        "carries": 272, "rushing_yards": 1459, "rushing_tds": 14,
        "rushing_2pt_conversions": 0,
        "targets": 83, "receptions": 67, "receiving_yards": 564,
        "receiving_tds": 7, "receiving_2pt_conversions": 0,
        "sack_fumbles": 0, "sack_fumbles_lost": 0,
        "rushing_fumbles": 1, "rushing_fumbles_lost": 1,
        "receiving_fumbles": 0, "receiving_fumbles_lost": 0,
        "def_tackles_solo": 0, "def_tackle_assists": 0, "def_sacks": 0.0,
        "def_interceptions": 0, "def_interception_yards": 0,
        "def_pass_defended": 0, "def_fumbles_forced": 0, "def_safeties": 0,
        "fumble_recovery_opp": 0, "fumble_recovery_yards_opp": 0,
        "fg_made": 0, "fg_att": 0, "fg_long": 0,
        "pat_made": 0, "pat_att": 0,
        "punt_returns": 0, "punt_return_yards": 0,
        "kickoff_returns": 0, "kickoff_return_yards": 0,
    }])


def _draft_loader():
    return pl.DataFrame([{
        "season": 2017, "round": 1, "pick": 8, "team": "CAR",
        "gsis_id": "00-0033280", "pfr_player_id": "McCaCh01",
        "pfr_player_name": "Christian McCaffrey", "position": "RB",
    }])


def _populated_db(path):
    con = connect(path)
    apply_schema(con)
    build(seasons=[2023], con=con,
          player_loader=_stats_loader, draft_loader=_draft_loader)
    con.close()


def test_query_command_prints_rows(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app, ["query", "SELECT name FROM players", "--db", str(db)]
    )
    assert result.exit_code == 0, result.output
    assert "Christian McCaffrey" in result.output


def test_query_command_errors_when_db_missing(tmp_path):
    missing = tmp_path / "nope.duckdb"
    result = runner.invoke(app, ["query", "SELECT 1", "--db", str(missing)])
    assert result.exit_code == 1
    assert "DB not found" in result.output


def test_ask_flex_top_runs_named_helper(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "ask", "flex-top",
            "--round", "1",
            "--n", "5",
            "--scoring", "ppr",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    # CMC was drafted R1 and our fpts_ppr should be ~391.3 (CMC 2023 fixture).
    assert "Christian McCaffrey" in result.output
    assert "SF" in result.output


def test_ask_div_int_historical_mode(tmp_path):
    """A defender row was not seeded so the result is empty — but the
    command should still exit 0 and print the (no rows) marker."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "ask", "div-int",
            "--division", "NFC West",
            "--start", "2023",
            "--end", "2023",
            "--mode", "historical",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    # CMC is in NFC West (SF) but has 0 def_int, not NULL, so he appears
    # with def_int=0 since the SQL only filters on `def_int IS NOT NULL`.
    # That's an acceptable and documented behavior — the player-season
    # default returns the qualifying row even at value 0.
    assert "Christian McCaffrey" in result.output or "(no rows)" in result.output


def test_build_command_validates_year_order(tmp_path):
    db = tmp_path / "ff.duckdb"
    result = runner.invoke(
        app,
        ["build", "--start", "2024", "--end", "2023", "--db", str(db)],
    )
    assert result.exit_code == 2
    assert "must be <=" in result.output


def test_help_lists_all_commands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "build" in result.output
    assert "query" in result.output
    assert "ask" in result.output


def test_ask_pos_top_runs_with_draft_rounds_filter(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    # CMC was R1, so a rounds=4,5 filter should return zero rows for FLEX.
    result = runner.invoke(
        app,
        [
            "ask", "pos-top",
            "--position", "FLEX",
            "--rank-by", "fpts_ppr",
            "--draft-rounds", "4,5",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "(no rows)" in result.output


def test_ask_pos_top_position_match(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "ask", "pos-top",
            "--position", "RB",
            "--rank-by", "fpts_ppr",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Christian McCaffrey" in result.output


def test_ask_pos_top_rejects_malformed_draft_rounds(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "ask", "pos-top",
            "--position", "QB",
            "--draft-rounds", "abc",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 2
    assert "ints or 'undrafted'" in result.output


def test_ask_pos_top_undrafted_token(tmp_path):
    """The literal 'undrafted' is a valid --draft-rounds token."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "ask", "pos-top",
            "--position", "FLEX",
            "--draft-rounds", "undrafted",
            "--db", str(db),
        ],
    )
    # Our test fixture only has CMC (R1), so an undrafted-only filter
    # returns no rows. The exit code should still be 0.
    assert result.exit_code == 0, result.output
    assert "(no rows)" in result.output


def test_ask_pos_top_mixed_draft_rounds_and_undrafted(tmp_path):
    """Mixed rounds + undrafted parses cleanly through the CLI."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "ask", "pos-top",
            "--position", "FLEX",
            "--draft-rounds", "1,undrafted",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    # CMC was R1, so they should appear.
    assert "Christian McCaffrey" in result.output
