"""Smoke tests for the CLI: verify each command wires through to its helper.

Builds a tiny populated DuckDB once per test from the committed
1985 PFR fixtures (no network), then runs the CLI commands against
it via typer's CliRunner.
"""

from __future__ import annotations

from typer.testing import CliRunner

from ffpts.cli import app
from ffpts.db import apply_schema, connect
from ffpts.pipeline import build


runner = CliRunner()


def _scraper():
    from tests.test_ingest_pfr import _FixtureScraper
    return _FixtureScraper()


def _populated_db(path):
    """Build a 1985 DB at `path` from the committed PFR fixtures."""
    con = connect(path)
    apply_schema(con)
    build(seasons=[1985], con=con, pfr_scraper=_scraper())
    con.close()


# --- Raw query / help / error paths -------------------------------------


def test_query_command_prints_rows(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "query",
            "SELECT name FROM players WHERE name = 'Walter Payton'",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Walter Payton" in result.output


def test_query_command_errors_when_db_missing(tmp_path):
    missing = tmp_path / "nope.duckdb"
    result = runner.invoke(app, ["query", "SELECT 1", "--db", str(missing)])
    assert result.exit_code == 1
    assert "DB not found" in result.output


def test_help_lists_all_commands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "build" in result.output
    assert "query" in result.output
    assert "ask" in result.output


def test_build_command_validates_year_order(tmp_path):
    db = tmp_path / "ff.duckdb"
    result = runner.invoke(
        app,
        ["build", "--start", "2024", "--end", "2023", "--db", str(db)],
    )
    assert result.exit_code == 2
    assert "must be <=" in result.output


# --- ask flex-top -------------------------------------------------------


def test_ask_flex_top_jerry_rice_round_1(tmp_path):
    """1985 R1 P16 SFO Jerry Rice is the only FLEX (RB/WR/TE) drafted
    in round 1 in our 1985 fixture set — other R1 picks were
    non-FLEX (DE/G/DT/OL)."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "ask", "flex-top",
            "--round", "1",
            "--n", "10",
            "--scoring", "ppr",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Jerry Rice" in result.output


# --- ask div-int --------------------------------------------------------


def test_ask_div_int_nfc_central_1985(tmp_path):
    """The Bears '85 had multiple defenders with INTs; query 1985 NFC
    Central."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "ask", "div-int",
            "--division", "NFC Central",
            "--start", "1985",
            "--end", "1985",
            "--mode", "historical",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    # Some defender from a 1985 NFC Central team should be in the
    # output (Mike Singletary, Mike Richardson, Wilber Marshall, etc.).
    assert "CHI" in result.output or "DET" in result.output \
        or "GNB" in result.output or "MIN" in result.output \
        or "TAM" in result.output


# --- ask pos-top --------------------------------------------------------


def test_ask_pos_top_position_match_walter_payton(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "ask", "pos-top",
            "--position", "RB",
            "--rank-by", "rush_yds",
            "--n", "5",
            "--start", "1985",
            "--end", "1985",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    # Payton 1551 yds led the league that year (or close to).
    assert "Walter Payton" in result.output


def test_ask_pos_top_runs_with_draft_rounds_filter(tmp_path):
    """Round 4-5 FLEX in 1985 — fixture has some R4/R5 picks who
    played that year."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
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
    # Either some matches landed or "(no rows)" — both are valid.
    assert result.exit_code == 0, result.output


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
    """'undrafted' is a valid --draft-rounds token. Most 1985 players
    were drafted before 1985 and thus have no draft entry in our
    fixture-only DB — they show as undrafted here."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "ask", "pos-top",
            "--position", "FLEX",
            "--draft-rounds", "undrafted",
            "--rank-by", "fpts_ppr",
            "--n", "5",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    # Walter Payton (drafted 1975) has no draft entry in our 1985-only
    # fixture DB — he shows as "undrafted" by the LEFT JOIN.
    assert "Walter Payton" in result.output


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
            "--rank-by", "fpts_ppr",
            "--n", "100",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    # Both groups represented:
    # - Walter Payton: drafted 1975 (no entry in our 1985-only fixture
    #   DB) -> shows as undrafted via the LEFT JOIN.
    # - Jerry Rice: 1985 R1 P16 SFO rookie -> draft_round=1 in DB.
    assert "Walter Payton" in result.output
    assert "Jerry Rice" in result.output
