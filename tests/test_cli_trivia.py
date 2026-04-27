"""CliRunner-driven tests for `ffpts trivia play`. stdin pre-seeded
to simulate the interactive guessing loop."""

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
    con = connect(path)
    apply_schema(con)
    build(seasons=[1985], con=con, pfr_scraper=_scraper())
    con.close()


def test_trivia_correct_guess_prints_correct_line(tmp_path):
    """Guessing a player in the top-N prints the Correct! line."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "trivia", "play",
            "--rank-by", "rush_yds",
            "--n", "3",
            "--position", "RB",
            "--start", "1985", "--end", "1985",
            "--db", str(db),
        ],
        input="payton\ngive up\n",
    )
    assert result.exit_code == 0, result.output
    assert "Correct!" in result.output
    assert "Walter Payton" in result.output


def test_trivia_wrong_guess_prints_not_in_top(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "trivia", "play",
            "--rank-by", "rush_yds",
            "--n", "3",
            "--position", "RB",
            "--start", "1985", "--end", "1985",
            "--db", str(db),
        ],
        input="zzzdoesnotexist\ngive up\n",
    )
    assert result.exit_code == 0, result.output
    assert "Not in the top 3" in result.output


def test_trivia_give_up_reveals_remainder(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "trivia", "play",
            "--rank-by", "rush_yds",
            "--n", "3",
            "--position", "RB",
            "--start", "1985", "--end", "1985",
            "--db", str(db),
        ],
        input="give up\n",
    )
    assert result.exit_code == 0, result.output
    assert "Remaining:" in result.output
    # Marcus Allen led the league in 1985 with 1759 rush yds.
    assert "Marcus Allen" in result.output
    assert "Final score:" in result.output


def test_trivia_quit_exits_silently(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "trivia", "play",
            "--rank-by", "rush_yds",
            "--n", "3",
            "--position", "RB",
            "--start", "1985", "--end", "1985",
            "--db", str(db),
        ],
        input="quit\n",
    )
    assert result.exit_code == 0, result.output
    assert "Final score:" not in result.output
    assert "Remaining:" not in result.output


def test_trivia_hint_prints_clue(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "trivia", "play",
            "--rank-by", "rush_yds",
            "--n", "3",
            "--position", "RB",
            "--start", "1985", "--end", "1985",
            "--db", str(db),
        ],
        input="hint\nquit\n",
    )
    assert result.exit_code == 0, result.output
    assert "Hint:" in result.output


def test_trivia_complete_game_succeeds(tmp_path):
    """Guess all answers correctly -> 'All N found' message."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    # Top-3 RBs by rush_yds in 1985: Marcus Allen (1759), Gerald Riggs
    # (1719), Walter Payton (1551). Guess each.
    result = runner.invoke(
        app,
        [
            "trivia", "play",
            "--rank-by", "rush_yds",
            "--n", "3",
            "--position", "RB",
            "--start", "1985", "--end", "1985",
            "--db", str(db),
        ],
        input="marcus allen\ngerald riggs\nwalter payton\n",
    )
    assert result.exit_code == 0, result.output
    assert result.output.count("Correct!") == 3
    assert "All 3 found" in result.output


def test_trivia_no_results_exits_cleanly(tmp_path):
    """Filters that produce no answer set exit 0 with a message."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "trivia", "play",
            "--rank-by", "rush_yds",
            "--n", "5",
            "--position", "QB",
            "--team", "ZZZ",
            "--db", str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "nothing to guess" in result.output


def test_trivia_empty_input_does_not_crash(tmp_path):
    """Pressing enter with no guess shouldn't increment counters or
    crash."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        [
            "trivia", "play",
            "--rank-by", "rush_yds",
            "--n", "3",
            "--position", "RB",
            "--start", "1985", "--end", "1985",
            "--db", str(db),
        ],
        input="\n\nquit\n",
    )
    assert result.exit_code == 0, result.output


def test_trivia_help_lists_command():
    """`ffpts trivia` should be discoverable from --help."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "trivia" in result.output

    result = runner.invoke(app, ["trivia", "--help"])
    assert result.exit_code == 0
    assert "play" in result.output
