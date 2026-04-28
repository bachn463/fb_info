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


def test_trivia_give_up_prints_full_ranked_list(tmp_path):
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
    assert "Final ranked list —" in result.output
    # Marcus Allen led the league in 1985 with 1759 rush yds.
    assert "Marcus Allen" in result.output
    assert "Final score:" in result.output


def test_trivia_quit_also_prints_full_ranked_list(tmp_path):
    """Even when quitting without guessing, the full ranked list
    is printed so the user always leaves with the answers."""
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
    assert "Final ranked list —" in result.output
    assert "Marcus Allen" in result.output
    assert "Final score: 0 / 3" in result.output


def test_trivia_full_finish_prints_full_ranked_list(tmp_path):
    """Same on a successful all-correct completion — list at end so
    the user can see all the answers and confirm."""
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
        input="marcus allen\ngerald riggs\nwalter payton\n",
    )
    assert result.exit_code == 0, result.output
    assert "All 3 found" in result.output
    assert "Final ranked list —" in result.output
    # All three names listed with the ✓ marker.
    assert "✓" in result.output
    assert "Marcus Allen" in result.output
    assert "Gerald Riggs" in result.output
    assert "Walter Payton" in result.output


def test_trivia_partial_score_marks_unfound_with_x(tmp_path):
    """Player you didn't guess shows the ✗ marker in the final list."""
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
        input="marcus allen\ngive up\n",
    )
    assert result.exit_code == 0, result.output
    assert "Final ranked list —" in result.output
    # Found marker (Marcus Allen) and missed marker (others).
    assert "✓" in result.output
    assert "✗" in result.output
    assert "Final score: 1 / 3" in result.output


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
    assert "Hint #1" in result.output


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


def test_trivia_title_describes_query_at_start_and_end(tmp_path):
    """The descriptive title appears at game start AND with the final
    ranked list at exit, including pos/year/scope info from the
    filters."""
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
    # Title fragments must appear (twice — opener + final list header).
    assert "Top 3 RB player-seasons by rush_yds (1985-1985)" in result.output
    assert "Final ranked list — Top 3 RB" in result.output


def test_trivia_title_includes_award_and_scope_clauses(tmp_path):
    """has-award + conference filters should surface in the title."""
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
            "--has-award", "PB",
            "--db", str(db),
        ],
        input="quit\n",
    )
    assert result.exit_code == 0, result.output
    assert "with award PB that season" in result.output


def test_trivia_hint_progressive_levels(tmp_path):
    """Calling `hint` repeatedly cycles through unfound players, and
    when it lands on the same player a second time it reveals one
    more layer than the first time."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    # n=1 means every `hint` lands on the same single answer, so each
    # call advances its level and reveals one more layer.
    result = runner.invoke(
        app,
        [
            "trivia", "play",
            "--rank-by", "rush_yds",
            "--n", "1",
            "--position", "RB",
            "--start", "1985", "--end", "1985",
            "--db", str(db),
        ],
        input="hint\nhint\nhint\nquit\n",
    )
    assert result.exit_code == 0, result.output
    assert "Hint #1 for #1:" in result.output
    assert "Hint #2 for #1:" in result.output
    assert "Hint #3 for #1:" in result.output
    # Level 1 reveals only team. Level 2 reveals team + year. Level 3
    # adds position. So #2 must contain "year" and #3 must contain
    # "position", but #1 must NOT contain "year".
    h1_line = next(l for l in result.output.splitlines() if "Hint #1 for #1:" in l)
    h2_line = next(l for l in result.output.splitlines() if "Hint #2 for #1:" in l)
    h3_line = next(l for l in result.output.splitlines() if "Hint #3 for #1:" in l)
    assert "team" in h1_line and "year" not in h1_line
    assert "team" in h2_line and "year" in h2_line
    assert "position" in h3_line


def test_trivia_play_rejects_age_rank_by(tmp_path):
    """`age` is a valid `ask pos-top` rank-by but trivia disallows it
    (trivial answer set: just the oldest player)."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["trivia", "play", "--rank-by", "age", "--n", "5",
         "--position", "RB", "--db", str(db)],
    )
    assert result.exit_code == 2
    assert "age" in result.output


def test_trivia_play_rejects_draft_year_rank_by(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["trivia", "play", "--rank-by", "draft_year", "--n", "5",
         "--db", str(db)],
    )
    assert result.exit_code == 2


def test_trivia_random_rejects_age_rank_by(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["trivia", "random", "--rank-by", "age", "--db", str(db)],
    )
    assert result.exit_code == 2


def test_trivia_help_lists_command():
    """`ffpts trivia` should be discoverable from --help."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "trivia" in result.output

    result = runner.invoke(app, ["trivia", "--help"])
    assert result.exit_code == 0
    assert "play" in result.output
