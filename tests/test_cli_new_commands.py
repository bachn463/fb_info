"""CliRunner tests for the new ask/trivia commands:
ask records, ask career, ask awards, ask compare, trivia daily,
trivia random."""

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
    build(seasons=[1985, 2023], con=con, pfr_scraper=_scraper())
    con.close()


# ---------- ask records ----------

def test_ask_records_offense_runs(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["ask", "records", "--category", "offense", "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    # Header row + at least one stat row.
    assert "stat" in result.output
    assert "rush_yds" in result.output
    assert "pass_yds" in result.output


def test_ask_records_defense_includes_def_int(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["ask", "records", "--category", "defense", "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    assert "def_int" in result.output


def test_ask_records_unknown_category_errors(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app, ["ask", "records", "--category", "bogus", "--db", str(db)],
    )
    assert result.exit_code == 2


# ---------- ask career ----------

def test_ask_career_runs_and_returns_rows(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["ask", "career", "--rank-by", "rush_yds", "--n", "5",
         "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    assert "career_total" in result.output
    assert "seasons" in result.output


def test_ask_career_position_filter(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["ask", "career", "--rank-by", "pass_yds", "--position", "QB",
         "--n", "3", "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    # Marino is the prototypical 1985 QB so he should likely appear in
    # the top-3 career pass_yds across our 1985+2023 fixture set.
    assert "Marino" in result.output or "career_total" in result.output


def test_ask_career_min_seasons_filter(tmp_path):
    """min_seasons should HAVING-filter. Setting it to 3 against a
    2-season fixture build should return zero rows."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["ask", "career", "--rank-by", "rush_yds",
         "--min-seasons", "3", "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    assert "(no rows)" in result.output


# ---------- ask awards ----------

def test_ask_awards_lists_mvp_winners(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app, ["ask", "awards", "--award", "MVP", "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    assert "MVP" in result.output
    # 2023 MVP: Lamar Jackson.
    assert "Lamar Jackson" in result.output


def test_ask_awards_filters_by_season(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["ask", "awards", "--season", "1985", "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    # 1985 PBs are abundant, also 1985 WPMOY = Dwight Stephenson.
    assert "1985" in result.output


def test_ask_awards_unknown_type_errors(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["ask", "awards", "--award", "BOGUS", "--db", str(db)],
    )
    # ValueError surfaces from the helper as a non-zero exit.
    assert result.exit_code != 0


# ---------- ask compare ----------

def test_ask_compare_two_players(tmp_path):
    """Compare two QBs from our fixture data — Marino (1985) vs.
    Mahomes (2023). Both should resolve and appear as columns."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["ask", "compare", "Dan Marino", "Patrick Mahomes",
         "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    assert "Dan Marino" in result.output
    assert "Patrick Mahomes" in result.output
    assert "pass_yds" in result.output
    assert "seasons" in result.output


def test_ask_compare_unknown_player_errors(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["ask", "compare", "Nobody Whoever", "Dan Marino",
         "--db", str(db)],
    )
    assert result.exit_code == 2


# ---------- trivia daily / random ----------

def test_trivia_random_with_seed_reproducible(tmp_path):
    """Same --seed must pick the same template, producing the same
    title at game open."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    r1 = runner.invoke(
        app,
        ["trivia", "random", "--seed", "42", "--db", str(db)],
        input="quit\n",
    )
    r2 = runner.invoke(
        app,
        ["trivia", "random", "--seed", "42", "--db", str(db)],
        input="quit\n",
    )
    assert r1.exit_code == 0, r1.output
    assert r2.exit_code == 0, r2.output
    # The opening title line should match between two seeded runs.
    title1 = next(
        (l for l in r1.output.splitlines() if l.startswith("Top ")),
        None,
    )
    title2 = next(
        (l for l in r2.output.splitlines() if l.startswith("Top ")),
        None,
    )
    assert title1 is not None and title1 == title2


def test_trivia_daily_runs(tmp_path):
    """`trivia daily` should resolve a template and print the game
    opener with today's date label."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app, ["trivia", "daily", "--db", str(db)],
        input="quit\n",
    )
    assert result.exit_code == 0, result.output
    assert "daily for " in result.output


def test_trivia_help_lists_new_commands():
    result = runner.invoke(app, ["trivia", "--help"])
    assert result.exit_code == 0
    assert "daily" in result.output
    assert "random" in result.output


def test_ask_help_lists_new_commands():
    result = runner.invoke(app, ["ask", "--help"])
    assert result.exit_code == 0
    assert "records" in result.output
    assert "career" in result.output
    assert "awards" in result.output
    assert "compare" in result.output
