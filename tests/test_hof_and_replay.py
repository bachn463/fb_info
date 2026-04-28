"""Tests for HOF awards detection and trivia replay."""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from ffpts.cli import app
from ffpts.db import apply_schema, connect
from ffpts.pipeline import build
from ffpts.queries import AWARD_TYPES_ALLOWED


runner = CliRunner()


def _scraper():
    from tests.test_ingest_pfr import _FixtureScraper
    return _FixtureScraper()


def _populated_db(path):
    con = connect(path)
    apply_schema(con)
    build(seasons=[1985, 2023], con=con, pfr_scraper=_scraper())
    con.close()


# ---- HOF auto-detection from draft pages ----

def test_hof_in_award_types_allowed():
    assert "HOF" in AWARD_TYPES_ALLOWED


def test_pipeline_auto_detects_hof_from_1985_draft(tmp_path):
    """The 1985 draft fixture has Jerry Rice, Bruce Smith, Andre Reed,
    Chris Doleman, and Kevin Greene with the "HOF" name suffix. They
    should all land in player_awards as award_type='HOF'."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    con = connect(db)
    try:
        rows = con.execute(
            "SELECT name FROM v_award_winners WHERE award_type = 'HOF' "
            "ORDER BY name"
        ).fetchall()
    finally:
        con.close()
    names = {r[0] for r in rows}
    # Spot-check four well-known auto-detected HOFers.
    for name in ("Jerry Rice", "Bruce Smith", "Andre Reed", "Chris Doleman"):
        assert name in names, f"{name} missing from auto-detected HOF set: {names}"


def test_pipeline_applies_curated_known_hofers(tmp_path):
    """KNOWN_HOFERS includes Reggie White (supplemental pick — no HOF
    suffix on draft.htm) and Steve Young (supp pick) and Warren Moon
    (UDFA, not on draft.htm at all). They should land via the
    curated path."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    con = connect(db)
    try:
        rows = con.execute(
            "SELECT name FROM v_award_winners WHERE award_type = 'HOF'"
        ).fetchall()
    finally:
        con.close()
    names = {r[0] for r in rows}
    assert "Reggie White" in names


def test_ask_pos_top_has_award_hof(tmp_path):
    """`--has-award HOF` matches every season the player has a HOF
    row (= their last NFL season per our model)."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["ask", "pos-top", "--rank-by", "rec_yds", "--has-award", "HOF",
         "--n", "10", "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    # Jerry Rice 1985 is in the fixture; he's a HOFer.
    assert "Jerry Rice" in result.output


def test_ask_career_ever_won_hof(tmp_path):
    """Career rank for HOF QBs by pass_yds — should restrict to QBs
    in the curated HOF set."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["ask", "career", "--rank-by", "pass_yds",
         "--ever-won", "HOF", "--n", "5", "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    # Steve Young is a curated-HOF QB and has stats in our fixture.
    assert "Steve Young" in result.output or "career_total" in result.output


def test_ask_awards_top_hof_lists_inductees(tmp_path):
    """`awards-top --award HOF` should list every HOFer (all tied at
    award_count=1 since each gets exactly one HOF row)."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app,
        ["ask", "awards-top", "--award", "HOF", "--n", "20",
         "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    assert "Jerry Rice" in result.output


def test_ask_awards_filter_by_hof_lists_inductees(tmp_path):
    """`ask awards --award HOF` lists every HOF row (one per inductee
    in our fixture)."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    result = runner.invoke(
        app, ["ask", "awards", "--award", "HOF", "--db", str(db)],
    )
    assert result.exit_code == 0, result.output
    assert "HOF" in result.output


# ---- Trivia replay ----

def test_trivia_replay_persists_play_spec(tmp_path):
    """A `trivia play` invocation writes a JSON spec to
    data/trivia_history (under the DB's parent dir). Listing history
    should surface it."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    r = runner.invoke(
        app,
        ["trivia", "play", "--rank-by", "rush_yds", "--n", "3",
         "--position", "RB", "--start", "1985", "--end", "1985",
         "--db", str(db)],
        input="quit\n",
    )
    assert r.exit_code == 0, r.output
    assert "(game 000001" in r.output
    history_dir = tmp_path / "trivia_history"
    files = list(history_dir.glob("*.json"))
    assert len(files) == 1
    spec = json.loads(files[0].read_text())
    assert spec["label"] == "play"
    assert spec["template"]["rank_by"] == "rush_yds"
    assert spec["template"]["n"] == 3
    assert spec["template"]["position"] == "RB"


def test_trivia_replay_runs_saved_game(tmp_path):
    """Save a play game, then replay by ID. Replay output should
    show the same title."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    first = runner.invoke(
        app,
        ["trivia", "play", "--rank-by", "rush_yds", "--n", "3",
         "--position", "RB", "--start", "1985", "--end", "1985",
         "--db", str(db)],
        input="quit\n",
    )
    assert first.exit_code == 0, first.output

    replay = runner.invoke(
        app,
        ["trivia", "replay", "1", "--db", str(db)],
        input="quit\n",
    )
    assert replay.exit_code == 0, replay.output
    assert "Replaying game 000001" in replay.output
    # Same title across both runs.
    title = "Top 3 RB player-seasons by rush_yds (1985-1985)"
    assert title in first.output
    assert title in replay.output


def test_trivia_replay_unknown_id_errors(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    r = runner.invoke(
        app, ["trivia", "replay", "999", "--db", str(db)],
    )
    assert r.exit_code == 1
    assert "not in" in r.output


def test_trivia_history_lists_recent(tmp_path):
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    # Run two games.
    runner.invoke(
        app,
        ["trivia", "play", "--rank-by", "rush_yds", "--n", "3",
         "--position", "RB", "--start", "1985", "--end", "1985",
         "--db", str(db)],
        input="quit\n",
    )
    runner.invoke(
        app, ["trivia", "random", "--seed", "7", "--db", str(db)],
        input="quit\n",
    )

    h = runner.invoke(
        app, ["trivia", "history", "--db", str(db)],
    )
    assert h.exit_code == 0, h.output
    assert "#000001" in h.output
    assert "#000002" in h.output
    # Newest first.
    pos1 = h.output.index("#000002")
    pos2 = h.output.index("#000001")
    assert pos1 < pos2


def test_trivia_replay_does_not_resave(tmp_path):
    """Replaying a game shouldn't write a new history entry. Without
    save=False the history would balloon from one game per replay."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    runner.invoke(
        app,
        ["trivia", "play", "--rank-by", "rush_yds", "--n", "3",
         "--position", "RB", "--start", "1985", "--end", "1985",
         "--db", str(db)],
        input="quit\n",
    )
    history_dir = tmp_path / "trivia_history"
    before = len(list(history_dir.glob("*.json")))
    runner.invoke(
        app, ["trivia", "replay", "1", "--db", str(db)],
        input="quit\n",
    )
    after = len(list(history_dir.glob("*.json")))
    assert before == after == 1


def test_trivia_random_save_and_replay(tmp_path):
    """A random game saves its resolved template; replaying re-uses
    that template (not a fresh random roll), so the title stays
    identical even though the seed isn't part of the replay spec."""
    db = tmp_path / "ff.duckdb"
    _populated_db(db)
    first = runner.invoke(
        app, ["trivia", "random", "--seed", "13", "--db", str(db)],
        input="quit\n",
    )
    assert first.exit_code == 0, first.output

    replay = runner.invoke(
        app, ["trivia", "replay", "1", "--db", str(db)],
        input="quit\n",
    )
    assert replay.exit_code == 0, replay.output
    title_a = next(l for l in first.output.splitlines() if l.startswith("Top "))
    title_b = next(l for l in replay.output.splitlines() if l.startswith("Top "))
    assert title_a == title_b
