"""Tests for parse_year_summary_awards against committed PFR fixtures."""

from pathlib import Path

import pytest

from ffpts.parsers import parse_year_summary_awards


FIX = Path(__file__).resolve().parent / "fixtures" / "standings"


def test_wpmoy_1985_dwight_stephenson():
    """Dwight Stephenson won the 1985 Walter Payton MOY."""
    rows = parse_year_summary_awards((FIX / "1985.html").read_text(), 1985)
    wpmoy = [r for r in rows if r["award_type"] == "WPMOY"]
    assert len(wpmoy) == 1
    assert wpmoy[0]["player_id"] == "pfr:StepDw00"
    assert wpmoy[0]["name"] == "Dwight Stephenson"
    assert wpmoy[0]["season"] == 1985
    assert wpmoy[0]["vote_finish"] is None


def test_wpmoy_2023_cameron_heyward():
    """Cameron Heyward won the 2023 WPMOY."""
    rows = parse_year_summary_awards((FIX / "2023.html").read_text(), 2023)
    wpmoy = [r for r in rows if r["award_type"] == "WPMOY"]
    assert len(wpmoy) == 1
    assert wpmoy[0]["player_id"] == "pfr:HeywCa01"
    assert wpmoy[0]["name"] == "Cameron Heyward"
    assert wpmoy[0]["season"] == 2023
    assert wpmoy[0]["vote_finish"] is None


def test_no_award_rows_for_html_without_wpmoy():
    """Synthetic HTML with no WPMOY heading produces empty list."""
    assert parse_year_summary_awards("<html><body>nothing here</body></html>", 2020) == []


def test_returned_rows_match_pipeline_insert_schema():
    """Rows carry player_awards columns plus a `name` field for the
    players-table upsert (centers / kickers / etc may not have stats
    rows on our parsed pages so the pipeline can't get their names
    elsewhere)."""
    rows = parse_year_summary_awards((FIX / "1985.html").read_text(), 1985)
    assert rows, "expected at least one award row"
    expected_keys = {"player_id", "name", "season", "award_type", "vote_finish"}
    for r in rows:
        assert set(r.keys()) == expected_keys
