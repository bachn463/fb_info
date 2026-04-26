from pathlib import Path

import pytest

from ffpts.parsers import parse_standings

FIX = Path(__file__).resolve().parent / "fixtures" / "standings"


@pytest.fixture(scope="module")
def standings_2023():
    return parse_standings((FIX / "2023.html").read_text(), season=2023)


@pytest.fixture(scope="module")
def standings_1985():
    return parse_standings((FIX / "1985.html").read_text(), season=1985)


def _by_franchise(rows, franchise):
    matching = [r for r in rows if r["franchise"] == franchise]
    assert matching, f"no row for franchise {franchise}"
    return matching[0]


def test_2023_returns_32_teams(standings_2023):
    assert len(standings_2023) == 32


def test_1985_returns_28_teams(standings_1985):
    assert len(standings_1985) == 28


def test_2023_bills_top_of_afc_east(standings_2023):
    bills = _by_franchise(standings_2023, "bills")
    assert bills["wins"] == 11
    assert bills["losses"] == 6
    assert bills["points"] == 451
    assert bills["points_against"] == 311
    assert bills["division"] == "AFC East"
    assert bills["conference"] == "AFC"


def test_2023_strips_seed_marker_from_team_name(standings_2023):
    """Bills (11-6, AFC East champion) shows as 'Buffalo Bills*' on the
    page; the parser strips the asterisk."""
    bills = _by_franchise(standings_2023, "bills")
    assert bills["team_display_name"] == "Buffalo Bills"


def test_1985_dolphins_first_place_afc_east(standings_1985):
    dolphins = _by_franchise(standings_1985, "dolphins")
    assert dolphins["wins"] == 12
    assert dolphins["losses"] == 4
    assert dolphins["division"] == "AFC East"
    assert dolphins["conference"] == "AFC"


def test_1985_chicago_15_1_super_bowl_year(standings_1985):
    """The '85 Bears went 15-1 — the gold-standard 1985 season."""
    bears = _by_franchise(standings_1985, "bears")
    assert bears["wins"] == 15
    assert bears["losses"] == 1
    assert bears["division"] == "NFC Central"


def test_division_count_2002_realignment_present_in_2023(standings_2023):
    """8 divisions of 4 teams each since 2002."""
    divisions = {r["division"] for r in standings_2023}
    expected = {
        "AFC East", "AFC North", "AFC South", "AFC West",
        "NFC East", "NFC North", "NFC South", "NFC West",
    }
    assert divisions == expected


def test_pre_realignment_divisions_in_1985(standings_1985):
    divisions = {r["division"] for r in standings_1985}
    expected = {
        "AFC East", "AFC Central", "AFC West",
        "NFC East", "NFC Central", "NFC West",
    }
    assert divisions == expected


def test_franchise_keys_match_pfr_franchise_table(standings_2023):
    from ffpts.normalize import PFR_FRANCHISE
    known = {f for f, _ in PFR_FRANCHISE.values()}
    for r in standings_2023:
        # Every standings row should have a franchise we know about.
        assert r["franchise"] in known, f"unknown franchise: {r}"


def test_team_slug_is_lowercase_three_letter(standings_2023):
    for r in standings_2023:
        slug = r["team_slug"]
        assert slug is not None
        assert slug == slug.lower()
        assert len(slug) == 3
