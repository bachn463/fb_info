from pathlib import Path

import pytest

from ffpts.parsers import parse_receiving

FIX = Path(__file__).resolve().parent / "fixtures" / "receiving"


@pytest.fixture(scope="module")
def receiving_2023():
    return parse_receiving((FIX / "2023.html").read_text(), season=2023)


@pytest.fixture(scope="module")
def receiving_1985():
    return parse_receiving((FIX / "1985.html").read_text(), season=1985)


def _by_name(rows, name):
    matching = [r for r in rows if r["name"] == name]
    assert matching, f"no row for {name}"
    return matching[0]


def test_cmc_2023_receiving_line(receiving_2023):
    cmc = _by_name(receiving_2023, "Christian McCaffrey")
    assert cmc["targets"] == 83
    assert cmc["rec"] == 67
    assert cmc["rec_yds"] == 564
    assert cmc["rec_td"] == 7
    assert cmc["team"] == "SFO"


def test_lamb_2023_targets_and_receptions(receiving_2023):
    """CeeDee Lamb led 2023 receiving yards: 135 rec, 1749 yds, 12 TD."""
    lamb = _by_name(receiving_2023, "CeeDee Lamb")
    assert lamb["rec"] == 135
    assert lamb["rec_yds"] == 1749
    assert lamb["rec_td"] == 12


def test_1985_roger_craig_published_line(receiving_1985):
    """Roger Craig 1985: 92 rec, 1016 yds, 6 rec TD — first 1k/1k season."""
    craig = _by_name(receiving_1985, "Roger Craig")
    assert craig["rec"] == 92
    assert craig["rec_yds"] == 1016
    assert craig["rec_td"] == 6


def test_1985_targets_column_present(receiving_1985):
    """PFR has back-filled targets to 1985 via play-by-play archives."""
    craig = _by_name(receiving_1985, "Roger Craig")
    assert craig["targets"] is not None
    assert craig["targets"] > 0


def test_returns_many_rows(receiving_2023, receiving_1985):
    assert len(receiving_2023) > 200
    assert len(receiving_1985) > 200


def test_per_player_team_unique(receiving_2023):
    keys = [(r["player_id"], r["team"]) for r in receiving_2023]
    assert len(keys) == len(set(keys))
