from pathlib import Path

import pytest

from ffpts.parsers import parse_kicking

FIX = Path(__file__).resolve().parent / "fixtures" / "kicking"


@pytest.fixture(scope="module")
def kicking_2023():
    return parse_kicking((FIX / "2023.html").read_text(), season=2023)


@pytest.fixture(scope="module")
def kicking_1985():
    return parse_kicking((FIX / "1985.html").read_text(), season=1985)


def _by_name(rows, name):
    matching = [r for r in rows if r["name"] == name]
    assert matching, f"no row for {name}"
    return matching[0]


def test_aubrey_2023_rookie_record_fg_made(kicking_2023):
    """Brandon Aubrey 2023 (DAL): 36/38 FG including a 60-yarder."""
    a = _by_name(kicking_2023, "Brandon Aubrey")
    assert a["team"] == "DAL"
    assert a["fg_made"] == 36
    assert a["fg_att"] == 38
    assert a["fg_long"] == 60


def test_justin_tucker_2023_xp(kicking_2023):
    """Tucker 2023 (BAL): 32/37 FG, 51/51 XP."""
    t = _by_name(kicking_2023, "Justin Tucker")
    assert t["team"] == "BAL"
    assert t["fg_made"] == 32
    assert t["xp_made"] == 51


def test_kevin_butler_1985_rookie_year(kicking_1985):
    """Kevin Butler 1985 (CHI rookie): 31/37 FG, 51 XP."""
    b = _by_name(kicking_1985, "Kevin Butler")
    assert b["team"] == "CHI"
    assert b["fg_made"] == 31
    assert b["fg_att"] == 37
    assert b["xp_made"] == 51
    assert b["fg_long"] == 46


def test_morten_andersen_1985(kicking_1985):
    a = _by_name(kicking_1985, "Morten Andersen")
    assert a["fg_made"] == 31
    assert a["fg_long"] == 55


def test_returns_many_rows(kicking_2023, kicking_1985):
    assert len(kicking_2023) > 20
    assert len(kicking_1985) > 20


def test_per_player_team_unique(kicking_2023):
    keys = [(r["player_id"], r["team"]) for r in kicking_2023]
    assert len(keys) == len(set(keys))
