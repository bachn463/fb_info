from pathlib import Path

import pytest

from ffpts.parsers import parse_defense

FIX = Path(__file__).resolve().parent / "fixtures" / "defense"


@pytest.fixture(scope="module")
def defense_2023():
    return parse_defense((FIX / "2023.html").read_text(), season=2023)


@pytest.fixture(scope="module")
def defense_1985():
    return parse_defense((FIX / "1985.html").read_text(), season=1985)


def _by_name(rows, name):
    matching = [r for r in rows if r["name"] == name]
    assert matching, f"no row for {name}"
    return matching[0]


def test_tj_watt_2023_led_league_in_sacks(defense_2023):
    """T.J. Watt: 19.0 sacks (NFL leader), 1 INT, 4 FF."""
    watt = _by_name(defense_2023, "T.J. Watt")
    assert watt["team"] == "PIT"
    assert watt["def_sacks"] == pytest.approx(19.0)
    assert watt["def_int"] == 1
    assert watt["def_fumbles_forced"] == 4


def test_lawrence_taylor_1985_pass_rusher(defense_1985):
    """LT 1985: 13.0 sacks, 4 forced fumbles, no INTs."""
    lt = _by_name(defense_1985, "Lawrence Taylor")
    assert lt["team"] == "NYG"
    assert lt["def_sacks"] == pytest.approx(13.0)
    assert lt["def_fumbles_forced"] == 4
    assert lt["def_int"] == 0


def test_reggie_white_1985_combined_tackles(defense_1985):
    """Reggie White 1985: 13.0 sacks, 100 solo tackles."""
    white = _by_name(defense_1985, "Reggie White")
    assert white["def_sacks"] == pytest.approx(13.0)
    assert white["def_tackles_solo"] == 100


def test_andre_tippett_1985_sack_total(defense_1985):
    """Andre Tippett's 16.5-sack 1985 — illustrates the .5 sack
    handling: PFR's value comes through as a float."""
    tippett = _by_name(defense_1985, "Andre Tippett")
    assert tippett["def_sacks"] == pytest.approx(16.5)


def test_pass_defended_present_in_2023_absent_in_1985(defense_2023, defense_1985):
    """PFR didn't track passes-defended in 1985 (the column doesn't
    exist on that page), so it should come back NULL for 1985 rows
    and an integer for 2023."""
    watt = _by_name(defense_2023, "T.J. Watt")
    assert isinstance(watt["def_pass_def"], int)

    lt = _by_name(defense_1985, "Lawrence Taylor")
    assert lt["def_pass_def"] is None


def test_returns_many_rows(defense_2023, defense_1985):
    assert len(defense_2023) > 500
    assert len(defense_1985) > 400


def test_per_player_team_unique(defense_2023):
    keys = [(r["player_id"], r["team"]) for r in defense_2023]
    assert len(keys) == len(set(keys))


def test_player_id_uses_pfr_prefix(defense_2023):
    for r in defense_2023:
        assert r["player_id"].startswith("pfr:")
