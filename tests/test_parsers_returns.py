from pathlib import Path

import pytest

from ffpts.parsers import parse_returns

FIX = Path(__file__).resolve().parent / "fixtures" / "returns"


@pytest.fixture(scope="module")
def returns_2023():
    return parse_returns((FIX / "2023.html").read_text(), season=2023)


@pytest.fixture(scope="module")
def returns_1985():
    return parse_returns((FIX / "1985.html").read_text(), season=1985)


def _by_name(rows, name):
    matching = [r for r in rows if r["name"] == name]
    assert matching, f"no row for {name}"
    return matching[0]


def test_2023_top_punt_returner(returns_2023):
    """Brandon Powell led 2023 punt returns by attempts (37/289)."""
    p = _by_name(returns_2023, "Brandon Powell")
    assert p["pr"] == 37
    assert p["pr_yds"] == 289
    assert p["pr_td"] == 0


def test_2023_smith_marsette_punt_return_td(returns_2023):
    """Ihmir Smith-Marsette had 1 PR TD in 2023 (37 returns, 322 yds)."""
    s = _by_name(returns_2023, "Ihmir Smith-Marsette")
    assert s["pr"] == 37
    assert s["pr_yds"] == 322
    assert s["pr_td"] == 1


def test_1985_fulton_walker_top_pr(returns_1985):
    """Fulton Walker 1985 (MIA): 62 punt returns, 692 yds (led the league)."""
    fw = _by_name(returns_1985, "Fulton Walker")
    assert fw["pr"] == 62
    assert fw["pr_yds"] == 692
    assert fw["kr"] == 21
    assert fw["kr_yds"] == 467


def test_1985_robbie_martin_pr_td(returns_1985):
    """Robbie Martin 1985 had 1 PR TD on 40 returns, plus 32 KR."""
    rm = _by_name(returns_1985, "Robbie Martin")
    assert rm["pr"] == 40
    assert rm["pr_td"] == 1
    assert rm["kr"] == 32


def test_returns_many_rows(returns_2023, returns_1985):
    assert len(returns_2023) > 50
    assert len(returns_1985) > 50


def test_per_player_team_unique(returns_2023):
    keys = [(r["player_id"], r["team"]) for r in returns_2023]
    assert len(keys) == len(set(keys))
