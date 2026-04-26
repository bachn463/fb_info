from pathlib import Path

import pytest

from ffpts.parsers import parse_rushing

FIX = Path(__file__).resolve().parent / "fixtures" / "rushing"


@pytest.fixture(scope="module")
def rushing_2023():
    return parse_rushing((FIX / "2023.html").read_text(), season=2023)


@pytest.fixture(scope="module")
def rushing_1985():
    return parse_rushing((FIX / "1985.html").read_text(), season=1985)


def _by_name(rows, name):
    matching = [r for r in rows if r["name"] == name]
    assert matching, f"no row for {name}"
    return matching[0]


def test_rushing_2023_cmc_record_matches_published(rushing_2023):
    cmc = _by_name(rushing_2023, "Christian McCaffrey")
    assert cmc["player_id"] == "pfr:McCaCh01"
    assert cmc["team"] == "SFO"
    assert cmc["team_slug"] == "sfo"
    assert cmc["position"] == "RB"
    assert cmc["rush_att"] == 272
    assert cmc["rush_yds"] == 1459
    assert cmc["rush_td"] == 14


def test_rushing_1985_payton_published_line(rushing_1985):
    """Walter Payton 1985: 324 att, 1551 yds, 9 TD."""
    wp = _by_name(rushing_1985, "Walter Payton")
    assert wp["team"] == "CHI"
    assert wp["rush_yds"] == 1551
    assert wp["rush_td"] == 9
    assert wp["rush_att"] == 324


def test_rushing_returns_many_rows_per_year(rushing_2023, rushing_1985):
    assert len(rushing_2023) > 100
    assert len(rushing_1985) > 100


def test_rushing_drops_summary_rows(rushing_2023):
    for r in rushing_2023:
        assert r["player_id"] is not None
        assert r["player_id"].startswith("pfr:")


def test_rushing_per_player_team_unique(rushing_2023):
    keys = [(r["player_id"], r["team"]) for r in rushing_2023]
    assert len(keys) == len(set(keys))
