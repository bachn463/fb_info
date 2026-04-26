from pathlib import Path

import pytest

from ffpts.parsers import parse_draft

FIX = Path(__file__).resolve().parent / "fixtures" / "draft"


@pytest.fixture(scope="module")
def draft_1985():
    return parse_draft((FIX / "1985.html").read_text(), season=1985)


@pytest.fixture(scope="module")
def draft_2023():
    return parse_draft((FIX / "2023.html").read_text(), season=2023)


def _by_pick(rows, year_round, overall):
    matching = [
        r for r in rows
        if r["round"] == year_round and r["overall_pick"] == overall
    ]
    assert matching, f"no row for round={year_round} pick={overall}"
    return matching[0]


def test_1985_first_overall_pick_bruce_smith(draft_1985):
    p = _by_pick(draft_1985, 1, 1)
    assert p["name"] == "Bruce Smith"
    assert p["team"] == "BUF"
    assert p["position"] == "DE"
    assert p["year"] == 1985
    assert p["player_id"] == "pfr:SmitBr00"


def test_1985_jerry_rice_round1_pick16_sfo(draft_1985):
    rice = next(r for r in draft_1985 if r["name"] == "Jerry Rice")
    assert rice["round"] == 1
    assert rice["overall_pick"] == 16
    assert rice["team"] == "SFO"
    assert rice["player_id"] == "pfr:RiceJe00"


def test_2023_first_overall_pick_bryce_young(draft_2023):
    p = _by_pick(draft_2023, 1, 1)
    assert p["name"] == "Bryce Young"
    assert p["team"] == "CAR"
    assert p["position"] == "QB"
    assert p["year"] == 2023


def test_returns_full_draft_classes(draft_1985, draft_2023):
    # 1985 had 12 rounds, 336 picks; 2023 had 7 rounds, 259 picks.
    # Picks without a PFR player slug (never made an NFL roster) are
    # dropped by the parser, so the count is lower than the gross
    # draft total. Both years should still cover the meaningful
    # majority.
    assert len(draft_1985) > 200
    assert len(draft_2023) > 200


def test_strips_hof_suffix_from_names(draft_1985):
    """PFR appends 'HOF' to Hall of Famers' names; the parser strips it."""
    smith = next(r for r in draft_1985 if r["player_id"] == "pfr:SmitBr00")
    assert smith["name"] == "Bruce Smith"
    assert "HOF" not in smith["name"]


def test_player_id_uses_pfr_prefix(draft_1985):
    for r in draft_1985:
        assert r["player_id"].startswith("pfr:")


def test_no_duplicate_overall_picks(draft_1985):
    picks = [r["overall_pick"] for r in draft_1985]
    assert len(picks) == len(set(picks))
