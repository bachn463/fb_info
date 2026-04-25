import pytest

from ffpts.normalize import (
    PFR_FRANCHISE,
    current_team_code_for_slug,
    franchise_for_slug,
    normalize_position,
)


def test_franchise_table_has_32_entries():
    assert len(PFR_FRANCHISE) == 32


def test_franchise_keys_are_unique_and_lowercase():
    keys = list(PFR_FRANCHISE.keys())
    assert all(k == k.lower() for k in keys)
    assert len(keys) == len(set(keys))


def test_franchise_values_are_unique():
    franchise_keys = [v[0] for v in PFR_FRANCHISE.values()]
    display_codes = [v[1] for v in PFR_FRANCHISE.values()]
    assert len(franchise_keys) == len(set(franchise_keys))
    assert len(display_codes) == len(set(display_codes))


def test_franchise_lookup_by_slug_is_case_insensitive():
    assert franchise_for_slug("crd") == "cardinals"
    assert franchise_for_slug("CRD") == "cardinals"
    assert franchise_for_slug("rai") == "raiders"
    assert franchise_for_slug("ram") == "rams"
    assert franchise_for_slug("oti") == "titans"


def test_current_team_code_lookup():
    assert current_team_code_for_slug("crd") == "ARI"
    assert current_team_code_for_slug("rai") == "LVR"
    assert current_team_code_for_slug("sdg") == "LAC"
    assert current_team_code_for_slug("oti") == "TEN"


def test_unknown_slug_returns_none():
    assert franchise_for_slug("zzz") is None
    assert current_team_code_for_slug("zzz") is None


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("RB", "RB"),
        ("HB", "RB"),
        ("hb", "RB"),
        ("TB", "RB"),
        ("WR-KR", "WR"),
        ("KR-WR", "WR"),
        ("FB-RB", "FB"),
        ("RB-FB", "RB"),
        ("QB", "QB"),
        ("TE", "TE"),
    ],
)
def test_normalize_position_known_aliases(raw, expected):
    assert normalize_position(raw) == expected


def test_normalize_position_falls_back_to_first_segment_for_unknown_combos():
    # "QB-WR" isn't in the alias table; we take the first segment.
    assert normalize_position("QB-WR") == "QB"
    assert normalize_position("CB/S") == "CB"


def test_normalize_position_handles_empty_and_none():
    assert normalize_position(None) is None
    assert normalize_position("") is None
    assert normalize_position("   ") is None
