"""Tests for the inline-awards-cell parser. Pure-function, synthetic input."""

import pytest

from ffpts.parsers import parse_awards_string


def test_empty_input_returns_empty_list():
    assert parse_awards_string("") == []
    assert parse_awards_string(None) == []


def test_pro_bowl_only():
    assert parse_awards_string("PB") == [
        {"award_type": "PB", "vote_finish": None},
    ]


def test_first_team_all_pro():
    assert parse_awards_string("AP-1") == [
        {"award_type": "AP_FIRST", "vote_finish": None},
    ]


def test_second_team_all_pro():
    assert parse_awards_string("AP-2") == [
        {"award_type": "AP_SECOND", "vote_finish": None},
    ]


def test_wpmoy():
    assert parse_awards_string("WPMOY") == [
        {"award_type": "WPMOY", "vote_finish": None},
    ]


def test_ap_voted_award_won():
    assert parse_awards_string("AP MVP-1") == [
        {"award_type": "MVP", "vote_finish": 1},
    ]


def test_ap_voted_award_runner_up():
    assert parse_awards_string("AP MVP-2") == [
        {"award_type": "MVP", "vote_finish": 2},
    ]


@pytest.mark.parametrize("token,expected_type", [
    ("AP MVP-1",  "MVP"),
    ("AP OPoY-1", "OPOY"),
    ("AP DPoY-1", "DPOY"),
    ("AP OROY-1", "OROY"),
    ("AP DROY-1", "DROY"),
    ("AP CPoY-1", "CPOY"),
])
def test_each_ap_award_type_recognized(token, expected_type):
    out = parse_awards_string(token)
    assert out == [{"award_type": expected_type, "vote_finish": 1}]


def test_real_marino_1985_string():
    """Marino 1985 fixture: 'PB,AP-1'."""
    assert parse_awards_string("PB,AP-1") == [
        {"award_type": "PB",       "vote_finish": None},
        {"award_type": "AP_FIRST", "vote_finish": None},
    ]


def test_real_2023_complex_string():
    """Real value seen in 2023 fixtures: 'PB,AP-2,AP MVP-2,AP OPoY-5'."""
    assert parse_awards_string("PB,AP-2,AP MVP-2,AP OPoY-5") == [
        {"award_type": "PB",        "vote_finish": None},
        {"award_type": "AP_SECOND", "vote_finish": None},
        {"award_type": "MVP",       "vote_finish": 2},
        {"award_type": "OPOY",      "vote_finish": 5},
    ]


def test_real_tagovailoa_2023_string():
    """'PB,AP CPoY-5' — Pro Bowl plus 5th-place CPOY votes."""
    assert parse_awards_string("PB,AP CPoY-5") == [
        {"award_type": "PB",   "vote_finish": None},
        {"award_type": "CPOY", "vote_finish": 5},
    ]


def test_unknown_token_silently_skipped():
    """Unknown tokens are dropped, not raised — keeps the parser
    tolerant to new PFR strings we haven't catalogued."""
    out = parse_awards_string("PB,SomeFutureAward-1,AP MVP-1")
    assert out == [
        {"award_type": "PB",  "vote_finish": None},
        {"award_type": "MVP", "vote_finish": 1},
    ]


def test_extra_whitespace_around_tokens_handled():
    out = parse_awards_string(" PB ,  AP MVP-1 , AP-1 ")
    assert {(d["award_type"], d["vote_finish"]) for d in out} == {
        ("PB", None),
        ("MVP", 1),
        ("AP_FIRST", None),
    }


def test_empty_tokens_in_list_skipped():
    out = parse_awards_string("PB,,AP-1")
    assert len(out) == 2
