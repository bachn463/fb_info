import pytest

from ffpts.scoring import StatLine, all_scoring, fantasy_points


def test_zero_line_is_zero_in_all_modes():
    assert all_scoring(StatLine()) == {"std": 0.0, "half": 0.0, "ppr": 0.0}


def test_passing_components():
    s = StatLine(pass_yds=300, pass_td=2, pass_int=1)
    # 300/25 + 2*4 - 2 = 12 + 8 - 2 = 18
    assert fantasy_points(s, "std") == pytest.approx(18.0)
    # passing-only: identical across scoring modes
    assert fantasy_points(s, "half") == pytest.approx(18.0)
    assert fantasy_points(s, "ppr") == pytest.approx(18.0)


def test_rushing_components():
    s = StatLine(rush_yds=120, rush_td=1)
    # 120/10 + 6 = 18
    assert fantasy_points(s, "std") == pytest.approx(18.0)


def test_receiving_modes_differ_only_by_per_reception_value():
    s = StatLine(rec=10, rec_yds=100, rec_td=1)
    # base: 100/10 + 6 = 16; + rec*{0, 0.5, 1}
    assert fantasy_points(s, "std") == pytest.approx(16.0)
    assert fantasy_points(s, "half") == pytest.approx(21.0)
    assert fantasy_points(s, "ppr") == pytest.approx(26.0)


def test_fumbles_and_two_point_conversions():
    s = StatLine(fumbles_lost=2, two_pt_pass=1, two_pt_rush=1, two_pt_rec=1)
    # -4 from fumbles + 6 from three 2pcs
    assert fantasy_points(s, "ppr") == pytest.approx(2.0)


def test_negative_score_possible():
    s = StatLine(pass_int=5, fumbles_lost=3)
    # -10 - 6 = -16
    assert fantasy_points(s, "std") == pytest.approx(-16.0)


def test_christian_mccaffrey_2023_real_line():
    # CMC 2023: 1459 rush yds, 14 rush TD; 67 rec, 564 rec yds, 7 rec TD;
    # 1 fumble lost. No 2pcs.
    cmc = StatLine(
        rush_yds=1459,
        rush_td=14,
        rec=67,
        rec_yds=564,
        rec_td=7,
        fumbles_lost=1,
    )
    # Std:  145.9 + 84 + 56.4 + 42 - 2 = 326.3
    # Half: 326.3 + 33.5 = 359.8
    # PPR:  326.3 + 67   = 393.3
    pts = all_scoring(cmc)
    assert pts["std"] == pytest.approx(326.3, abs=0.01)
    assert pts["half"] == pytest.approx(359.8, abs=0.01)
    assert pts["ppr"] == pytest.approx(393.3, abs=0.01)


def test_unknown_scoring_mode_raises():
    with pytest.raises(ValueError):
        fantasy_points(StatLine(), "superflex")
