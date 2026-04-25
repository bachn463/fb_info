"""Fantasy point formulas for skill-position players (QB/RB/WR/TE).

Pure functions — no I/O, no DB, no state. Same formula every fantasy
site uses for standard / half-PPR / PPR; only the per-reception value
differs across the three modes.
"""

from __future__ import annotations

from dataclasses import dataclass

ScoringMode = str  # "std" | "half" | "ppr"

_REC_VALUE: dict[str, float] = {"std": 0.0, "half": 0.5, "ppr": 1.0}


@dataclass(frozen=True)
class StatLine:
    """The raw stat columns that contribute to fantasy points.

    Anything missing from a given PFR row (e.g. `fumbles_lost` pre-1994)
    should be passed as 0; loaders set the row-level `has_fumbles_lost`
    flag separately so queries can tell the difference.
    """

    pass_yds: int = 0
    pass_td: int = 0
    pass_int: int = 0
    rush_yds: int = 0
    rush_td: int = 0
    rec: int = 0
    rec_yds: int = 0
    rec_td: int = 0
    fumbles_lost: int = 0
    two_pt_pass: int = 0
    two_pt_rush: int = 0
    two_pt_rec: int = 0


def fantasy_points(s: StatLine, scoring: ScoringMode = "ppr") -> float:
    if scoring not in _REC_VALUE:
        raise ValueError(f"unknown scoring mode: {scoring!r}")
    rec_pt = _REC_VALUE[scoring]
    return (
        s.pass_yds / 25.0
        + 4 * s.pass_td
        - 2 * s.pass_int
        + s.rush_yds / 10.0
        + 6 * s.rush_td
        + s.rec_yds / 10.0
        + 6 * s.rec_td
        + rec_pt * s.rec
        - 2 * s.fumbles_lost
        + 2 * (s.two_pt_pass + s.two_pt_rush + s.two_pt_rec)
    )


def all_scoring(s: StatLine) -> dict[str, float]:
    return {mode: fantasy_points(s, mode) for mode in _REC_VALUE}
