"""Named SQL helpers for common questions.

Each helper returns a ``(sql, params)`` tuple ready for use with
``con.execute(sql, params)``. The CLI and ad-hoc callers can pick a
helper rather than write the SQL each time.

**Project default unit of analysis: the player-season.** Every helper
here ranks player-seasons (one row per (player, season, team)) — *not*
career totals or team aggregates. The same player appears multiple
times if multiple of their seasons qualify. Career and team-aggregate
rollups will be added as separate, explicitly-named helpers if and
when needed; they are never the default.
"""

from __future__ import annotations

from typing import Literal

ScoringMode = Literal["std", "half", "ppr"]

_FPTS_COLUMN_FOR_SCORING: dict[str, str] = {
    "std":  "fpts_std",
    "half": "fpts_half",
    "ppr":  "fpts_ppr",
}


def flex_topN_by_draft_round(
    round_: int,
    n: int = 10,
    scoring: ScoringMode = "ppr",
) -> tuple[str, list]:
    """Top-N FLEX (RB/WR/TE) player-seasons drafted in a given round.

    Returns rows of (name, team, season, fpts, draft_round, draft_year,
    draft_overall_pick) ordered by fpts descending. The fpts column
    used corresponds to the ``scoring`` mode.
    """
    if scoring not in _FPTS_COLUMN_FOR_SCORING:
        raise ValueError(
            f"unknown scoring mode {scoring!r}; expected one of "
            f"{list(_FPTS_COLUMN_FOR_SCORING)}"
        )
    fpts_col = _FPTS_COLUMN_FOR_SCORING[scoring]
    sql = f"""
        SELECT name,
               team,
               season,
               {fpts_col} AS fpts,
               draft_round,
               draft_year,
               draft_overall_pick
        FROM   v_flex_seasons
        WHERE  draft_round = ?
          AND  {fpts_col} IS NOT NULL
        ORDER BY {fpts_col} DESC
        LIMIT  ?
    """
    return sql, [round_, n]
