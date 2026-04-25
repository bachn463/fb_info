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
DivisionMode = Literal["historical", "franchise"]

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


def most_def_int_by_division(
    division: str,
    *,
    start: int,
    end: int,
    n: int = 25,
    division_mode: DivisionMode = "historical",
) -> tuple[str, list]:
    """Top-N player-seasons by defensive interceptions, scoped to a division.

    Two interpretations of "division", picked by ``division_mode``:

    - ``"historical"``: filter by the per-season division as it existed
      that year. "NFC North" 1999-2001 returns no rows because the NFC
      North didn't exist before 2002 — those seasons were "NFC Central".
      Use this when you want strict period accuracy.

    - ``"franchise"``: filter by the *current* franchises that make up
      that division today. "NFC North" with this mode always means
      CHI / DET / GB / MIN, regardless of what the division was called
      that year. The query resolves the franchise set by selecting all
      franchises that ever lived in the named division.

    Returns rows of (name, team, season, def_int, conference, division,
    franchise) ordered by def_int desc. Same player can appear multiple
    times for different qualifying seasons (player-season default).
    """
    if division_mode == "historical":
        where = "v.division = ?"
        params: list = [division, start, end, n]
    elif division_mode == "franchise":
        where = """v.franchise IN (
            SELECT DISTINCT franchise
            FROM   v_player_season_full
            WHERE  division = ?
        )"""
        params = [division, start, end, n]
    else:
        raise ValueError(
            f"unknown division_mode {division_mode!r}; "
            f"expected 'historical' or 'franchise'"
        )

    sql = f"""
        SELECT v.name,
               v.team,
               v.season,
               v.def_int,
               v.conference,
               v.division,
               v.franchise
        FROM   v_player_season_full v
        WHERE  {where}
          AND  v.season BETWEEN ? AND ?
          AND  v.def_int IS NOT NULL
        ORDER BY v.def_int DESC, v.season ASC
        LIMIT  ?
    """
    return sql, params
