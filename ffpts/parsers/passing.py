"""Parse PFR's /years/YYYY/passing.htm season leaders table.

Returns one dict per QB (or any passer) with keys matching our
``player_season_stats`` schema. Numeric coercion happens here; rows
that lack a player slug (league-average summary rows etc.) are dropped.
"""

from __future__ import annotations

from ffpts.parsers._base import extract_table_rows

# PFR data-stat -> our schema column. Only stats we actually persist
# are listed; PFR exposes plenty more (cmp%, td%, int%, ANY/A, comebacks,
# QBR, etc.) which we ignore for now.
_PASSING_FIELDS_INT: dict[str, str] = {
    "age":              "age",
    "games":            "games",
    "games_started":    "games_started",
    "pass_cmp":         "pass_cmp",
    "pass_att":         "pass_att",
    "pass_yds":         "pass_yds",
    "pass_td":          "pass_td",
    "pass_int":         "pass_int",
    "pass_long":        "pass_long",
    "pass_sacked":      "pass_sacks_taken",
    "pass_sacked_yds":  "pass_sack_yds",
}

_PASSING_FIELDS_FLOAT: dict[str, str] = {
    "pass_rating": "pass_rating",
}


def _coerce_int(s: str | None) -> int | None:
    if s is None or s == "":
        return None
    try:
        return int(s.replace(",", ""))
    except ValueError:
        return None


def _coerce_float(s: str | None) -> float | None:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_passing(html: str, season: int) -> list[dict]:
    """Parse the passing-leaders table out of /years/YYYY/passing.htm.

    Each row is a dict ready to merge into the player_season_stats
    schema (with NULL columns for stats not on this page). Rows for
    league-average / summary lines are dropped — they have no player
    slug to join on.
    """
    rows: list[dict] = []
    for raw in extract_table_rows(html, "passing"):
        slug = raw.get("_player_slug")
        if not slug:
            continue
        out: dict = {
            "player_id": f"pfr:{slug}",
            "name": raw.get("name_display"),
            "season": season,
            "team": raw.get("team_name_abbr"),
            "team_slug": raw.get("_team_slug"),
            "position": raw.get("pos"),
            "awards":   raw.get("awards"),
        }
        for src, dst in _PASSING_FIELDS_INT.items():
            out[dst] = _coerce_int(raw.get(src))
        for src, dst in _PASSING_FIELDS_FLOAT.items():
            out[dst] = _coerce_float(raw.get(src))
        rows.append(out)
    return rows
