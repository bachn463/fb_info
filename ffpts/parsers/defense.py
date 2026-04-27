"""Parse PFR's /years/YYYY/defense.htm season leaders table."""

from __future__ import annotations

from ffpts.parsers._base import extract_table_rows


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


# PFR data-stat -> our column. PFR uses bare names (sacks, tackles_solo,
# fumbles_forced); we add the def_ prefix to disambiguate from offense.
_INT_FIELDS: dict[str, str] = {
    "age":              "age",
    "games":            "games",
    "games_started":    "games_started",
    "def_int":          "def_int",
    "def_int_yds":      "def_int_yds",
    "def_int_td":       "def_int_td",
    "pass_defended":    "def_pass_def",
    "fumbles_forced":   "def_fumbles_forced",
    "fumbles_rec":      "def_fumbles_rec",
    "fumbles_rec_yds":  "def_fumbles_rec_yds",
    "fumbles_rec_td":   "def_fumbles_rec_td",
    "tackles_combined": "def_tackles_combined",
    "tackles_solo":     "def_tackles_solo",
    "tackles_assists":  "def_tackles_assist",
    "safety_md":        "def_safeties",
}

_FLOAT_FIELDS: dict[str, str] = {
    "sacks": "def_sacks",
}


def parse_defense(html: str, season: int) -> list[dict]:
    """One dict per defender. ``pass_defended`` is missing pre-1985 in
    PFR; the value comes back as None, which is the right answer for
    seasons where PFR didn't track it.
    """
    rows: list[dict] = []
    for raw in extract_table_rows(html, "defense"):
        slug = raw.get("_player_slug")
        if not slug:
            continue
        out: dict = {
            "player_id": f"pfr:{slug}",
            "name":      raw.get("name_display"),
            "season":    season,
            "team":      raw.get("team_name_abbr"),
            "team_slug": raw.get("_team_slug"),
            "position":  raw.get("pos"),
            "awards":    raw.get("awards"),
        }
        for src, dst in _INT_FIELDS.items():
            out[dst] = _coerce_int(raw.get(src))
        for src, dst in _FLOAT_FIELDS.items():
            out[dst] = _coerce_float(raw.get(src))
        rows.append(out)
    return rows
