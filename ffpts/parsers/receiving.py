"""Parse PFR's /years/YYYY/receiving.htm season leaders table."""

from __future__ import annotations

from ffpts.parsers._base import extract_table_rows


def _coerce_int(s: str | None) -> int | None:
    if s is None or s == "":
        return None
    try:
        return int(s.replace(",", ""))
    except ValueError:
        return None


_INT_FIELDS: dict[str, str] = {
    "age":           "age",
    "games":         "games",
    "games_started": "games_started",
    "targets":       "targets",
    "rec":           "rec",
    "rec_yds":       "rec_yds",
    "rec_td":        "rec_td",
    "rec_long":      "rec_long",
    "fumbles":       "fumbles",
}


def parse_receiving(html: str, season: int) -> list[dict]:
    rows: list[dict] = []
    for raw in extract_table_rows(html, "receiving"):
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
        }
        for src, dst in _INT_FIELDS.items():
            out[dst] = _coerce_int(raw.get(src))
        rows.append(out)
    return rows
