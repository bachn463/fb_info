"""Parse PFR's /years/YYYY/returns.htm season leaders table."""

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
    "age":            "age",
    "games":          "games",
    "games_started":  "games_started",
    "punt_ret":       "pr",
    "punt_ret_yds":   "pr_yds",
    "punt_ret_td":    "pr_td",
    "kick_ret":       "kr",
    "kick_ret_yds":   "kr_yds",
    "kick_ret_td":    "kr_td",
}


def parse_returns(html: str, season: int) -> list[dict]:
    rows: list[dict] = []
    for raw in extract_table_rows(html, "returns"):
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
        rows.append(out)
    return rows
