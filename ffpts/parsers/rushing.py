"""Parse PFR's /years/YYYY/rushing.htm season leaders table."""

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
    "rush_att":      "rush_att",
    "rush_yds":      "rush_yds",
    "rush_td":       "rush_td",
    "rush_long":     "rush_long",
    "fumbles":       "fumbles",
}


def parse_rushing(html: str, season: int) -> list[dict]:
    """One dict per rusher, ready to merge into player_season_stats.

    Drops summary rows (no player slug). Numeric fields are coerced;
    PFR's ``fumbles`` count includes fumbles on rushes only on the
    rushing page (the receiving and passing pages report their own
    category-prefixed fumbles), so the merge step in ingest_pfr should
    sum across pages, not pick one.
    """
    rows: list[dict] = []
    for raw in extract_table_rows(html, "rushing"):
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
