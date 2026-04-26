"""Parse PFR's /years/YYYY/draft.htm draft-results table."""

from __future__ import annotations

from ffpts.parsers._base import extract_table_rows


def _coerce_int(s: str | None) -> int | None:
    if s is None or s == "":
        return None
    try:
        return int(s.replace(",", ""))
    except ValueError:
        return None


def _strip_hof_suffix(name: str | None) -> str | None:
    """PFR appends 'HOF' to Hall of Famers' names in the draft table.
    The clean name is everything before that suffix."""
    if not name:
        return name
    if name.endswith("HOF"):
        return name[:-3]
    return name


def parse_draft(html: str, season: int) -> list[dict]:
    """One dict per draft pick with our draft_picks schema columns:
    player_id, year, round, overall_pick, team, plus name and position
    that the pipeline uses for the players-table upsert.

    Drops picks without a player slug (e.g. picks where the player
    never registered in PFR's database).
    """
    rows: list[dict] = []
    for raw in extract_table_rows(html, "drafts"):
        slug = raw.get("_player_slug")
        if not slug:
            continue
        round_ = _coerce_int(raw.get("draft_round"))
        pick = _coerce_int(raw.get("draft_pick"))
        if round_ is None or pick is None:
            continue
        rows.append(
            {
                "player_id":    f"pfr:{slug}",
                "name":         _strip_hof_suffix(raw.get("player")),
                "year":         season,
                "round":        round_,
                "overall_pick": pick,
                "team":         raw.get("team"),
                "position":     raw.get("pos"),
            }
        )
    return rows
