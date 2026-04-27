"""Derive structured player_awards rows from the merged player-season
DataFrame produced by ``ingest_pfr.load_player_seasons``.

The merged DataFrame carries a raw ``awards`` column straight from
PFR's stat-page cell ("PB,AP MVP-1,AP OPoY-2"). This module splits
that into one row per (player_id, season, award_type) ready for
``INSERT BY NAME`` into the ``player_awards`` table.

Pure transformation — no I/O. Same player on multiple stat pages
(QB also showing on the rushing page) reports the same awards
string, so dedup is automatic via the table PK.
"""

from __future__ import annotations

import polars as pl

from ffpts.parsers import parse_awards_string


def derive_awards(player_seasons: pl.DataFrame) -> pl.DataFrame:
    """Take the merged player-season rows (with raw 'awards' strings
    in the ``awards`` column) and produce one row per
    (player_id, season, award_type).

    Returns a DataFrame with columns:
      - player_id  TEXT
      - season     INTEGER
      - award_type TEXT
      - vote_finish INTEGER (nullable)

    Empty input or all-None awards column produces an empty frame
    with the right schema.
    """
    schema = {
        "player_id":   pl.Utf8,
        "season":      pl.Int64,
        "award_type":  pl.Utf8,
        "vote_finish": pl.Int64,
    }
    if player_seasons.is_empty() or "awards" not in player_seasons.columns:
        return pl.DataFrame(schema=schema)

    rows: list[dict] = []
    seen: set[tuple[str, int, str]] = set()
    for r in player_seasons.iter_rows(named=True):
        raw = r.get("awards")
        if not raw:
            continue
        pid = r.get("player_id")
        season = r.get("season")
        if pid is None or season is None:
            continue
        for record in parse_awards_string(raw):
            key = (pid, int(season), record["award_type"])
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "player_id":   pid,
                    "season":      int(season),
                    "award_type":  record["award_type"],
                    "vote_finish": record["vote_finish"],
                }
            )

    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema)
