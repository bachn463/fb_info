"""Parsers for Pro Football Reference HTML pages (pre-1999 backfill path)."""

from ffpts.parsers._base import (
    extract_table_rows,
    extract_player_slug,
    extract_team_slug,
    unwrap_pfr_comments,
)
from ffpts.parsers.passing import parse_passing
from ffpts.parsers.rushing import parse_rushing

__all__ = [
    "extract_table_rows",
    "extract_player_slug",
    "extract_team_slug",
    "parse_passing",
    "parse_rushing",
    "unwrap_pfr_comments",
]
