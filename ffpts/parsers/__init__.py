"""Parsers for Pro Football Reference HTML pages."""

from ffpts.parsers._base import (
    extract_table_rows,
    extract_player_slug,
    extract_team_slug,
    unwrap_pfr_comments,
)
from ffpts.parsers.awards_string import parse_awards_string
from ffpts.parsers.defense import parse_defense
from ffpts.parsers.draft import parse_draft
from ffpts.parsers.kicking import parse_kicking
from ffpts.parsers.passing import parse_passing
from ffpts.parsers.receiving import parse_receiving
from ffpts.parsers.returns import parse_returns
from ffpts.parsers.rushing import parse_rushing
from ffpts.parsers.standings import parse_standings

__all__ = [
    "extract_table_rows",
    "extract_player_slug",
    "extract_team_slug",
    "parse_awards_string",
    "parse_defense",
    "parse_draft",
    "parse_kicking",
    "parse_passing",
    "parse_receiving",
    "parse_returns",
    "parse_rushing",
    "parse_standings",
    "unwrap_pfr_comments",
]
