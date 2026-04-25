# FF-pts

Local, queryable database of NFL player and team season stats — every
stat category (passing, rushing, receiving, defense, kicking, punting,
returns) plus draft and team-season metadata (conference, division,
franchise) — with **Std / Half-PPR / PPR fantasy points** computed
in-pipeline for skill-position players (QB / RB / WR / TE).

Storage is [DuckDB][duckdb]; query surface is raw SQL plus a small
library of named helpers. Data source is [nflverse][nflverse] via the
[`nflreadpy`][nflreadpy] Python package — covers **1999–present**.

[duckdb]:    https://duckdb.org/
[nflverse]:  https://github.com/nflverse
[nflreadpy]: https://pypi.org/project/nflreadpy/

## Quickstart

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"

# Build a DB covering 1999-2025 (loads from nflverse, writes data/ff.duckdb).
ffpts build --start 1999 --end 2025

# Run a named helper:
ffpts ask flex-top --round 3 --n 10 --scoring ppr
ffpts ask div-int  --division "NFC North" --start 1999 --end 2005 --mode historical
ffpts ask pos-top  --position QB --rank-by pass_yds --draft-rounds 4,5
ffpts ask pos-top  --position WR --rank-by rec_yds  --team SF
ffpts ask pos-top  --position ALL --rank-by def_int --division "NFC North"
ffpts ask pos-top  --position ALL --first-name-contains z --rank-by fpts_ppr
ffpts ask pos-top  --position ALL --last-name-contains  z --rank-by fpts_ppr
ffpts ask pos-top  --position QB --rank-by pass_yds --unique --n 10
#                                                    ^ best single season per player

# Or any raw SQL:
ffpts query "SELECT name, fpts_ppr FROM v_player_season_full
             WHERE position = 'WR' AND season = 2023
             ORDER BY fpts_ppr DESC LIMIT 5"
```

`ffpts build` is idempotent — re-running for the same year range
replaces those rows in one transaction per season.

## Default query unit

**Stat queries default to player-seasons.** "Who has the most X"
returns the top *(player, season, team)* rows — not career totals,
not team aggregates. Same player appears multiple times if multiple
of their seasons qualify. Career and team-aggregate rollups are
explicitly opt-in helpers (none yet); they are never the default.

## Schema

One wide fact table, three reference tables, two views:

```
players                (player_id, name, first_season, last_season)
draft_picks            (player_id, year, round, overall_pick, team)
team_seasons           (team, season, conference, division, franchise, w, l, t)
player_season_stats    one row per (player_id, season, team), every stat as a column
v_player_season_full   stats LEFT JOIN draft + team_seasons (the everyday view)
v_flex_seasons         v_player_season_full filtered to RB/WR/TE
```

DuckDB columns are nullable, columnar storage makes sparse rows cheap
(a defender's row has NULL passing/receiving columns, a QB's has NULL
defense columns).

## Scoring formula

Standard fantasy formula used by every major site for QB/RB/WR/TE.
Computed by [`ffpts.scoring`](ffpts/scoring.py) and verified inline
in the pipeline against a CMC 2023 fixture (PPR ≈ 393, matches the
public number).

| Event              | Std   | Half  | PPR   |
|--------------------|-------|-------|-------|
| Passing yards      | 1/25  | 1/25  | 1/25  |
| Passing TD         | 4     | 4     | 4     |
| Interception       | −2    | −2    | −2    |
| Rushing yards      | 1/10  | 1/10  | 1/10  |
| Rushing TD         | 6     | 6     | 6     |
| Receiving yards    | 1/10  | 1/10  | 1/10  |
| Receiving TD       | 6     | 6     | 6     |
| Reception          | 0     | 0.5   | 1     |
| Fumble lost        | −2    | −2    | −2    |
| 2-pt conversion    | 2     | 2     | 2     |

Kickers and team-defense fantasy formulas are out of scope.

## Why nflverse, not Pro Football Reference

The original plan was to scrape Pro Football Reference for
1970–present coverage. PFR sits behind Cloudflare's Turnstile in its
strictest mode, which 403s every programmatic client we tried —
`httpx`, `curl`, `cloudscraper`, `curl_cffi` with Chrome TLS
impersonation, and Playwright with stealth (headless, anti-automation
flags, 15s auto-pass wait). Bypass requires either paid CAPTCHA-
solving services or actual human interaction.

We pivoted to nflverse, which hosts cleanly-typed parquet files on
GitHub Releases (no Cloudflare). Coverage is **1999–present** for
weekly/seasonal stats, draft picks, rosters, and team metadata.

The HTTP scraper at [ffpts/scraper.py](ffpts/scraper.py) is preserved
as **dormant infrastructure** — fully tested, ready to use if a future
PFR backfill (1970–1998) becomes feasible. Nothing in the active
pipeline imports it.

## Known caveats

- **Coverage starts in 1999.** Pre-1999 player-seasons are not
  loaded. The two motivating queries adapt:
    - "FLEX in R3 top 10 PPR" — fully answerable.
    - "NFC North INTs 1990–2005" — runs as 1999–2005, with the
      ``historical`` division mode mapping 1999–2001 to NFC Central.
- **Multi-team seasons collapse.** nflverse seasonal data uses
  ``recent_team`` (the season-ending team) for traded players;
  per-team splits are not stored. PFR's "2TM"/"3TM" summary rows do
  not exist here.
- **Some columns NULL by design.** ``pass_long``, ``rush_long``,
  ``rec_long``, ``games_started``, ``pass_rating``, ``punts``,
  ``punt_yds``, ``punt_long``, ``def_int_td``, ``def_fumbles_rec_td``,
  ``kr_td``, ``pr_td`` aren't in nflverse's seasonal stats table.
  Some are derivable from play-by-play (``load_pbp``); others
  (punting) live in separate nflverse datasets and could be loaded
  later.
- **Division/conference history is hand-encoded** for 1999+ in
  [ffpts/ingest.py](ffpts/ingest.py). Cross-check against the NFL's
  published realignment timeline if you change the era table.

## Development

```bash
.venv/bin/pytest -q             # 97 tests, all unit/integration, no network
.venv/bin/pytest tests/test_pipeline.py -q     # the end-to-end test
```

Each commit on `main` is logical, atomic, and ships with passing
tests. Tests for a module live in the same commit as the module.

```
ffpts/
├── scoring.py     std/half/ppr formula on a frozen StatLine dataclass
├── normalize.py   franchise slug map, NFL team-code map, position aliases
├── db.py          DuckDB schema, connection, views
├── scraper.py     [DORMANT] HTTP + cache + throttle for a future PFR pull
├── ingest.py      nflverse -> our schema (player seasons, draft, team_seasons)
├── pipeline.py    build(seasons, ...) — idempotent, per-season transactions
├── queries.py     named helpers; player-season default
└── cli.py         `ffpts build | query | ask`
```
