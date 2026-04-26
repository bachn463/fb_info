# FF-pts

Local, queryable database of NFL player and team season stats — every
stat category (passing, rushing, receiving, defense, kicking, punting,
returns) plus draft and team-season metadata (conference, division,
franchise) — with **Std / Half-PPR / PPR fantasy points** computed
in-pipeline for skill-position players (QB / RB / WR / TE).

Storage is [DuckDB][duckdb]; query surface is raw SQL plus a small
library of named helpers. Two data sources, joined transparently:

- **[nflverse][nflverse]** via [`nflreadpy`][nflreadpy] for
  **1999–present** — clean parquet files, no scraping.
- **[Pro Football Reference][pfr]** for **1970–1998** — HTML scraped
  via a one-time browser-cookie session (see "Pre-1999 PFR backfill"
  below). Rows produced by both sources land in the same tables.

[duckdb]:    https://duckdb.org/
[nflverse]:  https://github.com/nflverse
[nflreadpy]: https://pypi.org/project/nflreadpy/
[pfr]:       https://www.pro-football-reference.com/

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
ffpts ask pos-top  --position FLEX --draft-rounds undrafted
ffpts ask pos-top  --position QB   --draft-rounds "4,5,undrafted"
#                                                  ^ rounds + undrafted compose

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

## Pre-1999 PFR backfill

For seasons before 1999, `ffpts build` pulls from PFR HTML — but PFR
sits behind a Cloudflare Turnstile challenge that 403s every default
HTTP client. The workaround is to reuse a real browser session via a
copied `cf_clearance` cookie. **One-time setup** before your first
pre-1999 build:

1. Open <https://www.pro-football-reference.com/years/1985/passing.htm>
   in your normal browser. Complete the Cloudflare challenge if shown.
2. Open DevTools (don't toggle the device emulator). In the Console:
   ```js
   navigator.userAgent
   ```
   Copy that string verbatim — Cloudflare validates it against the
   cookie.
3. DevTools → Application → Cookies → `pro-football-reference.com` →
   copy the value of `cf_clearance`.
4. Save both into `data/pfr_session.json` (gitignored):
   ```json
   {
     "cf_clearance": "<paste cookie value>",
     "user_agent": "<paste UA verbatim>"
   }
   ```
5. Run the build. Pre-1999 years are routed through PFR automatically:
   ```bash
   ffpts build --start 1970 --end 2025
   ```
   The scraper sleeps ≥ 5 s between live PFR fetches to be polite. A
   first-time pre-1999 backfill is ~25 sample years × 7 page types ≈
   3 minutes plus the throttle. Cache hits on subsequent runs.

Cloudflare rotates `cf_clearance` periodically (typically when your IP
shifts, or every ~30 days). When that happens the build raises
`CloudflareSessionExpired` with refresh instructions; redo steps 1–4
and re-run.

PFR's bulk-extraction policy is gray — Stathead is the sanctioned
commercial alternative. Use polite throttle, identify yourself, and
don't redistribute scraped HTML.

Pre-1999 player IDs use a `pfr:<slug>` namespace (e.g.
`pfr:McCaCh01`). nflverse player IDs use the GSIS format
(`00-0033280`). They live alongside each other in the `players`
table; same player crossing the 1999 boundary will appear as two
distinct entries unless a manual crosswalk fixer is added later.

## Why nflverse, not Pro Football Reference (originally)

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

- **Coverage now spans 1970–present** (with the pre-1999 PFR session
  set up — see above). Without the session config, the build still
  works but pre-1999 years are skipped.
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
- **Division/conference history is hand-encoded** for 1970+ in
  [ffpts/ingest.py](ffpts/ingest.py) (16 era bands cover every NFL
  realignment). Cross-check against the NFL's published timeline if
  you change the table.

- **Some PFR codes mean different franchises in different eras.**
  STL = Cardinals 1970–1987 / Rams 1995–2015; BAL = Colts 1970–1983 /
  Ravens 1996+; HOU = Oilers 1970–1996 / Texans 2002+. The era table
  embeds the franchise per band so queries by `franchise` resolve
  correctly across the boundary.

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
├── scraper.py     HTTP + cache + throttle + cf_clearance session
├── parsers/       PFR HTML -> typed rows (one module per page type)
│   ├── passing.py rushing.py receiving.py defense.py
│   ├── kicking.py returns.py draft.py standings.py
│   └── _base.py   comment-stripping + table-extract helpers
├── ingest.py      nflverse -> our schema (player seasons, draft, team_seasons)
├── ingest_pfr.py  PFR -> our schema (pre-1999 backfill orchestrator)
├── pipeline.py    build(seasons, ...) — routes per-year by source boundary
├── queries.py     named helpers; player-season default
└── cli.py         `ffpts build | query | ask`
```
