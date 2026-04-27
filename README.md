# FF-pts

Local, queryable database of NFL player and team season stats — every
stat category (passing, rushing, receiving, defense, kicking, punting,
returns) plus draft, team-season metadata (conference, division,
franchise), and **per-season awards** (MVP, OPOY, DPOY, OROY, DROY,
CPOY, WPMOY, Pro Bowl, AP First/Second-Team All-Pro). **Std / Half-PPR
/ PPR fantasy points** computed in-pipeline for skill-position players
(QB / RB / WR / TE).

Storage is [DuckDB][duckdb]; query surface is raw SQL plus a small
library of named helpers — and an interactive trivia game on top of
the same query layer.

[duckdb]:    https://duckdb.org/
[pfr]:       https://www.pro-football-reference.com/

**Data source: [Pro Football Reference][pfr] for 1970–present.** All
years pulled via a one-time browser-cookie session — see "PFR session
setup" below. Earlier hybrid (nflverse 1999+ + PFR 1970–1998) was
unified to all-PFR for consistency: one player_id namespace
(`pfr:<slug>`), one stat schema, awards available across the full
range.

## Quickstart

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"

# One-time: set up data/pfr_session.json (see "PFR session setup").

# Build the DB (1970–2025 ≈ 56 years × 9 PFR pages at the polite 5s
# throttle = ~45 minutes the first time, cached forever after).
ffpts build --start 1970 --end 2025

# Named query helpers:
ffpts ask flex-top --round 3 --n 10 --scoring ppr
ffpts ask div-int  --division "NFC North" --start 1990 --end 2005 --mode historical
ffpts ask pos-top  --position QB --rank-by pass_yds --draft-rounds 4,5
ffpts ask pos-top  --position WR --rank-by rec_yds  --team SF
ffpts ask pos-top  --position ALL --rank-by def_int --conference NFC
ffpts ask pos-top  --position ALL --first-name-contains z --rank-by fpts_ppr
ffpts ask pos-top  --position QB --rank-by pass_yds --unique --n 10
ffpts ask pos-top  --position FLEX --draft-rounds undrafted
ffpts ask pos-top  --position QB   --draft-rounds "4,5,undrafted"

# Awards filters (NEW):
ffpts ask pos-top --position FLEX --has-award OROY --rookie-only
ffpts ask pos-top --position QB   --has-award MVP --start 2010 --end 2024
ffpts ask pos-top --position ALL  --has-award AP_FIRST --rank-by def_sacks

# Stat threshold filters (NEW):
ffpts ask pos-top --position RB --rank-by rush_att --n 5 \
    --team PIT --start 1990 --end 2020 --max-stat rush_yds=999
#                       ^ "high-volume / low-yardage Steelers RBs"

# Display flags (NEW, opt-in; default invocation columns unchanged):
ffpts ask pos-top --position QB --rank-by fpts_ppr --has-award MVP \
    --show-awards --show-context

# Trivia game (NEW):
ffpts trivia play --rank-by rush_yds --n 5 \
    --position RB --team PIT --start 1990 --end 2020 --max-stat rush_yds=999

# Or any raw SQL:
ffpts query "SELECT name, fpts_ppr FROM v_player_season_full
             WHERE position = 'WR' AND season = 2023
             ORDER BY fpts_ppr DESC LIMIT 5"
ffpts query "SELECT name, season FROM v_award_winners
             WHERE  award_type = 'MVP' AND vote_finish = 1
               AND  season BETWEEN 1980 AND 1990
             ORDER BY season"
```

`ffpts build` is idempotent — re-running for the same year range
replaces those rows in one transaction per season.

## Default query unit

**Stat queries default to player-seasons.** "Who has the most X"
returns the top *(player, season, team)* rows — not career totals,
not team aggregates. Same player appears multiple times if multiple
of their seasons qualify. Use `--unique` to collapse to one row per
player (their best season).

## Trivia game

`ffpts trivia play` accepts the **same filter flags as
`ffpts ask pos-top`**, picks the answer set via `pos_topN` with
`--unique` defaulting to True, then runs an interactive REPL:

```
$ ffpts trivia play --rank-by rush_yds --n 5 --position RB \
      --start 1985 --end 1985

Top 5 player-seasons by rush_yds. Type a name (substring OK).
Commands: `give up`, `hint`, `quit`.
>>> payton
  Correct! #3 Walter Payton, 1985 (CHI, rush_yds=1551).
>>> riggs
  Correct! #2 Gerald Riggs, 1985 (ATL, rush_yds=1719).
>>> hint
  Hint: #1 played for RAI in 1985 (RB), season #4 of their career.
>>> marcus allen
  Correct! #1 Marcus Allen, 1985 (RAI, rush_yds=1759).
>>> give up
  Remaining:
    #4: James Wilder (TAM 1985, rush_yds=1300)
    #5: Curt Warner (SEA 1985, rush_yds=1094)

Final score: 3 / 5 in 4 guesses.
```

Special inputs: `give up` reveals remaining + final score; `hint`
prints a clue (team / season / position / season-of-career); `quit`
exits silently. Match logic is case-insensitive substring; ambiguous
matches prompt for more characters.

## Schema

```
players                (player_id, name, first_season, last_season)
draft_picks            (player_id, year, round, overall_pick, team)
team_seasons           (team, season, conference, division, franchise, w, l, t)
player_season_stats    one row per (player_id, season, team), every stat as a column
player_awards          (player_id, season, award_type, vote_finish)  — NEW
v_player_season_full   stats LEFT JOIN draft + team_seasons (the everyday view)
v_flex_seasons         v_player_season_full filtered to RB/WR/TE
v_award_winners        player_awards JOIN players for the name        — NEW
```

Award types in `player_awards`:

| `award_type`  | What                                  | `vote_finish`        |
|---------------|---------------------------------------|----------------------|
| `MVP`         | AP NFL MVP                            | 1 = won, 2+ = placing |
| `OPOY`        | AP Offensive Player of the Year       | as above              |
| `DPOY`        | AP Defensive Player of the Year       | as above              |
| `OROY`        | AP Offensive Rookie of the Year       | as above              |
| `DROY`        | AP Defensive Rookie of the Year       | as above              |
| `CPOY`        | AP Comeback Player of the Year        | as above              |
| `WPMOY`       | Walter Payton Man of the Year         | NULL (binary)         |
| `AP_FIRST`    | AP First-Team All-Pro                 | NULL (binary)         |
| `AP_SECOND`   | AP Second-Team All-Pro                | NULL (binary)         |
| `PB`          | Pro Bowl selection                    | NULL (binary)         |

`--has-award MVP` matches `vote_finish=1` (won) for voted awards and
any entry for binary ones. Implied position for AP All-Pro / Pro Bowl
selections is whatever `player_season_stats.position` is for that
year — joined via `v_player_season_full`.

DuckDB columns are nullable, columnar storage makes sparse rows cheap
(a defender's row has NULL passing/receiving columns, a QB's has NULL
defense columns).

## Scoring formula

Standard fantasy formula used by every major site for QB/RB/WR/TE.
Computed by [`ffpts.scoring`](ffpts/scoring.py).

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

## PFR session setup

PFR sits behind a Cloudflare Turnstile challenge that 403s every
default HTTP client (httpx, curl, cloudscraper, curl_cffi, Playwright
with stealth — all blocked). The workaround is to reuse a real browser
session via a copied `cf_clearance` cookie. **One-time setup** before
your first build:

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
5. Run the build:
   ```bash
   ffpts build --start 1970 --end 2025
   ```
   The scraper sleeps ≥ 5 s between live PFR fetches to be polite. A
   full 1970–2025 backfill is ~45 minutes the first time. Cache hits
   on subsequent runs.

Cloudflare rotates `cf_clearance` periodically (typically when your IP
shifts, or every ~30 days). When that happens the build raises
`CloudflareSessionExpired` with refresh instructions; redo steps 1–4
and re-run.

PFR's bulk-extraction policy is gray — Stathead is the sanctioned
commercial alternative. Use polite throttle, identify yourself, and
don't redistribute scraped HTML.

## Known caveats

- **Coverage spans 1970–present** with the PFR session set up. Without
  the session config, builds will fail before fetching anything.
- **Multi-team seasons** appear as one row per (player, season, team).
  PFR sometimes also has a `2TM`/`3TM` summary row that's typically
  filtered out by the parser (no player slug on the summary line).
- **Some columns NULL by design** for older seasons: defensive sacks
  weren't official until 1982 (NULL or unofficial estimates earlier);
  fumbles_lost wasn't tracked separately until 1994; defensive tackles
  are unofficial throughout PFR's data.
- **Division/conference history is hand-encoded** in
  [ffpts/ingest.py](ffpts/ingest.py) (16 era bands cover every NFL
  realignment 1970→present).
- **Some PFR team codes mean different franchises in different eras.**
  STL = Cardinals 1970–1987 / Rams 1995–2015; BAL = Colts 1970–1983 /
  Ravens 1996+; HOU = Oilers 1970–1996 / Texans 2002+. The era table
  embeds the franchise per band so queries by `franchise` resolve
  correctly across the boundary.
- **Hand-encoded supplemental + pre-merger draft picks** for players
  that PFR's regular draft pages don't cover — Reggie White, Bernie
  Kosar, Cris Carter, Josh Gordon (supplemental), OJ Simpson, Joe
  Greene (pre-1970 main draft). See
  [ffpts/supplemental_drafts.py](ffpts/supplemental_drafts.py).

## Development

```bash
.venv/bin/pytest -q              # ~340 tests, all network-free
.venv/bin/pytest tests/test_pipeline.py -q     # end-to-end pipeline
.venv/bin/pytest tests/test_cli_trivia.py -q   # trivia REPL
```

Each commit on `main` is logical, atomic, and ships with passing
tests. Tests for a module live in the same commit as the module.

```
ffpts/
├── scoring.py            std/half/ppr formula on a frozen StatLine
├── normalize.py          franchise slug map, NFL team-code map, position aliases
├── db.py                 DuckDB schema (incl. player_awards), connection, views
├── scraper.py            HTTP + cache + throttle + cf_clearance session
├── parsers/              PFR HTML -> typed rows (one module per page type)
│   ├── passing.py rushing.py receiving.py defense.py
│   ├── kicking.py returns.py draft.py standings.py
│   ├── awards_string.py  inline awards-cell parser ('PB,AP MVP-1')
│   └── _base.py          comment-stripping + table-extract helpers
├── ingest.py             [unused] legacy nflverse loader (kept in tree)
├── ingest_pfr.py         PFR HTML -> player-season + draft + standings rows
├── ingest_awards.py      derive player_awards from the merged stats DataFrame
├── pipeline.py           build(seasons, ...) — all years through PFR
├── supplemental_drafts.py hand-encoded supp + pre-merger draft picks
├── queries.py            named helpers + filter builder; player-season default
└── cli.py                `ffpts build | query | ask | trivia`
```
