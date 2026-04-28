# FF-pts

Local, queryable database of NFL player and team season stats — every
stat category (passing, rushing, receiving, defense, kicking, punting,
returns) plus draft, team-season metadata (conference, division,
franchise, W/L), per-player **college** (with hand-curated overrides
for transfers / UDFAs / supplemental picks), and **per-season awards**
(MVP, OPOY, DPOY, OROY, DROY, CPOY, WPMOY, Pro Bowl, AP First/Second-
Team All-Pro). **Std / Half-PPR / PPR fantasy points** are computed
in-pipeline for skill-position players (QB / RB / WR / TE).

Storage is [DuckDB][duckdb]; query surface is raw SQL plus a small
library of named helpers (player-season top-N, career totals, awards
listings, two-player comparisons, single-season records) — and an
interactive trivia game on top of the same query layer.

[duckdb]:    https://duckdb.org/
[pfr]:       https://www.pro-football-reference.com/

**Data source: [Pro Football Reference][pfr] for 1970–present.** All
years pulled via a one-time browser-cookie session — see "PFR session
setup" below. One uniform source means one player_id namespace
(`pfr:<slug>`), one stat schema, awards available across the full
range.

## Quickstart

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"

# One-time: set up data/pfr_session.json (see "PFR session setup").

# Build the DB (1970–2025 ≈ 56 years × 9 PFR pages at the polite 5s
# throttle = ~45 minutes the first time, cached forever after).
fb_info build --start 1970 --end 2025

# Player-season top-N (the everyday helper):
fb_info ask pos-top --rank-by fpts_ppr --n 10
fb_info ask pos-top --position QB --rank-by pass_yds --draft-rounds 4,5
fb_info ask pos-top --position WR --rank-by rec_yds  --team SF
fb_info ask pos-top --position ALL --rank-by def_int --conference NFC
fb_info ask pos-top --position FLEX --first-name-contains z --rank-by fpts_ppr
fb_info ask pos-top --position QB --rank-by pass_yds --unique --n 10
fb_info ask pos-top --position FLEX --draft-rounds undrafted

# Awards filters — the season they won, or any time in their career:
fb_info ask pos-top --position FLEX --has-award OROY --rookie-only
fb_info ask pos-top --position QB   --ever-won  MVP --start 2010 --end 2024
fb_info ask pos-top --position ALL  --has-award AP_FIRST --rank-by def_sacks

# Draft / college / career-stat threshold filters:
fb_info ask pos-top --position RB --rank-by rush_att --n 5 \
    --team PIT --start 1990 --end 2020 --max-stat rush_yds=999
#                       ^ "high-volume / low-yardage Steelers RBs"
fb_info ask pos-top --rank-by rush_yds --college "Alabama" --n 10
fb_info ask pos-top --position QB --drafted-by NE --rank-by pass_yds
fb_info ask pos-top --position QB --max-career-stat pass_yds=20000 --n 10

# Display flags — opt-in extra columns, default invocation unchanged:
fb_info ask pos-top --position QB --rank-by fpts_ppr --has-award MVP \
    --show-awards --show-context

# Career totals (different unit-of-analysis from pos-top):
fb_info ask career --rank-by rush_yds --position RB --n 10
fb_info ask career --rank-by pass_yds --ever-won MVP --min-seasons 5
fb_info ask career --rank-by rec_yds  --college "Alabama" --draft-rounds 1

# Single-season all-time records (curated dashboard):
fb_info ask records --category offense
fb_info ask records --category defense --n 3

# Award counts (winners only) with composing filters:
fb_info ask awards-top --award AP_FIRST --position SAFETY \
    --max-career-stat def_int=30 --n 10
#                       ^ "safeties with under 30 career INTs, ranked
#                          by AP First-Team All-Pro count"
fb_info ask awards-top --award MVP --n 10

# Two-player head-to-head:
fb_info ask compare "Tom Brady" "Peyton Manning"
fb_info ask compare --p1-id pfr:MariDa00 --p2-id pfr:MahoPa00  # disambiguate

# List award winners by type / season:
fb_info ask awards --award MVP                  # all MVP winners
fb_info ask awards --award PB --season 2023     # 2023 Pro Bowlers

# Two simpler legacy helpers:
fb_info ask flex-top --round 3 --n 10 --scoring ppr
fb_info ask div-int  --division "NFC North" --start 1990 --end 2005

# Trivia — three modes:
fb_info trivia play --rank-by rush_yds --n 5 \
    --position RB --team PIT --start 1990 --end 2020
fb_info trivia daily                       # same game for everyone today
fb_info trivia random                      # different every call
fb_info trivia random --seed 42            # reproducible
fb_info trivia random --start 1970 --end 1990 --team PIT
fb_info trivia random --mode career --rank-by rush_yds

# Or any raw SQL:
fb_info query "SELECT name, college, fpts_ppr FROM v_player_season_full
             WHERE position = 'WR' AND season = 2023
             ORDER BY fpts_ppr DESC LIMIT 5"
fb_info query "SELECT name, season FROM v_award_winners
             WHERE  award_type = 'MVP' AND vote_finish = 1
               AND  season BETWEEN 1980 AND 1990
             ORDER BY season"
```

`fb_info build` is idempotent — re-running for the same year range
replaces those rows in one transaction per season. The `apply_schema`
step also runs additive ALTER TABLEs for any column added since the DB
was first populated, so existing DBs migrate forward without a rebuild.

## Default query unit

**Stat queries default to player-seasons.** "Who has the most X"
returns the top *(player, season, team)* rows — not career totals,
not team aggregates. Same player appears multiple times if multiple
of their seasons qualify. Use `--unique` to collapse to one row per
player (their best season).

For career totals, use `ask career` — it groups by player and SUMs
across qualifying seasons. Ratio stats (`pass_cmp_pct`, `catch_rate`)
recompute correctly as `SUM(num) / NULLIF(SUM(den), 0)` rather than
naively averaging percentages.

## Position aliases

Caller-friendly names that expand to a set of PFR position labels:

| Alias    | Expands to                                       |
|----------|--------------------------------------------------|
| `ALL`    | no position filter (default)                     |
| `FLEX`   | `RB`, `WR`, `TE`                                 |
| `SAFETY` | `S`, `SS`, `FS`                                  |
| `DB`     | `CB`, `S`, `SS`, `FS`, `DB`, `RCB`, `LCB`        |
| `LB`     | `LB`, `OLB`, `MLB`, `ILB`, `RLB`, `LLB`          |
| `DL`     | `DE`, `DT`, `NT`, `LDE`, `RDE`, `LDT`, `RDT`     |

Or pass any single PFR label directly — `--position QB`, `--position K`,
`--position OT`, etc.

## Trivia

`fb_info trivia play` accepts the same filter flags as
`fb_info ask pos-top` (or `ask career` when `--mode career`), picks
the answer set, then runs an interactive REPL:

```
$ fb_info trivia play --rank-by rush_yds --n 5 --position RB \
      --start 1985 --end 1985

Top 5 RB player-seasons by rush_yds (1985-1985)
Type a name (substring OK). Commands: `give up`, `hint`, `quit`.
>>> payton
  Correct! #3 Walter Payton, 1985 (CHI, rush_yds=1551).
>>> riggs
  Correct! #2 Gerald Riggs, 1985 (ATL, rush_yds=1719).
>>> hint
  Hint #1 for #1: team RAI
>>> hint
  Hint #1 for #4: team NYG
>>> hint
  Hint #1 for #5: team NYJ
>>> hint
  Hint #2 for #1: team RAI, year 1985
>>> marcus allen
  Correct! #1 Marcus Allen, 1985 (RAI, rush_yds=1759).
>>> give up

Final ranked list — Top 5 RB player-seasons by rush_yds (1985-1985):
  ✓ #1: Marcus Allen (RAI 1985, rush_yds=1759)
  ✓ #2: Gerald Riggs (ATL 1985, rush_yds=1719)
  ✓ #3: Walter Payton (CHI 1985, rush_yds=1551)
  ✗ #4: Joe Morris (NYG 1985, rush_yds=1336)
  ✗ #5: Freeman McNeil (NYJ 1985, rush_yds=1331)

Final score: 3 / 5 in 5 guesses.
```

Behavior worth knowing:

- **Title at start + repeated on the final list.** Built from the
  active filters, so you always know what query you're guessing
  against.
- **Progressive hints.** `hint` cycles through unfound answers; each
  re-hit on the same player reveals one more layer (team → year →
  position → career-year → rank-by value → draft round → last-name
  initial). The first hint barely helps; the seventh nearly gives it
  away.
- **Match logic.** Case-insensitive substring on the player's display
  name. With `--no-unique`, a single correct guess credits *every* slot
  belonging to that player. Genuinely ambiguous matches (across
  multiple distinct players) print "matches N answers" without
  revealing the candidate names — that would spoil future guesses.
- **Always exit with the answers.** Every exit path (`give up`, `quit`,
  finishing) prints the full ranked list with ✓ / ✗ markers per row.

### `trivia daily` and `trivia random`

Both sample a random template from a broad distribution of stat /
position / year-range / team-or-division-or-conference / award-filter
/ rookie-only / draft-round / etc. combinations. `daily` seeds the RNG
from today's date so everyone playing the same day gets the same game;
`random` re-rolls each call (or pin via `--seed N` for reproducibility).
Both also occasionally roll **career mode** (~25% by default) — ranking
players by career-totals instead of single seasons. Output:

```
Top 10 RB career-totals by rush_yds (1990-2024), who ever won MVP
```

`trivia random` accepts the same filter set as `trivia play` plus a
`--mode {season,career}` flag — anything you pass becomes a hard pin
and the rest stays random:

```bash
fb_info trivia random --start 1970 --end 1990                # pin years
fb_info trivia random --team PIT                             # pin team
fb_info trivia random --rank-by def_int --position SAFETY    # pin both
fb_info trivia random --mode career --college "Alabama"      # career + college
```

If you pin a single-season-only filter (team / division / conference /
has-award / rookie-only / min-stat / max-stat / draft-start/end / 
tiebreak), the generator forces season mode — career queries can't
honor those filters and silently dropping a user pin would be wrong.
Explicit `--mode career` overrides this auto-fallback if you really
mean it (the pin gets dropped).

**Quality gate.** The random retry loop rejects any candidate template
whose answer set has fewer than N rows or includes any rank-value of 0
(meaning the filter pool exceeds the eligible-answer pool — there
shouldn't be a 0-rush-yds player on a rushing leaderboard). Up to 25
attempts before falling back to a minimum-filter template.

## Schema

```
players                 (player_id, name, first_season, last_season, college)
draft_picks             (player_id, year, round, overall_pick, team, college)
team_seasons            (team, season, conference, division, franchise, w, l, t)
player_season_stats     one row per (player_id, season, team), every stat as a column
player_awards           (player_id, season, award_type, vote_finish)
v_player_season_full    stats LEFT JOIN draft + team_seasons; adds:
                          - pass_cmp_pct = pass_cmp / NULLIF(pass_att, 0)
                          - catch_rate   = rec      / NULLIF(targets, 0)
                          - college (sourced from players.college)
v_flex_seasons          v_player_season_full filtered to RB / WR / TE
v_award_winners         player_awards JOIN players for the name
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

`--has-award MVP` matches the seasons the player won outright
(`vote_finish = 1` for vote-ranked awards; any row for the binary ones).
`--ever-won MVP` matches every season of any player who won MVP at any
point in their career. Both flags are repeatable: pass `--has-award MVP
--has-award OROY` for "won MVP OR OROY".

`college` is canonicalized on `players.college`. Two sources:
`draft_picks.college` (auto-scraped from PFR's draft pages — the
drafted-from school) is copied forward, then a curated
`KNOWN_COLLEGE_OVERRIDES` list overlays full college histories for
transfers (Jalen Hurts: Alabama → Oklahoma), supplemental-draft picks
(Reggie White: Tennessee), and notable UDFAs (Cliff Harris: Ouachita
Baptist). The override list lives in
[ffpts/supplemental_drafts.py](ffpts/supplemental_drafts.py); add
`CollegeOverride("Name", ("School A", "School B"))` entries to extend.

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
   fb_info build --start 1970 --end 2025
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
  `targets` weren't tracked separately until 1992 (so `catch_rate`
  before then can exceed 1.0 due to data quirks); `fumbles_lost`
  wasn't tracked separately until 1994; defensive tackles are
  unofficial throughout PFR's data.
- **Division/conference history is hand-encoded** in
  [ffpts/ingest.py](ffpts/ingest.py) (16 era bands cover every NFL
  realignment 1970→present).
- **Some PFR team codes mean different franchises in different eras.**
  STL = Cardinals 1970–1987 / Rams 1995–2015; BAL = Colts 1970–1983 /
  Ravens 1996+; HOU = Oilers 1970–1996 / Texans 2002+. The era table
  embeds the franchise per band so queries by `franchise` resolve
  correctly across the boundary.
- **Hand-encoded supplemental + pre-merger draft picks** for players
  PFR's regular draft pages don't cover — Reggie White, Bernie Kosar,
  Cris Carter, Josh Gordon (supplemental), OJ Simpson, Joe Greene
  (pre-1970 main draft). See
  [ffpts/supplemental_drafts.py](ffpts/supplemental_drafts.py).
- **College coverage is scrape + curated overrides.** PFR's draft page
  exposes the *drafted-from* school only, so transfers (e.g. Jalen
  Hurts → drafted from Oklahoma, but also attended Alabama) and
  supplemental / UDFA players need overrides. The curated list covers
  ~25 popular post-2010 transfers and HOF UDFA / supp-pick fills;
  extend by adding `CollegeOverride` entries.

## Development

```bash
.venv/bin/pytest -q              # ~430 tests, all network-free
.venv/bin/pytest tests/test_pipeline.py -q     # end-to-end pipeline
.venv/bin/pytest tests/test_cli_trivia.py tests/test_cli_new_commands.py -q
```

Each commit on `main` is logical, atomic, and ships with passing
tests. Tests for a module live in the same commit as the module.

```
ffpts/
├── scoring.py                 std/half/ppr formula on a frozen StatLine
├── normalize.py               franchise slug map, NFL team-code map, position aliases
├── db.py                      DuckDB schema (incl. player_awards,
│                                players.college), connection, views,
│                                additive column migrations
├── scraper.py                 HTTP + cache + throttle + cf_clearance session
├── parsers/                   PFR HTML -> typed rows (one module per page type)
│   ├── passing.py rushing.py receiving.py defense.py
│   ├── kicking.py returns.py draft.py standings.py
│   ├── awards_string.py       inline awards-cell parser ('PB,AP MVP-1')
│   ├── year_summary_awards.py WPMOY parser (only on /years/YYYY/)
│   └── _base.py               comment-stripping + table-extract helpers
├── ingest.py                  era-table + nflverse loader (era table is live;
│                                nflverse loader kept for reference, not called)
├── ingest_pfr.py              PFR HTML -> player-season + draft + standings rows
├── ingest_awards.py           derive player_awards from the merged stats DataFrame
├── pipeline.py                build(seasons, ...) — all years through PFR;
│                                applies college overrides last
├── supplemental_drafts.py     hand-encoded supp + pre-merger draft picks +
│                                KNOWN_COLLEGE_OVERRIDES
├── queries.py                 named helpers (pos_topN, career_topN, awards_list,
│                                award_topN) + filter builder; player-season default
└── cli.py                     `fb_info build | query | ask | trivia`
```
