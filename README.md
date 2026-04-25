# FF-pts

Queryable NFL player & team season-stats database, scraped from
[Pro Football Reference](https://www.pro-football-reference.com/) and stored in
DuckDB. Covers all stat categories (passing, rushing, receiving, defense,
kicking, punting, returns) plus draft and team-season metadata, from 1970
onward (configurable). Standard / Half-PPR / PPR fantasy points are computed
in-pipeline for skill-position players (QB / RB / WR / TE).

> Status: scaffold. See `pyproject.toml` and the planning doc for the design.

## Default query unit

Stat queries default to **player-seasons**: one row per (player, season, team).
"Who has the most X" returns the top player-seasons, not career totals or team
aggregates. The same player can appear multiple times for different qualifying
years.
