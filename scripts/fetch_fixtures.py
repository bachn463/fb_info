"""One-shot helper: fetch PFR HTML for the parser test fixtures.

Run once after creating data/pfr_session.json. Populates data/cache/
via the Scraper, then copies each cached file into
tests/fixtures/<page>/<year>.html so the parser tests stay
network-free. Re-running is a no-op for already-cached pages.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from ffpts.scraper import CloudflareSessionExpired, Scraper

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"

YEARS = [1985, 2023]

# (path_template, fixture_subdir). Path uses {year}; fixture file is
# named <year>.html under tests/fixtures/<fixture_subdir>/.
PAGES: list[tuple[str, str]] = [
    ("/years/{year}/passing.htm",   "passing"),
    ("/years/{year}/rushing.htm",   "rushing"),
    ("/years/{year}/receiving.htm", "receiving"),
    ("/years/{year}/defense.htm",   "defense"),
    ("/years/{year}/kicking.htm",   "kicking"),
    ("/years/{year}/returns.htm",   "returns"),
    ("/years/{year}/draft.htm",     "draft"),
    ("/years/{year}/",              "standings"),
]


def main() -> int:
    try:
        scraper = Scraper.from_session_file()
    except CloudflareSessionExpired as e:
        print(f"Cannot start: {e}", file=sys.stderr)
        return 1

    fetched = 0
    for year in YEARS:
        for path_tmpl, subdir in PAGES:
            path = path_tmpl.format(year=year)
            cache_hit = scraper.is_cached(path)
            try:
                html = scraper.get(path)
            except CloudflareSessionExpired as e:
                print(f"\nFAIL on {path}:\n{e}", file=sys.stderr)
                return 2
            except Exception as e:
                print(f"\nFAIL on {path}: {e.__class__.__name__}: {e}", file=sys.stderr)
                return 3
            note = "cache" if cache_hit else "fetched"
            print(f"  [{note:7}] {path}  ({len(html):,} bytes)")
            fixture_file = FIXTURES_DIR / subdir / f"{year}.html"
            fixture_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(scraper._cache_path_for(path), fixture_file)
            if not cache_hit:
                fetched += 1
    total = len(YEARS) * len(PAGES)
    print(f"\nDone. {fetched} live fetches, {total - fetched} cache hits.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
