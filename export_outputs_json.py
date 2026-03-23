#!/usr/bin/env python3
"""Export Betclic scraper rows to JSON (stdout or file)."""

import argparse
import importlib.util
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def load_betclic():
    spec = importlib.util.spec_from_file_location("betclic_scraper", ROOT / "betclic.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load betclic.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    p = argparse.ArgumentParser(description="Export scraper outputs as JSON")
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=ROOT / "output_rows.json",
        help="Output JSON path (default: output_rows.json in this folder)",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Max matches per sport (default: no practical cap)",
    )
    p.add_argument(
        "--sports",
        nargs="*",
        default=["football", "tennis", "basketball", "ice-hockey"],
        help="gRPC sport codes to scrape",
    )
    args = p.parse_args()

    mod = load_betclic()
    scraper = mod.BetclicScraper()
    lim = args.limit if args.limit is not None else mod.GRPC_MATCH_LIST_LIMIT_DEFAULT
    all_rows: list = []

    def _one(sport: str) -> list:
        try:
            rows = scraper._fetch_sport_grpc(sport, limit=lim)
            for r in rows:
                r["_sport_grpc"] = sport
            return rows
        except Exception as e:
            print(f"{sport}: {e}", file=sys.stderr)
            return []

    workers = max(1, len(args.sports))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_one, s): s for s in args.sports}
        for fut in as_completed(futures):
            all_rows.extend(fut.result())

    payload = {
        "meta": {
            "source": "Betclic gRPC-Web offering.begmedia.com",
            "sports_queried": args.sports,
            "limit_per_sport": lim,
            "row_count": len(all_rows),
        },
        "outputs": all_rows,
    }

    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Wrote {len(all_rows)} rows to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
