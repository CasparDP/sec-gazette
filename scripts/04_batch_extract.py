"""Batch extraction script for enforcement actions.

Usage:
    poetry run python scripts/04_batch_extract.py                  # all years, all files
    poetry run python scripts/04_batch_extract.py --limit 20       # first 20 per year
    poetry run python scripts/04_batch_extract.py --year 2000      # single year
    poetry run python scripts/04_batch_extract.py --year 2000 --limit 5
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.sec_digest.config import Config
from src.sec_digest.extractor import SECDigestExtractor, EnforcementActionFilter


def process_year(year: int, extractor, config, limit: Optional[int] = None) -> dict:
    """Extract enforcement actions for all markdown files in a given year.

    Skips files that already have a corresponding JSON output.
    """
    markdown_dir = config.paths.markdown / str(year)
    if not markdown_dir.exists():
        print(f"  No markdown directory for {year}, skipping.")
        return {"total": 0, "skipped": 0, "processed": 0, "with_actions": 0, "total_actions": 0, "errors": 0}

    all_files = sorted(markdown_dir.glob("*.md"))
    output_dir = config.paths.extracted / str(year)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"  Found {len(all_files)} markdown files")

    # Pre-filter: enforcement sections present + not already processed
    pending = []
    n_skipped = 0
    for md_file in all_files:
        output_file = output_dir / f"{md_file.stem}.json"
        if output_file.exists():
            n_skipped += 1
            continue
        content = md_file.read_text()
        has_actions, _ = EnforcementActionFilter.has_enforcement_actions(content)
        if has_actions:
            pending.append(md_file)

    print(f"  Already processed (skipped): {n_skipped}")
    print(f"  With enforcement sections: {len(pending)}")

    if limit is not None:
        pending = pending[:limit]
        print(f"  Applying --limit: processing first {len(pending)}")

    stats = {
        "total": len(pending),
        "skipped": n_skipped,
        "processed": 0,
        "with_actions": 0,
        "total_actions": 0,
        "errors": 0,
    }

    results = []
    for i, md_file in enumerate(pending, 1):
        print(f"  [{i}/{len(pending)}] {md_file.name}...", end=" ", flush=True)
        output_file = output_dir / f"{md_file.stem}.json"
        try:
            result = extractor.extract_from_file(md_file)

            with open(output_file, "w") as f:
                json.dump(result.model_dump(mode="json"), f, indent=2, default=str)

            stats["processed"] += 1
            if result.has_enforcement_actions:
                stats["with_actions"] += 1
                stats["total_actions"] += len(result.actions)

            print(f"✓ {len(result.actions)} actions")
            results.append(result)

        except Exception as e:
            print(f"✗ Error: {e}")
            stats["errors"] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(description="SEC Digest batch extraction")
    parser.add_argument("--year", type=int, default=None,
                        help="Process a single year (default: all years in config range)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max files to process per year (default: all)")
    args = parser.parse_args()

    print("=" * 80)
    print("SEC Digest Batch Extraction")
    print("=" * 80)

    config = Config.load()
    config.ensure_directories()

    extractor = SECDigestExtractor(
        model=config.llm.model,
        ollama_host=config.llm.host,
    )

    print(f"Model:    {config.llm.model}")
    print(f"Output:   {config.paths.extracted}")
    if args.limit:
        print(f"Limit:    {args.limit} files per year")

    years = [args.year] if args.year else range(config.scraper.start_year, config.scraper.end_year + 1)

    totals = {"skipped": 0, "processed": 0, "with_actions": 0, "total_actions": 0, "errors": 0}
    start_time = datetime.now()

    for year in years:
        print(f"\n{'=' * 80}")
        print(f"Year: {year}")
        print(f"{'=' * 80}")
        stats = process_year(year, extractor, config, limit=args.limit)
        for k in totals:
            totals[k] += stats.get(k, 0)

    elapsed = datetime.now() - start_time

    print(f"\n{'=' * 80}")
    print("Summary")
    print(f"{'=' * 80}")
    print(f"  Skipped (already done): {totals['skipped']}")
    print(f"  Processed:              {totals['processed']}")
    print(f"  With actions:           {totals['with_actions']}")
    print(f"  Total actions:          {totals['total_actions']}")
    print(f"  Errors:                 {totals['errors']}")
    print(f"  Time elapsed:           {elapsed}")
    if totals["processed"] > 0:
        avg = elapsed / totals["processed"]
        print(f"  Avg per document:       {avg}")


if __name__ == "__main__":
    main()
