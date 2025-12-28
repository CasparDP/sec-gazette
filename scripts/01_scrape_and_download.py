"""Script to scrape and download SEC News Digest PDFs."""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.sec_digest.config import Config
from src.sec_digest.scraper import SECDigestScraper


async def main():
    """Main download script."""
    # Load configuration
    print("Loading configuration...")
    config = Config.load()
    config.ensure_directories()

    print(f"\nConfiguration:")
    print(f"  Years: {config.scraper.start_year} - {config.scraper.end_year}")
    print(f"  Output: {config.paths.raw_data}")
    print(f"  Database: {config.paths.database}")
    print(f"  Delay: {config.scraper.delay_seconds} seconds")

    # Initialize scraper
    scraper = SECDigestScraper(
        output_dir=config.paths.raw_data,
        db_path=config.paths.database,
        delay_seconds=config.scraper.delay_seconds,
        max_retries=config.scraper.max_retries,
    )

    # Download PDFs for configured years
    for year in range(config.scraper.start_year, config.scraper.end_year + 1):
        print(f"\n{'=' * 80}")
        print(f"Processing year: {year}")
        print(f"{'=' * 80}")

        stats = await scraper.download_year(year, max_concurrent=3)

        print(f"\nYear {year} Summary:")
        print(f"  Total URLs: {stats['total']}")
        print(f"  ✓ Completed: {stats['completed']}")
        print(f"  ✗ Failed: {stats['failed']}")
        print(f"  ⊙ Skipped (already downloaded): {stats['skipped']}")
        if stats.get('skipped_failed', 0) > 0:
            print(f"  ⊘ Skipped (known 404s): {stats['skipped_failed']}")

    # Overall summary
    print(f"\n{'=' * 80}")
    print("Overall Summary")
    print(f"{'=' * 80}")

    summary = scraper.get_manifest_summary()
    for status, data in summary.items():
        size_mb = data['total_bytes'] / (1024 * 1024)
        print(f"{status}: {data['count']} files ({size_mb:.2f} MB)")


if __name__ == "__main__":
    asyncio.run(main())
