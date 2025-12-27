"""Script to parse all downloaded PDFs to markdown."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.sec_digest.config import Config
from src.sec_digest.parser import SECDigestParser


def main():
    """Main parsing script."""
    # Load configuration
    print("Loading configuration...")
    config = Config.load()
    config.ensure_directories()

    print(f"\nConfiguration:")
    print(f"  Input: {config.paths.raw_data}")
    print(f"  Output: {config.paths.markdown}")
    print(f"  Database: {config.paths.database}")

    # Initialize parser
    parser = SECDigestParser(
        output_dir=config.paths.markdown,
        db_path=config.paths.database,
    )

    # Process PDFs for configured years
    for year in range(config.scraper.start_year, config.scraper.end_year + 1):
        print(f"\n{'=' * 80}")
        print(f"Processing year: {year}")
        print(f"{'=' * 80}")

        # Find all PDFs for this year
        pdf_dir = config.paths.raw_data / str(year)
        if not pdf_dir.exists():
            print(f"Directory not found: {pdf_dir}")
            continue

        pdf_files = sorted(pdf_dir.glob("*.pdf"))
        print(f"Found {len(pdf_files)} PDF files")

        if not pdf_files:
            print("No PDFs to process")
            continue

        # Parse batch
        stats = parser.parse_batch(pdf_files, show_progress=True)

        print(f"\nYear {year} Summary:")
        print(f"  Total PDFs: {stats['total']}")
        print(f"  ✓ Completed: {stats['completed']}")
        print(f"  ✗ Failed: {stats['failed']}")
        print(f"  ⊙ Skipped: {stats['skipped']}")

    # Overall summary
    print(f"\n{'=' * 80}")
    print("Overall Parsing Summary")
    print(f"{'=' * 80}")

    summary = parser.get_parsing_summary()
    for status, data in summary.items():
        avg_pages = data.get('avg_pages', 0) or 0
        avg_length = data.get('avg_length', 0) or 0
        print(
            f"{status}: {data['count']} files "
            f"(avg {avg_pages:.1f} pages, {avg_length/1000:.1f}K chars)"
        )


if __name__ == "__main__":
    main()
