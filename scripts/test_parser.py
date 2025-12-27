"""Test the PDF parser on a sample file."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.sec_digest.config import Config
from src.sec_digest.parser import SECDigestParser


def main():
    """Test parser on a single PDF."""
    # Load configuration
    config = Config.load()

    # Find a sample PDF
    pdf_dir = config.paths.raw_data / "1985"
    sample_pdfs = sorted(pdf_dir.glob("*.pdf"))[:3]  # Get first 3 PDFs

    if not sample_pdfs:
        print("No PDFs found. Please run the download script first.")
        return

    print(f"Testing parser on {len(sample_pdfs)} sample PDF(s)...")
    print("=" * 80)

    # Initialize parser
    parser = SECDigestParser(
        output_dir=config.paths.markdown,
        db_path=config.paths.database,
    )

    # Parse each sample
    for pdf_path in sample_pdfs:
        print(f"\nParsing: {pdf_path.name}")
        print("-" * 80)

        result = parser.parse_pdf(pdf_path)
        parser.save_result_to_db(result)

        print(f"Status: {result.parsing_status}")
        if result.parsing_status == "completed":
            print(f"Pages: {result.page_count}")
            print(f"Markdown length: {result.markdown_length} chars")
            print(f"Output: {result.markdown_path}")

            # Show first 500 chars of markdown
            markdown_path = Path(result.markdown_path)
            if markdown_path.exists():
                content = markdown_path.read_text()
                print(f"\nFirst 500 characters of markdown:")
                print("-" * 80)
                print(content[:500])
                print("...")

        elif result.parsing_status == "failed":
            print(f"Error: {result.error_message}")

    print("\n" + "=" * 80)
    print("Test complete!")


if __name__ == "__main__":
    main()
