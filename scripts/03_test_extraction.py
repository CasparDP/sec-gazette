"""Test script for enforcement action extraction."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.sec_digest.config import Config
from src.sec_digest.extractor import SECDigestExtractor, EnforcementActionFilter


def test_prefilter():
    """Test the pre-filter on all documents."""
    print("=" * 80)
    print("Testing Pre-Filter")
    print("=" * 80)

    config = Config.load()
    markdown_dir = config.paths.markdown / "1985"

    all_files = sorted(markdown_dir.glob("*.md"))
    print(f"Total files: {len(all_files)}")

    matched_files = []
    for md_file in all_files:
        content = md_file.read_text()
        has_actions, sections = EnforcementActionFilter.has_enforcement_actions(content)

        if has_actions:
            matched_files.append(md_file)

    print(f"Files with enforcement actions: {len(matched_files)}")
    print(f"Files without: {len(all_files) - len(matched_files)}")
    print()

    return matched_files


def test_extraction():
    """Test LLM extraction on sample documents."""
    print("=" * 80)
    print("Testing LLM Extraction on Sample Documents")
    print("=" * 80)

    config = Config.load()
    extractor = SECDigestExtractor(
        model=config.llm.model,
        ollama_host=config.llm.host
    )

    # Test on a few sample files
    markdown_dir = config.paths.markdown / "1985"

    # Select diverse test cases
    test_files = [
        markdown_dir / "digest_1985-01-07.md",  # Has administrative proceedings
        markdown_dir / "digest_1985-06-20.md",  # Has multiple types
        markdown_dir / "digest_1985-03-20.md",  # Has civil proceedings
    ]

    for md_file in test_files:
        if not md_file.exists():
            print(f"⚠️  File not found: {md_file.name}")
            continue

        print(f"\n{'─' * 80}")
        print(f"Processing: {md_file.name}")
        print(f"{'─' * 80}")

        try:
            result = extractor.extract_from_file(md_file)

            print(f"Date: {result.digest_date}")
            print(f"Has enforcement actions: {result.has_enforcement_actions}")
            print(f"Actions found: {len(result.actions)}")

            if result.extraction_notes:
                print(f"Notes: {result.extraction_notes}")

            for i, action in enumerate(result.actions, 1):
                print(f"\n  Action {i}:")
                print(f"    Type: {action.action_type}")
                print(f"    Title: {action.title}")
                print(f"    Respondents: {len(action.respondents)}")
                if action.respondents:
                    for resp in action.respondents:
                        print(f"      - {resp.name} ({resp.entity_type or 'unknown'})")
                print(f"    Violations: {len(action.violations)}")
                print(f"    Sanctions: {len(action.sanctions)}")
                if action.sanctions:
                    for sanction in action.sanctions:
                        print(f"      - {sanction.sanction_type}: {sanction.description[:100]}...")
                print(f"    Settlement: {action.settlement}")
                print(f"    Release #: {action.release_number}")

            print()

        except Exception as e:
            print(f"❌ Error processing {md_file.name}: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 80)
    print("Test Complete")
    print("=" * 80)


if __name__ == "__main__":
    # First test the pre-filter
    matched = test_prefilter()

    print("\nPress Enter to continue with LLM extraction test (this will use the LLM)...")
    input()

    # Then test extraction
    test_extraction()
