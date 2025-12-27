"""Batch extraction script for enforcement actions."""

import sys
import json
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.sec_digest.config import Config
from src.sec_digest.extractor import SECDigestExtractor, EnforcementActionFilter


def main(limit: int = None):
    """Run batch extraction.

    Args:
        limit: Maximum number of documents to process (None = all)
    """
    print("=" * 80)
    print("SEC Digest Batch Extraction")
    print("=" * 80)

    # Load configuration
    config = Config.load()
    config.ensure_directories()

    # Initialize extractor
    extractor = SECDigestExtractor(
        model=config.llm.model,
        ollama_host=config.llm.host
    )

    # Get all markdown files
    markdown_dir = config.paths.markdown / "1985"
    all_files = sorted(markdown_dir.glob("*.md"))

    print(f"Total markdown files: {len(all_files)}")

    # Pre-filter to find documents with enforcement actions
    print("\nPre-filtering documents...")
    filtered_files = []
    for md_file in all_files:
        content = md_file.read_text()
        has_actions, _ = EnforcementActionFilter.has_enforcement_actions(content)
        if has_actions:
            filtered_files.append(md_file)

    print(f"Documents with enforcement actions: {len(filtered_files)}")

    # Apply limit if specified
    if limit:
        filtered_files = filtered_files[:limit]
        print(f"Processing first {limit} documents")

    print(f"\nStarting extraction...")
    print(f"Model: {config.llm.model}")
    print(f"Output: {config.paths.extracted}")
    print()

    # Process each file
    results = []
    stats = {
        "total": len(filtered_files),
        "processed": 0,
        "with_actions": 0,
        "total_actions": 0,
        "errors": 0,
    }

    start_time = datetime.now()

    for i, md_file in enumerate(filtered_files, 1):
        print(f"[{i}/{len(filtered_files)}] {md_file.name}...", end=" ", flush=True)

        try:
            # Extract
            result = extractor.extract_from_file(md_file)

            # Save individual result as JSON
            output_file = config.paths.extracted / "1985" / f"{md_file.stem}.json"
            output_file.parent.mkdir(parents=True, exist_ok=True)

            with open(output_file, "w") as f:
                json.dump(result.model_dump(mode="json"), f, indent=2, default=str)

            # Update stats
            stats["processed"] += 1
            if result.has_enforcement_actions:
                stats["with_actions"] += 1
                stats["total_actions"] += len(result.actions)

            print(f"✓ {len(result.actions)} actions")
            results.append(result)

        except Exception as e:
            print(f"✗ Error: {e}")
            stats["errors"] += 1

    elapsed = datetime.now() - start_time

    # Summary
    print("\n" + "=" * 80)
    print("Extraction Complete")
    print("=" * 80)
    print(f"Total processed: {stats['processed']}")
    print(f"Documents with actions: {stats['with_actions']}")
    print(f"Total actions extracted: {stats['total_actions']}")
    print(f"Errors: {stats['errors']}")
    print(f"Time elapsed: {elapsed}")
    print(f"Average per document: {elapsed / stats['processed'] if stats['processed'] > 0 else 0}")

    # Action type breakdown
    if results:
        print("\nAction Type Breakdown:")
        action_types = {"administrative": 0, "civil": 0, "criminal": 0}
        for result in results:
            for action in result.actions:
                action_types[action.action_type] += 1

        for action_type, count in action_types.items():
            print(f"  {action_type.capitalize()}: {count}")

    # Sample extractions
    print("\nSample Extractions:")
    for result in results[:3]:
        print(f"\n  {result.digest_date}:")
        for action in result.actions[:2]:  # Show first 2 actions per document
            print(f"    - {action.action_type}: {action.title}")
            if action.respondents:
                print(f"      Respondents: {', '.join(r.name for r in action.respondents[:3])}")

    print("\n" + "=" * 80)
    print(f"Results saved to: {config.paths.extracted / '1985'}")
    print("=" * 80)


if __name__ == "__main__":
    # Process first 10 documents
    main(limit=10)
