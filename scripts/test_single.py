"""Test single document extraction."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.sec_digest.config import Config
from src.sec_digest.extractor import SECDigestExtractor

config = Config.load()
extractor = SECDigestExtractor(model=config.llm.model, ollama_host=config.llm.host)

# Test the problematic file
md_file = Path("data/markdown/1985/digest_1985-01-14.md")
print(f"Testing: {md_file.name}")
print("=" * 80)

result = extractor.extract_from_file(md_file)

print(f"\nResult:")
print(f"  Has actions: {result.has_enforcement_actions}")
print(f"  Actions found: {len(result.actions)}")
print(f"  Notes: {result.extraction_notes}")
