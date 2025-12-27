"""LLM-based extraction of enforcement actions from SEC News Digest."""

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple

from dotenv import load_dotenv
from ollama import Client
from ollama._types import ResponseError
from pydantic import ValidationError

from .schemas import DigestExtraction, EnforcementAction

# Load environment variables
load_dotenv()


class EnforcementActionFilter:
    """Pre-filter to detect documents with enforcement actions."""

    # Fuzzy patterns to handle OCR errors
    # Allow missing letters, substitutions, and spacing issues
    PATTERNS = [
        # ADMINISTRATIVE PROCEEDINGS - allow for OCR errors
        r'ADM[I1L|!]N[I1L|!][S5Z]TRAT[I1L|!][VY]E\s+PR[O0Q][C(]EED[I1L|!]NG[S5Z]?',
        # CIVIL PROCEEDINGS
        r'C[I1L|!][VY][I1L|!]L\s+PR[O0Q][C(]EED[I1L|!]NG[S5Z]?',
        # CRIMINAL PROCEEDINGS
        r'CR[I1L|!]M[I1L|!]NAL\s+PR[O0Q][C(]EED[I1L|!]NG[S5Z]?',
        # Also catch common variations
        r'ADM[I1L|!]N.*?PROCEED',
        r'C[I1L|!]V[I1L|!]L.*?PROCEED',
        r'CR[I1L|!]M.*?PROCEED',
    ]

    @classmethod
    def has_enforcement_actions(cls, content: str) -> Tuple[bool, List[str]]:
        """
        Check if document contains enforcement action sections.

        Returns:
            Tuple of (has_actions: bool, matched_sections: List[str])
        """
        matched_sections = []

        for pattern in cls.PATTERNS:
            matches = re.finditer(pattern, content, re.IGNORECASE)
            for match in matches:
                # Get some context around the match
                start = max(0, match.start() - 50)
                end = min(len(content), match.end() + 50)
                context = content[start:end].strip()
                matched_sections.append(context)

        # Return true if we found any matches and they look substantial
        has_actions = len(matched_sections) > 0

        return has_actions, matched_sections


class SECDigestExtractor:
    """Extract enforcement actions from SEC News Digest using LLM."""

    def __init__(
        self,
        model: str = "deepseek-v3.2:cloud",
        ollama_host: str = "http://localhost:11434",
        max_retries: int = 5,
        initial_retry_delay: float = 2.0,
    ):
        """Initialize extractor.

        Args:
            model: Ollama model name
            ollama_host: Ollama server host (use https://ollama.com for cloud models)
            max_retries: Maximum number of retries for transient errors
            initial_retry_delay: Initial delay between retries in seconds (will increase exponentially)
        """
        self.model = model
        self.ollama_host = ollama_host
        self.max_retries = max_retries
        self.initial_retry_delay = initial_retry_delay

        # Check if this is a cloud model and configure client accordingly
        api_key = os.environ.get("OLLAMA_API_KEY")

        if ":cloud" in model and api_key:
            # Use Ollama Cloud with authentication
            self.client = Client(
                host="https://ollama.com",
                headers={"Authorization": f"Bearer {api_key}"}
            )
        elif api_key and ollama_host == "https://ollama.com":
            # Explicit cloud host
            self.client = Client(
                host=ollama_host,
                headers={"Authorization": f"Bearer {api_key}"}
            )
        else:
            # Use local Ollama instance
            self.client = Client(host=ollama_host)

    def extract_from_file(self, markdown_path: Path) -> Optional[DigestExtraction]:
        """Extract enforcement actions from a markdown file.

        Args:
            markdown_path: Path to markdown file

        Returns:
            DigestExtraction object or None if extraction failed
        """
        # Parse date from filename (format: digest_YYYY-MM-DD.md)
        filename = markdown_path.stem
        date_str = filename.replace("digest_", "")
        digest_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        # Read content
        content = markdown_path.read_text()

        # Pre-filter
        has_actions, matched_sections = EnforcementActionFilter.has_enforcement_actions(
            content
        )

        if not has_actions:
            # No enforcement actions found, return empty result
            return DigestExtraction(
                digest_date=digest_date,
                has_enforcement_actions=False,
                actions=[],
                extraction_notes="No enforcement action sections detected by pre-filter"
            )

        # Extract using LLM
        return self._extract_with_llm(digest_date, content, matched_sections)

    def _extract_with_llm(
        self,
        digest_date,
        content: str,
        matched_sections: List[str]
    ) -> DigestExtraction:
        """Use LLM to extract structured data with retry logic."""

        # Build extraction prompt
        prompt = self._build_extraction_prompt(content)

        # Retry loop with exponential backoff
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                # Call Ollama with JSON format
                response = self.client.chat(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": self._get_system_prompt()
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    format="json",
                    options={
                        "temperature": 0.0,  # Deterministic for extraction
                    }
                )

                # Parse response
                result_json = json.loads(response["message"]["content"])

                # Convert to Pydantic model
                actions = [
                    EnforcementAction(**action)
                    for action in result_json.get("actions", [])
                ]

                return DigestExtraction(
                    digest_date=digest_date,
                    has_enforcement_actions=len(actions) > 0,
                    actions=actions,
                    extraction_notes=result_json.get("extraction_notes")
                )

            except ResponseError as e:
                last_exception = e
                # Check if this is a retryable error
                if e.status_code in [503, 429, 500, 502, 504]:
                    # Transient errors - retry with exponential backoff
                    if attempt < self.max_retries - 1:
                        delay = self.initial_retry_delay * (2 ** attempt)
                        # Add jitter (random 0-25% of delay)
                        import random
                        jitter = delay * random.uniform(0, 0.25)
                        sleep_time = delay + jitter

                        print(f"    Retry {attempt + 1}/{self.max_retries} after {sleep_time:.1f}s (status {e.status_code})", end="", flush=True)
                        time.sleep(sleep_time)
                        print("...", end="", flush=True)
                        continue
                    else:
                        # Max retries reached
                        break
                else:
                    # Non-retryable error (e.g., 401 unauthorized)
                    break

            except (json.JSONDecodeError, ValidationError, KeyError) as e:
                # LLM returned invalid JSON or schema mismatch - don't retry
                last_exception = e
                break

        # If we get here, all retries failed
        return DigestExtraction(
            digest_date=digest_date,
            has_enforcement_actions=True,  # Pre-filter detected them
            actions=[],
            extraction_notes=f"Extraction failed after {self.max_retries} retries: {type(last_exception).__name__}: {str(last_exception)}"
        )

    def _get_system_prompt(self) -> str:
        """Get system prompt with anti-hallucination instructions."""
        return """You are an expert legal document analyst specializing in SEC enforcement actions.

Your task is to extract structured information about enforcement actions from SEC News Digest documents.

CRITICAL RULES TO PREVENT HALLUCINATION:
1. ONLY extract information that is EXPLICITLY stated in the text
2. If a field is not mentioned, leave it as null or empty - DO NOT GUESS
3. Do not infer information that is not directly stated
4. If the OCR quality is poor and you cannot read something clearly, note it in extraction_notes
5. Copy exact text for names, case numbers, and legal citations
6. If you are uncertain about any information, DO NOT include it

Focus on these sections:
- ADMINISTRATIVE PROCEEDINGS
- CIVIL PROCEEDINGS
- CRIMINAL PROCEEDINGS

Extract ONLY information from these enforcement action sections. Ignore other sections like "Investment Company Act Releases", "Securities Act Registrations", etc."""

    def _build_extraction_prompt(self, content: str) -> str:
        """Build extraction prompt."""
        return f"""Extract all enforcement actions from this SEC News Digest document.

Document content:
{content}

Return a JSON object with this structure:
{{
  "actions": [
    {{
      "action_type": "administrative" | "civil" | "criminal",
      "title": "Title from the document or null",
      "respondents": [
        {{
          "name": "Full name",
          "entity_type": "individual" | "company" | "other" | null,
          "location": "Location if mentioned or null"
        }}
      ],
      "violations": [
        {{
          "statute": "Section/Rule violated or null",
          "description": "Description or null"
        }}
      ],
      "sanctions": [
        {{
          "sanction_type": "suspension" | "revocation" | "injunction" | "fine" | "imprisonment" | "disgorgement" | "cease_and_desist" | "other" | null,
          "description": "Description of sanction",
          "duration": "Duration if applicable or null",
          "amount": "Monetary amount if applicable or null"
        }}
      ],
      "settlement": true | false | null,
      "court": "Court name or null",
      "case_number": "Case number or null",
      "release_number": "Release number or null",
      "full_text": "Complete text of this enforcement action"
    }}
  ],
  "extraction_notes": "Any notes about quality issues or null"
}}

Remember:
- Only extract what is explicitly stated
- If information is missing, use null
- Include the full text of each action
- Be precise with names and legal citations"""
