# SEC News Digest Extraction Pipeline

This file is the consolidated project guide and status reference for this repository.

## Project Goal

Build a reproducible pipeline that extracts structured SEC enforcement action data from SEC News Digest archives and stores it in DuckDB for research and analysis.

Primary coverage target: 1956-2014 (digitally available SEC digest era).

## Current Pipeline

1. Download digest source files from SEC (`data/raw/{year}/`).
2. Parse source files to markdown (`data/markdown/{year}/`).
3. Extract structured actions via LLM (`data/extracted/{year}/`).
4. Load validated outputs into DuckDB (`data/processed/sec_digest.duckdb`).

## Repository Structure

```text
sec-gazette/
├── config.yaml
├── pyproject.toml
├── scripts/
│   ├── 01_scrape_and_download.py
│   ├── 02_parse_pdfs.py
│   ├── 03_test_extraction.py
│   ├── 04_batch_extract.py
│   ├── 05_load_to_duckdb.py
│   └── test_single.py
├── src/sec_digest/
│   ├── config.py
│   ├── scraper.py
│   ├── parser.py
│   ├── extractor.py
│   └── schemas.py
└── data/
    ├── raw/
    ├── markdown/
    ├── extracted/
    └── processed/
```

## Tech Stack

- Python 3.11+
- Poetry (dependency and env management)
- httpx (async downloading)
- DuckDB (manifest and analytical storage)
- Docling (PDF parsing)
- Pydantic (schemas and validation)
- LLM provider: Ollama/OpenAI/Anthropic (configurable)

## SEC URL and File Format Rules (Verified)

The digest hosting pattern changes by era and is not fully uniform.

### Implemented logic in `src/sec_digest/scraper.py`

- Pre-2003:
  - File type: `.pdf`
  - URL pattern: `/news/digest/{year}/digMMDDYY.pdf`
- 2003-2006:
  - File type: `.txt`
  - URL pattern: `/news/digest/digMMDDYY.txt` (no year segment)
- 2007+ (primary):
  - File type: `.htm`
  - URL pattern: `/news/digest/{year}/digMMDDYY.htm`
- 2007 fallback (implemented):
  - If primary 2007 `.htm` returns 404, downloader tries:
    - `/news/digest/2007/digMMDDYY.txt`
    - `/news/digest/digMMDDYY.txt`

This fallback exists because 2007 appears mixed based on live spot checks.

## Parser Behavior (Updated)

`src/sec_digest/parser.py` now supports all current source formats:

- `.pdf` -> Docling conversion to markdown
- `.txt` -> direct text pass-through to markdown
- `.htm` / `.html` -> lightweight HTML tag stripping + text normalization

Output remains:

- `data/markdown/{year}/digest_YYYY-MM-DD.md`

`scripts/02_parse_pdfs.py` now parses all digest source types (`pdf`, `txt`, `htm`, `html`) from each year folder.

## Manifest and Tracking

Download tracking table: `download_manifest` in DuckDB.

Key fields:

- `url`
- `year`
- `date`
- `local_path`
- `download_status`
- `file_size_bytes`
- `downloaded_at`
- `error_message`

Notes:

- Existing local files are marked `skipped`.
- Previous failed URLs are skipped in later runs.
- When 2007 fallback succeeds, `error_message` stores which fallback URL was used.

## Commands (Poetry)

Use Poetry environment for all runs.

```bash
poetry run python scripts/01_scrape_and_download.py
poetry run python scripts/02_parse_pdfs.py
poetry run python scripts/03_test_extraction.py
poetry run python scripts/04_batch_extract.py
poetry run python scripts/05_load_to_duckdb.py
```

Single-file extraction debug:

```bash
poetry run python scripts/test_single.py
```

## Configuration

Primary runtime settings are in `config.yaml`.

Expected key groups:

- `paths`: raw_data, markdown, extracted, database
- `scraper`: start_year, end_year, delay_seconds, max_retries
- `llm`: provider/model/host and related inference settings

## Quality and Reliability Notes

- Respect SEC rate limits. Keep 1-2 second delays minimum.
- Retry logic is implemented for transient request failures.
- OCR quality in older documents can reduce extraction fidelity.
- Markdown cleanup and extraction guardrails are important for hallucination control.

## Near-Term Priorities

1. Run broad smoke coverage across transition years (2002, 2003, 2006, 2007, 2008).
2. Add tests for URL generation and fallback behavior in `scraper.py`.
3. Add parser tests for `.txt` and `.htm` conversion quality.
4. Keep README lightweight and point here for operational details.

## Historical Notes

- Earlier documentation assumed PDF-only archives.
- Live verification showed mixed patterns after 2002; current logic reflects verified behavior and fallback safeguards.
