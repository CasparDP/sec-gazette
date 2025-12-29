# SEC News Digest Extraction Pipeline

## Project Overview
Build a pipeline to extract structured data on SEC enforcement actions from the SEC News Digest archives (https://www.sec.gov/news/digest/digarchives/). This historical data (1956-2014, digitally available) will be used for academic research on audit quality and potentially released as a public dataset.

## Tech Stack
- Python 3.11+ with poetry for dependency management
- DuckDB for data storage
- docling for PDF parsing
- LLM provider (choose one): Ollama (local), OpenAI API, or Claude API (anthropic package)
- httpx for async HTTP requests
- BeautifulSoup for HTML parsing
- pydantic for data validation and schemas
- python-dotenv for environment variable management
- pytest for testing

## Directory Structure
Create this structure:

sec_digest_extraction/
├── pyproject.toml
├── README.md
├── .env.example              # Template for environment variables
├── config.yaml               # Configuration for LLM provider, paths, etc.
├── data/
│   ├── raw/                    # Original PDFs organized by year
│   ├── markdown/               # Docling parsed output
│   ├── extracted/              # LLM JSON output
│   └── processed/              # Final DuckDB database
├── src/
│   └── sec_digest/
│       ├── __init__.py
│       ├── config.py           # Configuration loading and validation
│       ├── scraper.py          # Index scraping and PDF download
│       ├── parser.py           # Docling PDF to markdown
│       ├── extractor.py        # LLM structured extraction
│       ├── consolidate.py      # Aggregate to DuckDB
│       └── schemas.py          # Pydantic models for extraction
├── scripts/
│   ├── 01_scrape_and_download.py
│   ├── 02_parse_pdfs.py
│   ├── 03_extract_with_llm.py
│   └── 04_consolidate.py
├── tests/
│   ├── __init__.py
│   ├── test_scraper.py
│   ├── test_parser.py
│   ├── test_extractor.py
│   └── test_schemas.py
└── notebooks/
    └── exploration.ipynb

## Phase 1: Discovery & Download

### Step 1.1: Explore SEC Archive Structure
First, explore the SEC digest archives to understand:
- URL patterns for archive index pages
- How PDFs are linked (direct links vs. intermediate pages)
- Date range available (appears to go back to 1930s)
- Any rate limiting or robots.txt restrictions

Start here: https://www.sec.gov/news/digest/digarchives/

The archive includes the years 1956 until 2014. For years prior to 1956, the digests are available in scanned format only.

Example url to a PDF from 1984: https://www.sec.gov/news/digest/1984/dig092884.pdf
The hrefs include the year and date in the filename.

```html
<a href="/news/digest/1984/dig092884.pdf">September 28, 1984 issue (dig092884.pdf)</a>
```

### Step 1.2: Build the Scraper
Create `src/sec_digest/scraper.py` with:
- Function to parse archive index pages and extract PDF URLs
- Async PDF downloader with rate limiting (1-2 second delays between requests)
- Progress tracking and resumption capability (save manifest to JSON/DuckDB)
- Organize downloads as: `data/raw/{year}/digest_{YYYY-MM-DD}.pdf`

### Step 1.3: Create Download Manifest
Track all downloads in a DuckDB table or JSON manifest:
```python
{
    "url": "https://...",
    "year": 1985,
    "date": "1985-01-15",
    "local_path": "data/raw/1985/digest_1985-01-15.pdf",
    "download_status": "pending|completed|failed",
    "file_size_bytes": null,
    "downloaded_at": null
}
```

#TODO: No all years have PDFs. Until and including 2001 it is PDF. 2002-2006 it is .txt and frmo 2007 - 2014 it is .htm. Adjust download code accordingly.Also adjust parsing code accordingly. .txt may not need parsing at all. Can be read directly. .htm may need html parsing instead of pdf parsing.

## Phase 2: PDF Parsing

### Step 2.1: Docling Integration
Create `src/sec_digest/parser.py` with:
- Batch PDF to markdown conversion using docling
- Handle OCR for older scanned documents
- Output to: `data/markdown/{year}/digest_{YYYY-MM-DD}.md`
- Log parsing quality/errors

## Phase 3: LLM Extraction

### Step 3.1: Define Extraction Schema
Create `src/sec_digest/schemas.py` with Pydantic models:
```python
from pydantic import BaseModel
from typing import Optional
from datetime import date
from enum import Enum

class ActionType(str, Enum):
    ADMINISTRATIVE_PROCEEDING = "administrative_proceeding"
    LITIGATION = "litigation"
    CEASE_AND_DESIST = "cease_and_desist"
    SETTLED = "settled"
    SUSPENSION = "suspension"
    OTHER = "other"

class RespondentType(str, Enum):
    INDIVIDUAL = "individual"
    COMPANY = "company"
    AUDITOR = "auditor"
    BROKER_DEALER = "broker_dealer"
    INVESTMENT_ADVISOR = "investment_advisor"
    OTHER = "other"

class ViolationType(str, Enum):
    SECURITIES_FRAUD = "securities_fraud"
    BOOKS_AND_RECORDS = "books_and_records"
    AUDIT_FAILURE = "audit_failure"
    INSIDER_TRADING = "insider_trading"
    REGISTRATION = "registration"
    DISCLOSURE = "disclosure"
    OTHER = "other"

class EnforcementAction(BaseModel):
    action_type: ActionType
    respondent_name: str
    respondent_type: RespondentType
    violations: list[ViolationType]
    auditor_involved: Optional[str] = None
    audit_firm: Optional[str] = None
    penalty_amount_usd: Optional[float] = None
    is_settled: bool = False
    description: str
    raw_text: str  # Original text for validation

class DigestExtraction(BaseModel):
    digest_date: date
    enforcement_actions: list[EnforcementAction]
    trading_suspensions: list[dict]  # Can expand schema later
    other_items: list[str]
    extraction_notes: Optional[str] = None
```

### Step 3.2: LLM Extraction Script
Create `src/sec_digest/extractor.py` with:
- Load markdown files
- Call Claude API with structured output (tool use or JSON mode)
- Batch processing with checkpointing
- Cost tracking (input/output tokens)
- Output to: `data/extracted/{year}/digest_{YYYY-MM-DD}.json`

Use this system prompt for extraction:

You are extracting structured data from SEC News Digest documents.
Focus on enforcement actions, particularly those involving:

Auditors and accounting firms
Books and records violations
Financial reporting fraud

Be precise about names, dates, and amounts. If information is ambiguous,
note it in extraction_notes. Include the raw_text for each action to
enable validation.

## Phase 4: Consolidation

### Step 4.1: Build Final Dataset
Create `src/sec_digest/consolidate.py` with:
- Load all extracted JSONs
- Validate against Pydantic schemas
- Insert into DuckDB: `data/processed/sec_digest.duckdb`
- Create tables: `enforcement_actions`, `digests`, `extraction_metadata`

### Step 4.2: Quality Checks
- Count enforcement actions by year (should be relatively consistent)
- Flag potential extraction errors (missing fields, unusual values)
- Sample for manual validation

## Implementation Order
1. Start with Phase 1 - get the scraping working for a single year (e.g., 1985)
2. Test docling on a few PDFs to assess quality
3. Build extraction pipeline on small sample before scaling
4. Add comprehensive error handling and logging throughout

## Configuration
Create a `config.yaml` to manage settings:
```yaml
llm:
  provider: "anthropic"  # Options: "anthropic", "openai", "ollama"
  model: "claude-sonnet-4-20250514"
  temperature: 0.0

scraper:
  delay_seconds: 2
  max_retries: 3

paths:
  raw_data: "data/raw"
  markdown: "data/markdown"
  extracted: "data/extracted"
  database: "data/processed/sec_digest.duckdb"
```

## Important Notes
- Respect SEC servers: use 1-2 second delays between requests
- Check robots.txt before scraping
- Store API keys in `.env` file (use python-dotenv), never commit secrets
- Commit working code frequently
- Add good docstrings and type hints
- Write tests for critical functions (schema validation, URL parsing)

## First Task
Start by:
1. Initialize the project with poetry (`poetry init`)
2. Create the directory structure
3. Explore the SEC archive pages to understand the URL structure
4. Build the initial scraper for the archive index

Let me know what you find on the SEC site structure before proceeding with the full implementation.
