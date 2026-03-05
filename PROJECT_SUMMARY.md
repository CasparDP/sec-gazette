# SEC News Digest Enforcement Actions Extraction Pipeline

## Project Overview

This project extracts historical SEC enforcement actions from the SEC News Digest archive (1975-1995) using a multi-stage pipeline:

1. **Download** - Fetch PDF files from SEC archives
2. **Parse** - Convert PDFs to markdown using Docling
3. **Extract** - Use LLM (Ollama Cloud) to extract structured enforcement action data
4. **Load** - Store in DuckDB for analysis

**Current Status:** Phase 3 (LLM Extraction) complete with optimizations. Ready for full batch processing.

## Architecture

```
data/
├── pdfs/           # Downloaded PDFs by year
│   ├── 1985/
│   └── ...
├── markdown/       # Parsed markdown by year
│   ├── 1985/
│   └── ...
└── extracted/      # JSON extractions by year
    ├── 1985/
    └── ...

scripts/
├── 01_scrape_and_download.py   # Download PDFs from SEC
├── 02_parse_pdfs.py            # Convert PDFs to markdown
├── 03_test_extraction.py       # Test LLM extraction on samples
├── 04_batch_extract.py         # Batch LLM extraction
├── 05_load_to_duckdb.py        # Load into database
└── test_single.py              # Debug single document

src/sec_digest/
├── config.py                # Configuration and paths
├── extractor.py            # LLM extraction logic
└── schemas.py              # Pydantic data models
```

## Configuration

**File:** `config.yaml`

```yaml
paths:
  pdfs: data/pdfs
  markdown: data/markdown
  extracted: data/extracted
  database: data/sec_enforcement.duckdb

llm:
  model: deepseek-v3.2:cloud
  host: https://ollama.com

download:
  base_url: https://www.sec.gov/files/data/news-digest
  year: 1985
```

**Environment:** `.env`

```bash
OLLAMA_API_KEY=<your-api-key>
```

Get API key from: https://docs.ollama.com/cloud

## Data Models

### EnforcementAction

```python
{
  "action_type": "administrative" | "civil" | "criminal",
  "title": str,
  "respondents": [
    {
      "name": str,
      "entity_type": "individual" | "company" | "other",
      "location": str
    }
  ],
  "violations": [
    {
      "statute": str,
      "description": str
    }
  ],
  "sanctions": [
    {
      "sanction_type": "suspension" | "revocation" | "injunction" | "fine" | "imprisonment" | "disgorgement" | "cease_and_desist" | "other",
      "description": str,
      "duration": str,
      "amount": str
    }
  ],
  "settlement": bool,
  "court": str,
  "case_number": str,
  "release_number": str,
  "full_text": str
}
```

## Scripts and Commands

### Phase 1: Download PDFs

```bash
# Single year
poetry run python scripts/01_scrape_and_download.py --year 1985

# Multiple years (overnight job)
nohup bash -c '
for year in {1975..1995}; do
  echo "Downloading year $year..."
  poetry run python scripts/01_scrape_and_download.py --year $year
done
' > download.log 2>&1 &

tail -f download.log
```

### Phase 2: Parse PDFs to Markdown

```bash
# Single year
poetry run python scripts/02_parse_pdfs.py --year 1985

# Multiple years (overnight job)
nohup bash -c '
for year in {1975..1995}; do
  echo "Parsing year $year..."
  poetry run python scripts/02_parse_pdfs.py --year $year
done
' > parse.log 2>&1 &

tail -f parse.log
```

### Phase 3: LLM Extraction

```bash
# Test on 3 sample documents
poetry run python scripts/03_test_extraction.py

# Test single problematic document
poetry run python scripts/test_single.py

# Batch extract first 10 documents
poetry run python scripts/04_batch_extract.py

# Full batch (modify limit=None in script)
# Edit scripts/04_batch_extract.py: main(limit=None)
poetry run python scripts/04_batch_extract.py
```

### Phase 4: Load to DuckDB

```bash
poetry run python scripts/05_load_to_duckdb.py
```

Query the database:

```python
import duckdb
con = duckdb.connect('data/sec_enforcement.duckdb')
con.execute("SELECT * FROM enforcement_actions LIMIT 5").fetchall()
```

## Key Optimizations Implemented

### 1. Pre-filtering (Cost Savings)

**Problem:** Many documents don't contain enforcement actions
**Solution:** Fuzzy regex pre-filter before LLM processing

```python
# Handles OCR errors: ADM1N1STRAT1VE → ADMINISTRATIVE
PATTERNS = [
    r'ADM[I1L|!]N[I1L|!][S5Z]TRAT[I1L|!][VY]E\s+PR[O0Q][C(]EED[I1L|!]NG[S5Z]?',
    r'C[I1L|!][VY][I1L|!]L\s+PR[O0Q][C(]EED[I1L|!]NG[S5Z]?',
    r'CR[I1L|!]M[I1L|!]NAL\s+PR[O0Q][C(]EED[I1L|!]NG[S5Z]?',
]
```

**Result:** 197/250 documents in 1985 have enforcement actions (79% filter rate)

### 2. Markdown Table Stripping (Token Reduction)

**Problem:** Tables contain irrelevant filings and garbled OCR
**Solution:** Strip all markdown tables before LLM processing

```python
def _strip_markdown_tables(content: str) -> str:
    """Remove lines containing | character (markdown tables)."""
    lines = [line for line in content.split('\n') if '|' not in line]
    return '\n'.join(lines)
```

**Result:** 24.4% content reduction (13.5KB → 10.2KB average)

### 3. Markdown Fence Cleaning (Parse Errors)

**Problem:** LLM sometimes returns JSON wrapped in ```json ... ```
**Solution:** Strip code fences before parsing

```python
def _clean_json_response(content: str) -> str:
    """Remove markdown code fences from JSON response."""
    if content.startswith('```'):
        lines = content.split('\n')[1:]  # Remove opening fence
        if lines[-1].strip() == '```':
            lines = lines[:-1]  # Remove closing fence
        return '\n'.join(lines)
    return content
```

**Result:** Eliminated JSONDecodeError from wrapped responses

### 4. Retry Logic with Exponential Backoff (Reliability)

**Problem:** Transient API errors (503, 429)
**Solution:** Exponential backoff with jitter

```python
# Retries: 2s → 4s → 8s → 16s → 32s (with 0-25% jitter)
for attempt in range(max_retries):
    try:
        response = self.client.chat(...)
    except ResponseError as e:
        if e.status_code in [503, 429, 500, 502, 504]:
            delay = initial_retry_delay * (2 ** attempt)
            jitter = delay * random.uniform(0, 0.25)
            time.sleep(delay + jitter)
```

**Result:** Handles transient errors, respects rate limits

### 5. Anti-Hallucination System Prompt

**Problem:** LLM might invent data
**Solution:** Strict extraction-only system prompt

```
CRITICAL RULES TO PREVENT HALLUCINATION:
1. ONLY extract information that is EXPLICITLY stated in the text
2. If a field is not mentioned, leave it as null or empty - DO NOT GUESS
3. Do not infer information that is not directly stated
4. If the OCR quality is poor and you cannot read something clearly, note it in extraction_notes
5. Copy exact text for names, case numbers, and legal citations
6. If you are uncertain about any information, DO NOT include it
```

**Result:** High quality extractions with OCR error notes

## Known Issues and Solutions

### Issue 1: Hourly API Rate Limits

**Symptom:** `status code: 429` with "hourly usage limit" message
**Cause:** Ollama Cloud free tier limits
**Solution:**
- Wait for hourly quota reset
- Or upgrade Ollama Cloud plan
- Retry logic handles gracefully

### Issue 2: OCR Quality in Old Documents

**Symptom:** Misspellings like "Comission", "reglstered", "usoe COCA"
**Solution:**
- Fuzzy pre-filter handles common OCR errors
- LLM notes OCR issues in `extraction_notes`
- Table stripping removes worst garbled content

### Issue 3: Empty LLM Responses (Resolved)

**Symptom:** JSONDecodeError on large/garbled documents
**Cause:** Large payloads with heavily garbled tables
**Solution:** Table stripping reduced payload size by 24%

## Testing Random Samples

After downloading and parsing all years, test a random sample:

```bash
# Generate random sample of 20 documents
poetry run python -c "
import random
from pathlib import Path

files = list(Path('data/markdown').rglob('*.md'))
sample = random.sample(files, min(20, len(files)))

print('Random sample for testing:')
for f in sample:
    print(f'  {f}')
" > sample_files.txt

cat sample_files.txt
```

Then manually test extraction on these samples using `test_single.py` (modify the file path).

## Database Schema

```sql
-- Main table
CREATE TABLE enforcement_actions (
    id INTEGER PRIMARY KEY,
    digest_date DATE NOT NULL,
    action_type VARCHAR NOT NULL,
    title VARCHAR,
    settlement BOOLEAN,
    court VARCHAR,
    case_number VARCHAR,
    release_number VARCHAR,
    full_text TEXT,
    extraction_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Related entities
CREATE TABLE respondents (
    id INTEGER PRIMARY KEY,
    action_id INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    entity_type VARCHAR,
    location VARCHAR,
    FOREIGN KEY (action_id) REFERENCES enforcement_actions(id)
);

CREATE TABLE violations (
    id INTEGER PRIMARY KEY,
    action_id INTEGER NOT NULL,
    statute VARCHAR,
    description TEXT,
    FOREIGN KEY (action_id) REFERENCES enforcement_actions(id)
);

CREATE TABLE sanctions (
    id INTEGER PRIMARY KEY,
    action_id INTEGER NOT NULL,
    sanction_type VARCHAR,
    description TEXT,
    duration VARCHAR,
    amount VARCHAR,
    FOREIGN KEY (action_id) REFERENCES enforcement_actions(id)
);
```

## Performance Metrics

**Test Run (10 documents, 1985):**
- Total processed: 10
- Documents with actions: 8 (2 hit rate limits)
- Total actions extracted: 15
- Time elapsed: ~8.5 minutes
- Average per document: ~51 seconds

**Projected for Full Dataset:**
- 1975-1995: 21 years × ~250 documents/year = ~5,250 documents
- Estimated with pre-filter: ~4,150 documents with actions
- Estimated time: ~35 hours at current rate
- Estimated API cost: ~$20-40 (depending on Ollama pricing)

## Next Steps

### Immediate (Before Next Session)

1. ✅ Download and parse PDFs for 1975-1995 (overnight job)
2. ✅ Test random sample for new edge cases
3. ⏸️ If samples look good, wait for next session

### Future Work

1. **Full LLM Extraction** - Process all ~4,150 documents with enforcement actions
2. **Load to DuckDB** - Import all extracted JSON files
3. **Data Quality Review** - Spot check extractions, validate against source PDFs
4. **Analysis** - Query database for trends, patterns, statistics
5. **Improvements:**
   - Batch processing in parallel (if API limits allow)
   - Better OCR error handling
   - Validation against known SEC release numbers
   - Export to CSV/Excel for sharing

## File Locations

**Configuration:**
- `config.yaml` - Pipeline configuration
- `.env` - API keys (gitignored)

**Data:**
- `data/pdfs/{year}/` - Downloaded PDF files
- `data/markdown/{year}/` - Parsed markdown files
- `data/extracted/{year}/` - JSON extraction results
- `data/sec_enforcement.duckdb` - DuckDB database

**Code:**
- `src/sec_digest/` - Core library
- `scripts/` - Executable scripts for each phase

## Troubleshooting

### API Authentication Errors

```bash
# Error: 401 Unauthorized
# Fix: Check .env file has OLLAMA_API_KEY set
cat .env | grep OLLAMA_API_KEY
```

### Rate Limiting

```bash
# Error: 429 Too Many Requests
# Fix: Wait for hourly quota reset or upgrade plan
# Retry logic will handle automatically
```

### JSON Parsing Errors

```bash
# Error: JSONDecodeError
# Fix: Already handled by markdown fence cleaning
# If still occurring, check extractor.py line 217-219
```

### Missing Dependencies

```bash
# Reinstall dependencies
poetry install
```

## Example Queries

```python
import duckdb

con = duckdb.connect('data/sec_enforcement.duckdb')

# Total actions by type
con.execute("""
    SELECT action_type, COUNT(*)
    FROM enforcement_actions
    GROUP BY action_type
""").fetchall()

# Top respondents
con.execute("""
    SELECT r.name, COUNT(*) as action_count
    FROM respondents r
    GROUP BY r.name
    HAVING action_count > 1
    ORDER BY action_count DESC
    LIMIT 10
""").fetchall()

# Actions by year
con.execute("""
    SELECT YEAR(digest_date) as year, COUNT(*) as count
    FROM enforcement_actions
    GROUP BY year
    ORDER BY year
""").fetchall()

# Sanction types
con.execute("""
    SELECT sanction_type, COUNT(*) as count
    FROM sanctions
    WHERE sanction_type IS NOT NULL
    GROUP BY sanction_type
    ORDER BY count DESC
""").fetchall()
```

## Credits

**Tools Used:**
- [Ollama Cloud](https://ollama.com) - LLM API (deepseek-v3.2)
- [Docling](https://github.com/DS4SD/docling) - PDF parsing
- [DuckDB](https://duckdb.org) - Analytics database
- [Pydantic](https://pydantic.dev) - Data validation

**Data Source:**
- [SEC News Digest Archive](https://www.sec.gov/news/digest-archive) (1975-1995)

---

**Last Updated:** 2025-12-27
**Status:** Phase 3 complete with optimizations, ready for full batch processing
