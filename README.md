# SEC News Digest Historical Database

> **🚧 Work in Progress**
> 
> This project is under active development. The initial data extraction pipeline is being built and tested. Full completion status will be announced in this README.

A comprehensive extraction pipeline for building a structured database of SEC enforcement actions from historical News Digest archives (1956-2014).

## Project Overview

This project aims to digitize and extract structured data from decades of SEC News Digest publications, transforming scattered PDF archives into a queryable database of enforcement actions. This dataset will support academic research on audit quality, regulatory patterns, and historical enforcement trends.

### Key Objectives

- **Historical Data Extraction:** Parse and structure SEC enforcement actions from 1956-2014 archives
- **Comprehensive Coverage:** Focus on enforcement actions, trading suspensions, and regulatory proceedings
- **Research-Ready Dataset:** Create a public dataset for academic analysis of SEC enforcement patterns
- **Audit Focus:** Prioritize data related to auditors, accounting firms, and financial reporting violations

## What This Database Will Contain

The extracted database includes:

- **Enforcement Actions** - Administrative proceedings, litigation, cease-and-desist orders, suspensions
- **Respondent Information** - Names, types (individuals, companies, audit firms, brokers, etc.)
- **Violation Details** - Categories such as securities fraud, books and records violations, audit failures, insider trading
- **Penalty Information** - Settlement amounts and terms
- **Structured Metadata** - Dates, extraction confidence notes, source references

## Technology Stack

- **Python 3.11+** with Poetry dependency management
- **DuckDB** for efficient data storage and querying
- **Docling** for intelligent PDF parsing and OCR
- **Ollama** for local LLM-powered structured extraction
- **Async HTTP** with httpx for respectful web scraping
- **Pydantic** for data validation and schema enforcement

## Project Phases

1. **Discovery & Download** - Scrape SEC archive index and download historical PDFs
2. **PDF Parsing** - Convert PDFs to structured markdown using Docling
3. **LLM Extraction** - Use Ollama to extract and structure enforcement action data
4. **Consolidation** - Validate and load data into DuckDB for analysis

## Getting Started

See [CLAUDE_INSTRUCTIONS.md](./CLAUDE_INSTRUCTIONS.md) for detailed implementation specifications.

### Quick Setup

```bash
# Initialize project
poetry init

# Create directory structure
mkdir -p data/{raw,markdown,extracted,processed} src/sec_digest scripts tests notebooks

# Install dependencies
poetry install
```

## Important Notes

- **Rate Limiting:** The scraper respects SEC servers with 1-2 second delays between requests
- **Data Privacy:** All processing is local; sensitive data handling follows best practices
- **Reproducibility:** Configuration and environment variables should be tracked (see `.env.example`)

---

## About This Project

This project was conceived as an independent extension of research into SEC enforcement patterns. It was influenced by collaborative work with [**Arndt Weinrich**](https://github.com/arndtupb) on distinguishing error from intent in accounting enforcement using machine learning. The resulting dataset may be valuable for that research and other academic investigations into regulatory patterns and enforcement trends. Beyond its research potential, this project also serves as a learning exercise in exploring Claude Code's capabilities for complex, multi-phase data engineering workflows.

### Related Research

This work was motivated by and builds upon collaborative research:

> Peter, Caspar D., and Arndt Weinrich. 2025. "Distinguishing Error from Intent in Accounting Enforcement Using LLMs." *Working Paper*.

If you use this dataset in your own research, citations to both this repository and the related working paper are appreciated.

### AI Disclosure

This project was built with the help of **Claude Code** (Anthropic's AI coding assistant). Think of it as having a really helpful colleague who never gets tired, doesn't need coffee, and occasionally suggests you name things in ways that are technically correct but slightly overcomplicated. 

While the original project concept, research objectives, and technical architecture came from the author's brain, Claude Code assisted with:

- Code structure and organization
- Implementation of scraping, parsing, and extraction pipelines
- Schema design and validation logic
- Error handling and best practices
- Documentation and instructions
- Pretending to understand why anyone would want to parse 60+ years of SEC documents

All code has been reviewed by actual human eyes and is the author's responsibility. We're using AI in development because it genuinely helps, and we think that's pretty cool—so here we are, being transparent about it.

---

## License

This project and the resulting dataset are released under the **Creative Commons Attribution 4.0 International (CC BY 4.0)** license.

### How to Cite

If you use this dataset or code in your research, please cite it as follows:

```bibtex
@dataset{sec_news_digest_database,
  author = {Peter, Caspar David},
  title = {SEC News Digest Historical Database (1956-2014)},
  year = {2025},
  url = {https://github.com/CasparDP/sec-gazette},
  license = {CC BY 4.0}
}
```

Or in plain text:

```
Peter, Caspar David. (2025). SEC News Digest Historical Database (1956-2014). 
Retrieved from https://github.com/CasparDP/sec-gazette
```

We ask that you include a link back to this repository when using the data. This helps the research community discover and build upon this work.

---

## Contributing

This is a research project. Contributions, suggestions, and improvements are welcome. Please see the project guidelines (to be added) for contribution standards.

## Contact

For questions or collaboration inquiries, please open an issue in this repository.
