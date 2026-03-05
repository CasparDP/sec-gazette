"""Digest parsing module for SEC News Digest documents."""

from pathlib import Path
from typing import Optional, Dict
from datetime import datetime
import html
import re
import duckdb
from pydantic import BaseModel

try:
    from docling.document_converter import DocumentConverter
except ImportError as e:
    raise ImportError(
        f"Failed to import docling: {e}. "
        "Please ensure docling is installed: poetry add docling"
    )


class ParsingResult(BaseModel):
    """Result of parsing a single digest source file."""
    pdf_path: str
    markdown_path: str
    parsing_status: str = "pending"  # pending|completed|failed
    page_count: Optional[int] = None
    markdown_length: Optional[int] = None
    parsed_at: Optional[str] = None
    error_message: Optional[str] = None


class SECDigestParser:
    """Parser for SEC News Digest files (PDF, TXT, HTM)."""

    def __init__(
        self,
        output_dir: Path,
        db_path: Path,
    ):
        """Initialize the parser.

        Args:
            output_dir: Directory to save markdown files
            db_path: Path to DuckDB database for tracking
        """
        self.output_dir = Path(output_dir)
        self.db_path = Path(db_path)

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._init_database()

        # Initialize docling converter with default settings
        self.converter = DocumentConverter()

    def _init_database(self) -> None:
        """Initialize DuckDB database and create parsing table."""
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS parsing_results (
                    pdf_path VARCHAR PRIMARY KEY,
                    markdown_path VARCHAR,
                    parsing_status VARCHAR,
                    page_count INTEGER,
                    markdown_length INTEGER,
                    parsed_at TIMESTAMP,
                    error_message VARCHAR
                )
            """)

    def _html_to_text(self, raw_html: str) -> str:
        """Convert simple HTML content to plain text markdown-compatible output."""
        # Remove script/style blocks first.
        content = re.sub(r"<script[\\s\\S]*?</script>", "", raw_html, flags=re.IGNORECASE)
        content = re.sub(r"<style[\\s\\S]*?</style>", "", content, flags=re.IGNORECASE)

        # Insert line breaks around common block tags to preserve rough structure.
        content = re.sub(r"</?(p|div|br|li|tr|h[1-6])[^>]*>", "\n", content, flags=re.IGNORECASE)

        # Strip remaining tags and decode entities.
        content = re.sub(r"<[^>]+>", "", content)
        content = html.unescape(content)

        # Normalize line breaks and whitespace.
        lines = [line.strip() for line in content.splitlines()]
        non_empty = [line for line in lines if line]
        return "\n".join(non_empty)

    def parse_pdf(
        self,
        pdf_path: Path,
        year: Optional[int] = None,
    ) -> ParsingResult:
        """Parse a single digest file to markdown.

        Args:
            pdf_path: Path to the digest file (.pdf, .txt, .htm)
            year: Year (for organizing output), extracted from path if not provided

        Returns:
            ParsingResult with status and metadata
        """
        pdf_path = Path(pdf_path)

        # Extract year from path if not provided
        if year is None:
            # Path format: data/raw/YYYY/digest_YYYY-MM-DD.(pdf|txt|htm)
            try:
                year = int(pdf_path.parent.name)
            except ValueError:
                year = None

        # Create output path: data/markdown/YYYY/digest_YYYY-MM-DD.md
        year_dir = str(year) if year is not None else "unknown"
        markdown_name = pdf_path.with_suffix(".md").name
        markdown_path = self.output_dir / year_dir / markdown_name
        markdown_path.parent.mkdir(parents=True, exist_ok=True)

        result = ParsingResult(
            pdf_path=str(pdf_path),
            markdown_path=str(markdown_path),
        )

        # Skip if already parsed
        if markdown_path.exists():
            result.parsing_status = "skipped"
            result.markdown_length = len(markdown_path.read_text())
            return result

        try:
            suffix = pdf_path.suffix.lower()

            if suffix == ".pdf":
                # Convert PDF to markdown using docling.
                conversion_result = self.converter.convert(str(pdf_path))
                markdown_content = conversion_result.document.export_to_markdown()
                result.page_count = (
                    len(conversion_result.document.pages)
                    if hasattr(conversion_result.document, "pages")
                    else None
                )
            elif suffix == ".txt":
                # TXT digests can be used directly as markdown content.
                markdown_content = pdf_path.read_text(encoding="utf-8", errors="replace")
            elif suffix in {".htm", ".html"}:
                raw_html = pdf_path.read_text(encoding="utf-8", errors="replace")
                markdown_content = self._html_to_text(raw_html)
            else:
                raise ValueError(f"Unsupported file type: {suffix}")

            # Save markdown file
            markdown_path.write_text(markdown_content, encoding="utf-8")

            # Update result
            result.parsing_status = "completed"
            result.markdown_length = len(markdown_content)
            result.parsed_at = datetime.now().isoformat()

        except Exception as e:
            result.parsing_status = "failed"
            result.error_message = str(e)

        return result

    def save_result_to_db(self, result: ParsingResult) -> None:
        """Save parsing result to database.

        Args:
            result: ParsingResult to save
        """
        with duckdb.connect(str(self.db_path)) as conn:
            # Check if exists
            existing = conn.execute(
                "SELECT pdf_path FROM parsing_results WHERE pdf_path = ?",
                [result.pdf_path],
            ).fetchone()

            if existing:
                # Update
                conn.execute(
                    """
                    UPDATE parsing_results
                    SET markdown_path = ?,
                        parsing_status = ?,
                        page_count = ?,
                        markdown_length = ?,
                        parsed_at = ?,
                        error_message = ?
                    WHERE pdf_path = ?
                    """,
                    [
                        result.markdown_path,
                        result.parsing_status,
                        result.page_count,
                        result.markdown_length,
                        result.parsed_at,
                        result.error_message,
                        result.pdf_path,
                    ],
                )
            else:
                # Insert
                conn.execute(
                    """
                    INSERT INTO parsing_results
                    (pdf_path, markdown_path, parsing_status, page_count,
                     markdown_length, parsed_at, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        result.pdf_path,
                        result.markdown_path,
                        result.parsing_status,
                        result.page_count,
                        result.markdown_length,
                        result.parsed_at,
                        result.error_message,
                    ],
                )

    def parse_batch(
        self,
        pdf_paths: list[Path],
        show_progress: bool = True,
    ) -> Dict[str, int]:
        """Parse multiple digest files.

        Args:
            pdf_paths: List of digest file paths
            show_progress: Whether to show progress (default: True)

        Returns:
            Dictionary with statistics
        """
        stats = {
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "total": len(pdf_paths),
        }

        for i, pdf_path in enumerate(pdf_paths, 1):
            try:
                result = self.parse_pdf(pdf_path)
                self.save_result_to_db(result)
                stats[result.parsing_status] += 1

                if show_progress and i % 10 == 0:
                    print(
                        f"Progress: {i}/{len(pdf_paths)} "
                        f"(Completed: {stats['completed']}, "
                        f"Failed: {stats['failed']}, "
                        f"Skipped: {stats['skipped']})"
                    )

            except Exception as e:
                print(f"Error processing {pdf_path}: {e}")
                stats["failed"] += 1

        return stats

    def get_parsing_summary(self) -> Dict[str, Dict]:
        """Get summary of parsing results from database.

        Returns:
            Summary statistics by status
        """
        with duckdb.connect(str(self.db_path)) as conn:
            result = conn.execute(
                """
                SELECT
                    parsing_status,
                    COUNT(*) as count,
                    AVG(page_count) as avg_pages,
                    AVG(markdown_length) as avg_length
                FROM parsing_results
                GROUP BY parsing_status
                """
            ).fetchall()

            summary = {}
            for row in result:
                summary[row[0]] = {
                    "count": row[1],
                    "avg_pages": round(row[2], 1) if row[2] else None,
                    "avg_length": round(row[3], 1) if row[3] else None,
                }

            return summary
