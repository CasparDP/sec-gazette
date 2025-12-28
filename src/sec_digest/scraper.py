"""SEC News Digest scraper for downloading PDF files."""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List
import httpx
import duckdb
from pydantic import BaseModel


class DigestManifest(BaseModel):
    """Manifest entry for a digest download."""
    url: str
    year: int
    date: str  # ISO format: YYYY-MM-DD
    local_path: str
    download_status: str = "pending"  # pending|completed|failed|skipped
    file_size_bytes: Optional[int] = None
    downloaded_at: Optional[str] = None
    error_message: Optional[str] = None


class SECDigestScraper:
    """Scraper for SEC News Digest PDFs."""

    BASE_URL = "https://www.sec.gov/news/digest"
    USER_AGENT = "SEC Digest Research Project academic-research@example.com"

    def __init__(
        self,
        output_dir: Path,
        db_path: Path,
        delay_seconds: int = 2,
        max_retries: int = 3,
    ):
        """Initialize the scraper.

        Args:
            output_dir: Directory to save downloaded PDFs
            db_path: Path to DuckDB database for manifest
            delay_seconds: Delay between requests (default: 2)
            max_retries: Maximum retry attempts (default: 3)
        """
        self.output_dir = Path(output_dir)
        self.db_path = Path(db_path)
        self.delay_seconds = delay_seconds
        self.max_retries = max_retries

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._init_database()

    def _init_database(self) -> None:
        """Initialize DuckDB database and create manifest table."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS download_manifest (
                    url VARCHAR PRIMARY KEY,
                    year INTEGER,
                    date DATE,
                    local_path VARCHAR,
                    download_status VARCHAR,
                    file_size_bytes BIGINT,
                    downloaded_at TIMESTAMP,
                    error_message VARCHAR
                )
            """)

    def generate_urls_for_year(self, year: int) -> List[DigestManifest]:
        """Generate all potential PDF URLs for a given year.

        SEC News Digest is published on business days (weekdays).
        We generate all dates and let the downloader handle 404s.

        Args:
            year: Year to generate URLs for (1956-2014)

        Returns:
            List of DigestManifest entries
        """
        manifests = []

        # Generate all dates in the year
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
        current_date = start_date

        while current_date <= end_date:
            # Format: digMMDDYY.pdf (e.g., dig092884.pdf)
            month = current_date.strftime("%m")
            day = current_date.strftime("%d")
            year_2digit = current_date.strftime("%y")

            filename = f"dig{month}{day}{year_2digit}.pdf"
            url = f"{self.BASE_URL}/{year}/{filename}"

            # Create local path: data/raw/YYYY/digest_YYYY-MM-DD.pdf
            date_str = current_date.strftime("%Y-%m-%d")
            local_path = self.output_dir / str(year) / f"digest_{date_str}.pdf"

            manifests.append(
                DigestManifest(
                    url=url,
                    year=year,
                    date=date_str,
                    local_path=str(local_path),
                )
            )

            current_date += timedelta(days=1)

        return manifests

    def save_manifest_to_db(self, manifests: List[DigestManifest]) -> None:
        """Save manifest entries to database (insert or ignore if exists).

        Args:
            manifests: List of manifest entries to save
        """
        with duckdb.connect(str(self.db_path)) as conn:
            for manifest in manifests:
                # Check if already exists
                result = conn.execute(
                    "SELECT url FROM download_manifest WHERE url = ?",
                    [manifest.url],
                ).fetchone()

                if not result:
                    conn.execute(
                        """
                        INSERT INTO download_manifest
                        (url, year, date, local_path, download_status)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        [
                            manifest.url,
                            manifest.year,
                            manifest.date,
                            manifest.local_path,
                            manifest.download_status,
                        ],
                    )

    async def download_pdf(
        self, manifest: DigestManifest, client: httpx.AsyncClient
    ) -> DigestManifest:
        """Download a single PDF file.

        Args:
            manifest: Manifest entry for the PDF
            client: HTTP client instance

        Returns:
            Updated manifest entry with download status
        """
        local_path = Path(manifest.local_path)

        # Skip if already downloaded
        if local_path.exists():
            manifest.download_status = "skipped"
            manifest.file_size_bytes = local_path.stat().st_size
            return manifest

        # Create directory
        local_path.parent.mkdir(parents=True, exist_ok=True)

        headers = {
            "User-Agent": self.USER_AGENT,
            "Accept": "application/pdf,*/*",
        }

        for attempt in range(self.max_retries):
            try:
                response = await client.get(
                    manifest.url, headers=headers, timeout=30.0, follow_redirects=True
                )

                if response.status_code == 200:
                    # Save PDF
                    local_path.write_bytes(response.content)
                    manifest.download_status = "completed"
                    manifest.file_size_bytes = len(response.content)
                    manifest.downloaded_at = datetime.now().isoformat()
                    return manifest

                elif response.status_code == 404:
                    # Not found - likely weekend/holiday
                    manifest.download_status = "failed"
                    manifest.error_message = "404 Not Found (likely weekend/holiday)"
                    return manifest

                else:
                    # Other error
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.delay_seconds * 2)
                        continue
                    else:
                        manifest.download_status = "failed"
                        manifest.error_message = f"HTTP {response.status_code}"
                        return manifest

            except Exception as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.delay_seconds * 2)
                    continue
                else:
                    manifest.download_status = "failed"
                    manifest.error_message = str(e)
                    return manifest

        return manifest

    async def download_year(self, year: int, max_concurrent: int = 3) -> dict:
        """Download all PDFs for a given year.

        Args:
            year: Year to download (1956-2014)
            max_concurrent: Maximum concurrent downloads (default: 3)

        Returns:
            Summary statistics
        """
        print(f"\nGenerating URLs for year {year}...")
        manifests = self.generate_urls_for_year(year)
        print(f"Generated {len(manifests)} potential URLs")

        # Save to database
        print("Saving manifest to database...")
        self.save_manifest_to_db(manifests)

        # Filter out URLs already marked as failed (404s from previous runs)
        print("Checking for previously failed downloads...")
        with duckdb.connect(str(self.db_path)) as conn:
            failed_urls = set()
            result = conn.execute(
                """
                SELECT url FROM download_manifest
                WHERE year = ? AND download_status = 'failed'
                """,
                [year],
            ).fetchall()
            failed_urls = {row[0] for row in result}

        # Filter manifests to exclude already-failed URLs
        original_count = len(manifests)
        manifests = [m for m in manifests if m.url not in failed_urls]
        skipped_failed = original_count - len(manifests)

        if skipped_failed > 0:
            print(f"Skipping {skipped_failed} URLs already marked as failed (404s)")

        # Download PDFs
        print(f"Starting downloads (max {max_concurrent} concurrent)...")
        stats = {
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "skipped_failed": skipped_failed,
            "total": original_count,
        }

        async with httpx.AsyncClient() as client:
            semaphore = asyncio.Semaphore(max_concurrent)

            async def download_with_semaphore(manifest):
                async with semaphore:
                    result = await self.download_pdf(manifest, client)
                    await asyncio.sleep(self.delay_seconds)
                    return result

            # Process in batches to show progress
            batch_size = 10
            for i in range(0, len(manifests), batch_size):
                batch = manifests[i : i + batch_size]
                results = await asyncio.gather(
                    *[download_with_semaphore(m) for m in batch]
                )

                # Update database with results
                with duckdb.connect(str(self.db_path)) as conn:
                    for result in results:
                        conn.execute(
                            """
                            UPDATE download_manifest
                            SET download_status = ?,
                                file_size_bytes = ?,
                                downloaded_at = ?,
                                error_message = ?
                            WHERE url = ?
                            """,
                            [
                                result.download_status,
                                result.file_size_bytes,
                                result.downloaded_at,
                                result.error_message,
                                result.url,
                            ],
                        )

                        # Update stats
                        stats[result.download_status] += 1

                # Show progress
                progress = min(i + batch_size, len(manifests))
                print(
                    f"Progress: {progress}/{len(manifests)} "
                    f"(Completed: {stats['completed']}, "
                    f"Failed: {stats['failed']}, "
                    f"Skipped: {stats['skipped']})"
                )

        return stats

    def get_manifest_summary(self) -> dict:
        """Get summary of downloads from database.

        Returns:
            Summary statistics by status
        """
        with duckdb.connect(str(self.db_path)) as conn:
            result = conn.execute(
                """
                SELECT
                    download_status,
                    COUNT(*) as count,
                    SUM(file_size_bytes) as total_bytes
                FROM download_manifest
                GROUP BY download_status
                """
            ).fetchall()

            summary = {}
            for row in result:
                summary[row[0]] = {
                    "count": row[1],
                    "total_bytes": row[2] or 0,
                }

            return summary
