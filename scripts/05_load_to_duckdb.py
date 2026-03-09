"""Load extracted enforcement actions into DuckDB.

Usage:
    poetry run python scripts/05_load_to_duckdb.py                  # incremental: all years
    poetry run python scripts/05_load_to_duckdb.py --year 2000      # incremental: one year
    poetry run python scripts/05_load_to_duckdb.py --full-reload    # wipe + reload everything
"""

import sys
import json
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from src.sec_digest.config import Config


def create_tables(con):
    """Create database schema if not already present."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS enforcement_actions (
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
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS respondents (
            id INTEGER PRIMARY KEY,
            action_id INTEGER NOT NULL,
            name VARCHAR NOT NULL,
            entity_type VARCHAR,
            location VARCHAR,
            FOREIGN KEY (action_id) REFERENCES enforcement_actions(id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS violations (
            id INTEGER PRIMARY KEY,
            action_id INTEGER NOT NULL,
            statute VARCHAR,
            description TEXT,
            FOREIGN KEY (action_id) REFERENCES enforcement_actions(id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS sanctions (
            id INTEGER PRIMARY KEY,
            action_id INTEGER NOT NULL,
            sanction_type VARCHAR,
            description TEXT,
            duration VARCHAR,
            amount VARCHAR,
            FOREIGN KEY (action_id) REFERENCES enforcement_actions(id)
        )
    """)


def get_next_ids(con) -> tuple[int, int, int, int]:
    """Return next available IDs for each table (max existing + 1)."""
    def next_id(table, col="id"):
        row = con.execute(f"SELECT MAX({col}) FROM {table}").fetchone()
        return (row[0] or 0) + 1
    return (
        next_id("enforcement_actions"),
        next_id("respondents"),
        next_id("violations"),
        next_id("sanctions"),
    )


def get_already_loaded_dates(con) -> set:
    """Return set of digest_date strings already in enforcement_actions."""
    rows = con.execute("SELECT DISTINCT CAST(digest_date AS VARCHAR) FROM enforcement_actions").fetchall()
    return {r[0] for r in rows}


def load_json_files(con, json_files: list, skip_dates: set) -> dict:
    """Insert records from a list of JSON files, skipping already-loaded dates."""
    action_id, respondent_id, violation_id, sanction_id = get_next_ids(con)

    stats = {"loaded": 0, "skipped_existing": 0, "no_actions": 0, "errors": 0, "total_actions": 0}

    for json_file in json_files:
        try:
            with open(json_file) as f:
                data = json.load(f)
        except Exception as e:
            print(f"  ✗ Could not read {json_file.name}: {e}")
            stats["errors"] += 1
            continue

        digest_date = data.get("digest_date", "")

        if digest_date in skip_dates:
            stats["skipped_existing"] += 1
            continue

        if not data.get("has_enforcement_actions") or not data.get("actions"):
            stats["no_actions"] += 1
            continue

        extraction_notes = data.get("extraction_notes")

        for action in data["actions"]:
            con.execute("""
                INSERT INTO enforcement_actions
                (id, digest_date, action_type, title, settlement, court,
                 case_number, release_number, full_text, extraction_notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                action_id, digest_date,
                action["action_type"],
                action.get("title"),
                action.get("settlement"),
                action.get("court"),
                action.get("case_number"),
                action.get("release_number"),
                action.get("full_text"),
                extraction_notes,
            ])

            for resp in action.get("respondents", []):
                con.execute("""
                    INSERT INTO respondents (id, action_id, name, entity_type, location)
                    VALUES (?, ?, ?, ?, ?)
                """, [respondent_id, action_id, resp["name"],
                      resp.get("entity_type"), resp.get("location")])
                respondent_id += 1

            for viol in action.get("violations", []):
                con.execute("""
                    INSERT INTO violations (id, action_id, statute, description)
                    VALUES (?, ?, ?, ?)
                """, [violation_id, action_id,
                      viol.get("statute"), viol.get("description")])
                violation_id += 1

            for sanc in action.get("sanctions", []):
                con.execute("""
                    INSERT INTO sanctions (id, action_id, sanction_type, description, duration, amount)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [sanction_id, action_id,
                      sanc.get("sanction_type"), sanc["description"],
                      sanc.get("duration"), sanc.get("amount")])
                sanction_id += 1

            action_id += 1
            stats["total_actions"] += 1

        stats["loaded"] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(description="Load extracted JSON files into DuckDB")
    parser.add_argument("--year", type=int, default=None,
                        help="Load only a specific year (default: all year folders)")
    parser.add_argument("--full-reload", action="store_true",
                        help="Wipe enforcement tables and reload everything from scratch")
    args = parser.parse_args()

    print("=" * 80)
    print("Loading Extracted Data into DuckDB")
    print("=" * 80)

    config = Config.load()
    db_path = config.paths.database
    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nDatabase: {db_path}")
    con = duckdb.connect(str(db_path))

    create_tables(con)

    if args.full_reload:
        print("\n--full-reload: clearing all enforcement tables...")
        con.execute("DELETE FROM sanctions")
        con.execute("DELETE FROM violations")
        con.execute("DELETE FROM respondents")
        con.execute("DELETE FROM enforcement_actions")

    # Collect JSON files from the appropriate year folder(s)
    extracted_root = config.paths.extracted
    if args.year:
        year_dirs = [extracted_root / str(args.year)]
    else:
        year_dirs = sorted(d for d in extracted_root.iterdir() if d.is_dir())

    json_files = []
    for year_dir in year_dirs:
        found = sorted(year_dir.glob("*.json"))
        if found:
            print(f"  {year_dir.name}/: {len(found)} JSON files")
        json_files.extend(found)

    print(f"\nTotal JSON files found: {len(json_files)}")

    if not json_files:
        print("Nothing to load. Run 04_batch_extract.py first.")
        con.close()
        return

    # Skip dates already in DB (unless full reload wiped them)
    skip_dates = get_already_loaded_dates(con)
    if skip_dates and not args.full_reload:
        print(f"Skipping {len(skip_dates)} dates already in DB (use --full-reload to replace)")

    print("\nLoading...")
    stats = load_json_files(con, json_files, skip_dates)

    print(f"\n{'=' * 80}")
    print("Summary")
    print(f"{'=' * 80}")
    print(f"  Loaded digests:       {stats['loaded']}")
    print(f"  Total actions:        {stats['total_actions']}")
    print(f"  Skipped (in DB):      {stats['skipped_existing']}")
    print(f"  Skipped (no actions): {stats['no_actions']}")
    print(f"  Errors:               {stats['errors']}")

    # Quick DB totals
    n = con.execute("SELECT COUNT(*) FROM enforcement_actions").fetchone()[0]
    print(f"\n  enforcement_actions total: {n}")

    con.close()


if __name__ == "__main__":
    main()
