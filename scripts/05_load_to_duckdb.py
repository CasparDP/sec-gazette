"""Load extracted enforcement actions into DuckDB."""

import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from src.sec_digest.config import Config


def create_tables(con):
    """Create database schema."""

    # Main enforcement actions table
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

    # Respondents table
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

    # Violations table
    con.execute("""
        CREATE TABLE IF NOT EXISTS violations (
            id INTEGER PRIMARY KEY,
            action_id INTEGER NOT NULL,
            statute VARCHAR,
            description TEXT,
            FOREIGN KEY (action_id) REFERENCES enforcement_actions(id)
        )
    """)

    # Sanctions table
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


def load_json_files(con, extracted_dir: Path):
    """Load JSON files into database."""

    json_files = sorted(extracted_dir.glob("**/*.json"))
    print(f"Found {len(json_files)} JSON files to load")

    action_id = 1
    respondent_id = 1
    violation_id = 1
    sanction_id = 1

    for json_file in json_files:
        with open(json_file) as f:
            data = json.load(f)

        # Skip if no actions
        if not data.get("has_enforcement_actions"):
            continue

        digest_date = data["digest_date"]
        extraction_notes = data.get("extraction_notes")

        # Insert each action
        for action in data.get("actions", []):
            # Insert main action
            con.execute("""
                INSERT INTO enforcement_actions
                (id, digest_date, action_type, title, settlement, court,
                 case_number, release_number, full_text, extraction_notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                action_id,
                digest_date,
                action["action_type"],
                action.get("title"),
                action.get("settlement"),
                action.get("court"),
                action.get("case_number"),
                action.get("release_number"),
                action.get("full_text"),
                extraction_notes
            ])

            # Insert respondents
            for resp in action.get("respondents", []):
                con.execute("""
                    INSERT INTO respondents (id, action_id, name, entity_type, location)
                    VALUES (?, ?, ?, ?, ?)
                """, [
                    respondent_id,
                    action_id,
                    resp["name"],
                    resp.get("entity_type"),
                    resp.get("location")
                ])
                respondent_id += 1

            # Insert violations
            for viol in action.get("violations", []):
                con.execute("""
                    INSERT INTO violations (id, action_id, statute, description)
                    VALUES (?, ?, ?, ?)
                """, [
                    violation_id,
                    action_id,
                    viol.get("statute"),
                    viol.get("description")
                ])
                violation_id += 1

            # Insert sanctions
            for sanc in action.get("sanctions", []):
                con.execute("""
                    INSERT INTO sanctions (id, action_id, sanction_type, description, duration, amount)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [
                    sanction_id,
                    action_id,
                    sanc.get("sanction_type"),
                    sanc["description"],
                    sanc.get("duration"),
                    sanc.get("amount")
                ])
                sanction_id += 1

            action_id += 1

    print(f"Loaded {action_id - 1} enforcement actions")


def show_sample_queries(con):
    """Run and display sample queries."""

    print("\n" + "=" * 80)
    print("Sample Queries and Results")
    print("=" * 80)

    # Query 1: Overview
    print("\n1. Overview Statistics:")
    print("-" * 80)
    result = con.execute("""
        SELECT
            COUNT(*) as total_actions,
            COUNT(DISTINCT digest_date) as digest_count,
            MIN(digest_date) as earliest_date,
            MAX(digest_date) as latest_date
        FROM enforcement_actions
    """).fetchall()
    for row in result:
        print(f"Total actions: {row[0]}")
        print(f"Digest documents: {row[1]}")
        print(f"Date range: {row[2]} to {row[3]}")

    # Query 2: Action types
    print("\n2. Action Type Distribution:")
    print("-" * 80)
    result = con.execute("""
        SELECT action_type, COUNT(*) as count
        FROM enforcement_actions
        GROUP BY action_type
        ORDER BY count DESC
    """).fetchall()
    for row in result:
        print(f"{row[0].capitalize()}: {row[1]}")

    # Query 3: Top respondents
    print("\n3. Top Respondents (by number of actions):")
    print("-" * 80)
    result = con.execute("""
        SELECT
            r.name,
            r.entity_type,
            COUNT(DISTINCT r.action_id) as action_count
        FROM respondents r
        GROUP BY r.name, r.entity_type
        HAVING action_count > 1
        ORDER BY action_count DESC
        LIMIT 5
    """).fetchall()
    for row in result:
        print(f"{row[0]} ({row[1] or 'unknown'}): {row[2]} actions")

    if not result:
        print("No respondents with multiple actions in this sample")

    # Query 4: Sanction types
    print("\n4. Sanction Type Distribution:")
    print("-" * 80)
    result = con.execute("""
        SELECT sanction_type, COUNT(*) as count
        FROM sanctions
        WHERE sanction_type IS NOT NULL
        GROUP BY sanction_type
        ORDER BY count DESC
    """).fetchall()
    for row in result:
        print(f"{row[0] or 'other'}: {row[1]}")

    # Query 5: Sample enforcement action with details
    print("\n5. Sample Enforcement Action with Full Details:")
    print("-" * 80)
    result = con.execute("""
        SELECT
            ea.id,
            ea.digest_date,
            ea.action_type,
            ea.title,
            ea.settlement,
            ea.release_number
        FROM enforcement_actions ea
        WHERE ea.title IS NOT NULL
        LIMIT 1
    """).fetchone()

    if result:
        action_id = result[0]
        print(f"Action ID: {result[0]}")
        print(f"Date: {result[1]}")
        print(f"Type: {result[2]}")
        print(f"Title: {result[3]}")
        print(f"Settlement: {result[4]}")
        print(f"Release: {result[5]}")

        # Get respondents
        print("\nRespondents:")
        respondents = con.execute("""
            SELECT name, entity_type, location
            FROM respondents
            WHERE action_id = ?
        """, [action_id]).fetchall()
        for r in respondents:
            print(f"  - {r[0]} ({r[1] or 'unknown'}){f' - {r[2]}' if r[2] else ''}")

        # Get violations
        print("\nViolations:")
        violations = con.execute("""
            SELECT statute, description
            FROM violations
            WHERE action_id = ?
        """, [action_id]).fetchall()
        for v in violations:
            statute = v[0] or "Unspecified"
            desc = v[1][:100] + "..." if v[1] and len(v[1]) > 100 else v[1]
            print(f"  - {statute}")
            if desc:
                print(f"    {desc}")

        # Get sanctions
        print("\nSanctions:")
        sanctions = con.execute("""
            SELECT sanction_type, description, duration, amount
            FROM sanctions
            WHERE action_id = ?
        """, [action_id]).fetchall()
        for s in sanctions:
            desc = s[1][:80] + "..." if len(s[1]) > 80 else s[1]
            print(f"  - Type: {s[0] or 'other'}")
            print(f"    Description: {desc}")
            if s[2]:
                print(f"    Duration: {s[2]}")
            if s[3]:
                print(f"    Amount: {s[3]}")


def main():
    """Main function."""
    print("=" * 80)
    print("Loading Extracted Data into DuckDB")
    print("=" * 80)

    config = Config.load()

    # Connect to DuckDB
    db_path = config.paths.database
    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nDatabase: {db_path}")
    con = duckdb.connect(str(db_path))

    # Create schema
    print("\nCreating tables...")
    create_tables(con)

    # Clear existing data for re-import
    con.execute("DELETE FROM sanctions")
    con.execute("DELETE FROM violations")
    con.execute("DELETE FROM respondents")
    con.execute("DELETE FROM enforcement_actions")

    # Load data
    print("\nLoading JSON files...")
    extracted_dir = config.paths.extracted / "1985"
    load_json_files(con, extracted_dir)

    # Show sample queries
    show_sample_queries(con)

    print("\n" + "=" * 80)
    print("Database ready for analysis!")
    print("=" * 80)
    print(f"\nYou can query the database at: {db_path}")
    print("\nTables created:")
    print("  - enforcement_actions (main table)")
    print("  - respondents (entities involved)")
    print("  - violations (laws violated)")
    print("  - sanctions (penalties imposed)")

    con.close()


if __name__ == "__main__":
    main()
