#!/usr/bin/env python3
"""
Import SQLite plans.db tables directly into PostgreSQL.

Tables imported:
- node_plan

Usage:
    python import_plans_db.py [--clear]

    --clear: Truncate existing tables before import (recommended for clean import)

Requires environment variables (via .env or environment):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import argparse
import os
import sys
import sqlite3
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# Path to SQLite database
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_PATH = os.path.join(SCRIPT_DIR, "..", "db", "plans.db")

BATCH_SIZE = 5000


def get_pg_conn():
    """Create PostgreSQL connection."""
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "statsdb"),
        user=os.getenv("POSTGRES_USER", "devuser"),
        password=os.getenv("POSTGRES_PASSWORD", "devpass"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
    )


def get_sqlite_conn():
    """Create SQLite connection."""
    if not os.path.isfile(SQLITE_DB_PATH):
        print(f"Error: SQLite database not found at {SQLITE_DB_PATH}")
        sys.exit(1)
    return sqlite3.connect(SQLITE_DB_PATH)


def import_node_plan(sqlite_conn, pg_conn):
    """Import node_plan table in batches."""
    print("Importing node_plan...")

    sqlite_cur = sqlite_conn.cursor()
    sqlite_cur.execute("SELECT COUNT(*) FROM node_plan")
    total_rows = sqlite_cur.fetchone()[0]
    print(f"  Total rows to import: {total_rows}")

    pg_cur = pg_conn.cursor()
    imported = 0

    sqlite_cur.execute("""
        SELECT id, org_name, node_id, json_import_file_id, start_at, stop_at,
               invoice_amount, usd_per_hour, gpu_class_id, ram, cpu
        FROM node_plan
    """)

    while True:
        rows = sqlite_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        execute_values(
            pg_cur,
            """
            INSERT INTO node_plan (id, org_name, node_id, json_import_file_id, start_at, stop_at,
                                   invoice_amount, usd_per_hour, gpu_class_id, ram, cpu)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                org_name = EXCLUDED.org_name,
                node_id = EXCLUDED.node_id,
                json_import_file_id = EXCLUDED.json_import_file_id,
                start_at = EXCLUDED.start_at,
                stop_at = EXCLUDED.stop_at,
                invoice_amount = EXCLUDED.invoice_amount,
                usd_per_hour = EXCLUDED.usd_per_hour,
                gpu_class_id = EXCLUDED.gpu_class_id,
                ram = EXCLUDED.ram,
                cpu = EXCLUDED.cpu
            """,
            rows
        )
        pg_conn.commit()

        imported += len(rows)
        print(f"  Progress: {imported}/{total_rows} ({100*imported//total_rows}%)")

    # Update sequence
    pg_cur.execute("""
        SELECT setval('node_plan_id_seq', (SELECT COALESCE(MAX(id), 1) FROM node_plan))
    """)
    pg_conn.commit()

    print(f"  Imported {imported} rows")


def run_migration(pg_conn):
    """Run the migration to create tables if they don't exist."""
    migration_path = os.path.join(SCRIPT_DIR, "..", "db", "migrations", "002_plans_tables.sql")

    if not os.path.isfile(migration_path):
        print(f"Warning: Migration file not found at {migration_path}")
        return

    print("Running migration 002_plans_tables.sql...")
    with open(migration_path, 'r') as f:
        sql = f.read()

    pg_cur = pg_conn.cursor()
    pg_cur.execute(sql)
    pg_conn.commit()
    print("  Migration complete")


def clear_tables(pg_conn):
    """Truncate node_plan table."""
    print("Clearing existing data...")
    pg_cur = pg_conn.cursor()
    pg_cur.execute("TRUNCATE node_plan RESTART IDENTITY CASCADE")
    pg_conn.commit()
    print("  Tables cleared")


def main():
    parser = argparse.ArgumentParser(description="Import SQLite plans.db to PostgreSQL")
    parser.add_argument("--clear", action="store_true", help="Clear existing tables before import")
    args = parser.parse_args()

    print("=" * 50)
    print("SQLite to PostgreSQL Import: plans.db")
    print("=" * 50)
    print(f"SQLite source: {SQLITE_DB_PATH}")
    print()

    # Connect to databases
    sqlite_conn = get_sqlite_conn()
    pg_conn = get_pg_conn()

    print(f"Connected to PostgreSQL at {os.getenv('POSTGRES_HOST', 'localhost')}")
    print()

    # Run migration first
    run_migration(pg_conn)
    print()

    # Clear tables if requested
    if args.clear:
        clear_tables(pg_conn)
        print()

    # Import node_plan table
    import_node_plan(sqlite_conn, pg_conn)

    # Close connections
    sqlite_conn.close()
    pg_conn.close()

    print()
    print("=" * 50)
    print("Import complete!")
    print("=" * 50)


if __name__ == "__main__":
    main()
