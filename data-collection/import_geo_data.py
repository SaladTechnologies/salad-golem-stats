#!/usr/bin/env python3
"""
Import geographic data from JSON file to PostgreSQL.

Usage:
    python import_geo_data.py input_file [--clear] [--dry-run] [--tables table1,table2]

    input_file: Path to JSON file exported by export_geo_data.py
    --clear: Truncate specified tables before import
    --dry-run: Show what would be imported without actually doing it
    --tables: Import only specified tables (default: all tables in file)

Imports geographic data from backup or transfer files.

Requires environment variables (via .env or environment):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import argparse
import json
import os
import sys
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()


def get_db_conn():
    """Create PostgreSQL connection."""
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "statsdb"),
        user=os.getenv("POSTGRES_USER", "devuser"),
        password=os.getenv("POSTGRES_PASSWORD", "devpass"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
    )


def load_export_file(file_path):
    """Load and validate export file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Validate structure
        if "export_metadata" not in data or "tables" not in data:
            raise ValueError("Invalid export file format")

        metadata = data["export_metadata"]

        print(f"📁 Export file: {os.path.abspath(file_path)}")
        print(f"📅 Export date: {metadata.get('timestamp', 'Unknown')}")
        print(f"🗄️  Source: {metadata.get('source_database', 'Unknown')}")
        print(f"� Total records: {metadata.get('total_records', 0)}")
        print(f"📋 Tables: {metadata.get('exported_tables', [])}")

        return data

    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON file: {e}")
        return None
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return None


def create_table_if_not_exists(cursor, table_name, schema_info):
    """Create table if it doesn't exist based on schema info."""
    if not schema_info:
        print(
            f"⚠️  No schema information available for {table_name}, skipping table creation"
        )
        return

    # Check if table exists
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        );
    """,
        (table_name,),
    )

    if cursor.fetchone()[0]:
        print(f"✅ Table {table_name} already exists")
        return

    print(f"🔨 Creating table {table_name}...")

    # Build CREATE TABLE statement from schema
    columns = schema_info.get("columns", [])
    if not columns:
        print(f"❌ No column information available for {table_name}")
        return

    column_defs = []
    primary_keys = []

    for col in columns:
        col_def = f"{col['name']} {col['type']}"
        if not col.get("nullable", True):
            col_def += " NOT NULL"
        if col.get("default"):
            col_def += f" DEFAULT {col['default']}"
        column_defs.append(col_def)

        # Common primary key patterns
        if col["name"] in ["id", f"{table_name}_id"] or col["name"].endswith("_id"):
            if not col.get("nullable", True):  # Only if NOT NULL
                primary_keys.append(col["name"])

    # Add primary key if found
    if primary_keys:
        column_defs.append(
            f"PRIMARY KEY ({', '.join(primary_keys[:1])})"
        )  # Use first potential key

    create_sql = f"""
        CREATE TABLE {table_name} (
            {', '.join(column_defs)}
        );
    """

    cursor.execute(create_sql)
    print(f"✅ Table {table_name} created")


def import_table_data(cursor, table_name, table_data, clear_table=False):
    """Import data for a single table."""
    records = table_data.get("data", [])
    schema_info = table_data.get("schema", {})

    print(f"\n📥 Processing table: {table_name}")
    print(f"   Records to import: {len(records)}")

    if not records:
        print("   ⚠️  No data to import")
        return True

    # Create table if needed
    create_table_if_not_exists(cursor, table_name, schema_info)

    # Clear table if requested
    if clear_table:
        print(f"   🗑️  Clearing existing {table_name} data...")
        cursor.execute(f"TRUNCATE {table_name} RESTART IDENTITY CASCADE;")
        print("   ✅ Table cleared")

    # Get column names from first record
    column_names = list(records[0].keys())
    print(f"   Columns: {column_names}")

    # Convert to tuples for batch insert
    values = []
    for record in records:
        values.append(tuple(record.get(col) for col in column_names))

    # Build insert query
    placeholders = ", ".join(["%s"] * len(column_names))
    columns_str = ", ".join(column_names)

    # Use simple insert (geographic data typically doesn't need upsert)
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"

    # Handle potential conflicts for tables that might have unique constraints
    if table_name in ["city_snapshots", "country_snapshots"]:
        # Add ON CONFLICT DO NOTHING to avoid duplicate key errors
        insert_sql += " ON CONFLICT DO NOTHING"

    execute_values(cursor, insert_sql, values, template=None, page_size=1000)

    print(f"   ✅ Imported {len(records)} records")
    return True


def import_geo_data(file_path, clear_tables=False, dry_run=False, target_tables=None):
    """Import geographic data from JSON file."""
    # Load export file
    data = load_export_file(file_path)
    if not data:
        return False

    tables_data = data["tables"]

    # Filter tables if specified
    if target_tables:
        tables_data = {
            name: data for name, data in tables_data.items() if name in target_tables
        }
        missing_tables = set(target_tables) - set(tables_data.keys())
        if missing_tables:
            print(f"⚠️  Tables not found in export file: {missing_tables}")

    if not tables_data:
        print("⚠️  No tables to import")
        return True

    total_records = sum(
        table_data.get("record_count", 0) for table_data in tables_data.values()
    )

    if dry_run:
        print(f"\n🔍 DRY RUN - Would import:")
        for table_name, table_data in tables_data.items():
            count = table_data.get("record_count", 0)
            print(f"   {table_name}: {count} records")

            # Show sample data
            if table_data.get("data"):
                sample = table_data["data"][0]
                print(
                    f"     Sample: {list(sample.keys())[:4]}..."
                )  # Show first 4 columns
        print(f"   Total: {total_records} records")
        return True

    try:
        conn = get_db_conn()
        cursor = conn.cursor()

        print("\nConnecting to PostgreSQL...")
        print(
            f"Database: {os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_DB', 'statsdb')}"
        )

        # Import each table
        for table_name, table_data in tables_data.items():
            success = import_table_data(cursor, table_name, table_data, clear_tables)
            if not success:
                conn.rollback()
                return False

        # Show final stats
        print(f"\n📊 Import Summary:")
        for table_name in tables_data.keys():
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            print(f"   {table_name}: {count} total records in database")

        conn.commit()
        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        if "conn" in locals():
            conn.rollback()
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Import geographic data from JSON file"
    )
    parser.add_argument("input_file", help="Input JSON file path")
    parser.add_argument(
        "--clear", action="store_true", help="Clear existing tables before import"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without doing it",
    )
    parser.add_argument(
        "--tables",
        help="Comma-separated list of tables to import (default: all tables in file)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Geographic Data Import Tool")
    print("=" * 60)

    if not os.path.exists(args.input_file):
        print(f"❌ Input file not found: {args.input_file}")
        sys.exit(1)

    # Parse target tables
    target_tables = None
    if args.tables:
        target_tables = [t.strip() for t in args.tables.split(",")]
        print(f"Target tables: {target_tables}")

    if args.clear and not args.dry_run:
        table_msg = "specified tables" if target_tables else "all geographic tables"
        response = input(
            f"⚠️  This will DELETE all existing data in {table_msg}. Continue? (y/N): "
        )
        if response.lower() != "y":
            print("Import cancelled.")
            sys.exit(0)

    success = import_geo_data(args.input_file, args.clear, args.dry_run, target_tables)

    if success:
        if args.dry_run:
            print("\n🔍 Dry run completed - no data was modified")
        else:
            print("\n🎉 Geographic data import completed successfully!")
    else:
        print("\n💥 Import failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
