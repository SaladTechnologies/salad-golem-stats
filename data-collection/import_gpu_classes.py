#!/usr/bin/env python3
"""
Import GPU classes data from JSON file to PostgreSQL.

Usage:
    python import_gpu_classes.py input_file [--clear] [--dry-run]

    input_file: Path to JSON file exported by export_gpu_classes.py
    --clear: Truncate gpu_classes table before import
    --dry-run: Show what would be imported without actually doing it

Imports GPU classes data from backup or transfer files.

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
        if "export_metadata" not in data or "data" not in data:
            raise ValueError("Invalid export file format")

        metadata = data["export_metadata"]

        print(f"📁 Export file: {os.path.abspath(file_path)}")
        print(f"📅 Export date: {metadata.get('timestamp', 'Unknown')}")
        print(f"🗄️  Source: {metadata.get('source_database', 'Unknown')}")
        print(f"� Records: {metadata.get('record_count', len(data['data']))}")

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


def create_table_if_not_exists(cursor, schema_info):
    """Create gpu_classes table if it doesn't exist based on schema info."""
    if not schema_info:
        print("⚠️  No schema information available, skipping table creation")
        return

    # Check if table exists
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'gpu_classes'
        );
    """
    )

    if cursor.fetchone()[0]:
        print("✅ Table gpu_classes already exists")
        return

    print("🔨 Creating gpu_classes table...")

    # Build CREATE TABLE statement from schema
    columns = schema_info.get("columns", [])
    if not columns:
        print("❌ No column information available")
        return

    column_defs = []
    for col in columns:
        col_def = f"{col['name']} {col['type']}"
        if not col.get("nullable", True):
            col_def += " NOT NULL"
        if col.get("default"):
            col_def += f" DEFAULT {col['default']}"
        column_defs.append(col_def)

    # Add primary key if gpu_class_id exists
    if any(col["name"] == "gpu_class_id" for col in columns):
        column_defs.append("PRIMARY KEY (gpu_class_id)")

    create_sql = f"""
        CREATE TABLE gpu_classes (
            {', '.join(column_defs)}
        );
    """

    cursor.execute(create_sql)
    print("✅ Table gpu_classes created")


def import_gpu_classes(file_path, clear_table=False, dry_run=False):
    """Import GPU classes data from JSON file."""
    # Load export file
    data = load_export_file(file_path)
    if not data:
        return False

    gpu_classes = data["data"]
    schema_info = data["export_metadata"].get("schema", {})

    if not gpu_classes:
        print("⚠️  No GPU classes data to import")
        return True

    if dry_run:
        print(f"\n🔍 DRY RUN - Would import {len(gpu_classes)} GPU classes:")
        for i, gpu in enumerate(gpu_classes[:3]):  # Show first 3
            print(
                f"   {i+1}. {gpu.get('gpu_class_name', 'Unknown')} - {gpu.get('vram_gb', 'N/A')}GB"
            )
        if len(gpu_classes) > 3:
            print(f"   ... and {len(gpu_classes) - 3} more")
        return True

    try:
        conn = get_db_conn()
        cursor = conn.cursor()

        print("\nConnecting to PostgreSQL...")
        print(
            f"Database: {os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_DB', 'statsdb')}"
        )

        # Create table if needed
        create_table_if_not_exists(cursor, schema_info)

        # Clear table if requested
        if clear_table:
            print("🗑️  Clearing existing gpu_classes data...")
            cursor.execute("TRUNCATE gpu_classes RESTART IDENTITY CASCADE;")
            print("✅ Table cleared")

        # Prepare insert data
        if gpu_classes:
            # Get column names from first record
            column_names = list(gpu_classes[0].keys())

            print(f"\n📥 Importing {len(gpu_classes)} GPU classes...")
            print(f"   Columns: {column_names}")

            # Convert to tuples for batch insert
            values = []
            for gpu in gpu_classes:
                values.append(tuple(gpu.get(col) for col in column_names))

            # Build insert query with ON CONFLICT
            placeholders = ", ".join(["%s"] * len(column_names))
            columns_str = ", ".join(column_names)

            # Use upsert if gpu_class_id column exists
            if "gpu_class_id" in column_names:
                update_clause = ", ".join(
                    [
                        f"{col} = EXCLUDED.{col}"
                        for col in column_names
                        if col != "gpu_class_id"
                    ]
                )
                insert_sql = f"""
                    INSERT INTO gpu_classes ({columns_str})
                    VALUES %s
                    ON CONFLICT (gpu_class_id) DO UPDATE SET {update_clause}
                """
            else:
                insert_sql = f"INSERT INTO gpu_classes ({columns_str}) VALUES %s"

            execute_values(cursor, insert_sql, values, template=None, page_size=1000)

            print(f"✅ Imported {len(gpu_classes)} GPU classes")

            # Show some stats
            cursor.execute("SELECT COUNT(*) FROM gpu_classes;")
            total_count = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT gpu_class_name, vram_gb 
                FROM gpu_classes 
                WHERE gpu_class_name IS NOT NULL 
                ORDER BY vram_gb DESC NULLS LAST, gpu_class_name 
                LIMIT 5;
            """
            )
            samples = cursor.fetchall()

            print(f"\n📊 Database now contains {total_count} GPU classes")
            if samples:
                print("📋 Sample GPU classes:")
                for name, vram in samples:
                    vram_str = f"{vram}GB" if vram else "Unknown"
                    print(f"   - {name} ({vram_str})")

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
        description="Import GPU classes data from JSON file"
    )
    parser.add_argument("input_file", help="Input JSON file path")
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing gpu_classes table before import",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without doing it",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("GPU Classes Import Tool")
    print("=" * 60)

    if not os.path.exists(args.input_file):
        print(f"❌ Input file not found: {args.input_file}")
        sys.exit(1)

    if args.clear and not args.dry_run:
        response = input(
            "⚠️  This will DELETE all existing GPU classes data. Continue? (y/N): "
        )
        if response.lower() != "y":
            print("Import cancelled.")
            sys.exit(0)

    success = import_gpu_classes(args.input_file, args.clear, args.dry_run)

    if success:
        if args.dry_run:
            print("\n🔍 Dry run completed - no data was modified")
        else:
            print("\n🎉 GPU classes import completed successfully!")
    else:
        print("\n💥 Import failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
