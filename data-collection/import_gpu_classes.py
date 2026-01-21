#!/usr/bin/env python3
"""
Import GPU classes data from CSV file to PostgreSQL.

Usage:
    python import_gpu_classes.py input_file [--clear] [--dry-run]

    input_file: Path to CSV file with GPU classes data
    --clear: Truncate gpu_classes table before import
    --dry-run: Show what would be imported without actually doing it

Imports GPU classes data from CSV files.

Requires environment variables (via .env or environment):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import argparse
import csv
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


def parse_gpu_row(row):
    """Parse CSV row into GPU classes data."""
    try:
        # CSV columns: gpu_class_id,batch_price,low_price,medium_price,high_price,gpu_type,gpu_class_name,vram_gb
        return {
            "gpu_class_id": row[0].strip(),
            "batch_price": float(row[1]) if row[1] and row[1].strip() else None,
            "low_price": float(row[2]) if row[2] and row[2].strip() else None,
            "medium_price": float(row[3]) if row[3] and row[3].strip() else None,
            "high_price": float(row[4]) if row[4] and row[4].strip() else None,
            "gpu_type": row[5].strip() if row[5] and row[5].strip() else None,
            "gpu_class_name": row[6].strip() if row[6] else None,
            "vram_gb": int(row[7]) if row[7] and row[7].strip() else None,
        }
    except (IndexError, ValueError) as e:
        raise ValueError(f"Invalid row format: {e}")


def load_csv_file(file_path):
    """Load and validate CSV file."""
    gpu_classes = []
    errors = []

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)

            for row_num, row in enumerate(reader, 1):
                # Skip empty rows
                if not row or len(row) < 8:
                    if any(
                        cell.strip() for cell in row
                    ):  # Only report if row has content
                        errors.append(
                            f"Row {row_num}: Incomplete row (expected 8 columns, got {len(row)})"
                        )
                    continue

                try:
                    gpu = parse_gpu_row(row)

                    # Basic validation
                    if not gpu["gpu_class_id"]:
                        errors.append(f"Row {row_num}: Missing required gpu_class_id")
                        continue

                    gpu_classes.append(gpu)

                except ValueError as e:
                    errors.append(f"Row {row_num}: {e}")
                except Exception as e:
                    errors.append(f"Row {row_num}: Unexpected error - {e}")

        print(f"📁 CSV file: {os.path.abspath(file_path)}")
        print(f"📊 Parsed {len(gpu_classes)} valid GPU classes")

        if errors:
            print(f"⚠️  {len(errors)} rows had errors")

        return gpu_classes, errors

    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return None, None
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return None, None


def import_gpu_classes(file_path, clear_table=False, dry_run=False):
    """Import GPU classes data from CSV file."""
    # Load CSV file
    gpu_classes, errors = load_csv_file(file_path)
    if gpu_classes is None:
        return False

    if not gpu_classes:
        print("⚠️  No GPU classes data to import")
        return True

    if dry_run:
        print(f"\n🔍 DRY RUN - Would import {len(gpu_classes)} GPU classes:")
        for i, gpu in enumerate(gpu_classes[:5]):  # Show first 5
            vram = f"{gpu['vram_gb']}GB" if gpu['vram_gb'] else "N/A"
            price = f"${gpu['medium_price']:.3f}" if gpu['medium_price'] else "N/A"
            print(
                f"   {i+1}. {gpu['gpu_class_name']} - {vram} - {price}"
            )
        if len(gpu_classes) > 5:
            print(f"   ... and {len(gpu_classes) - 5} more")
            
        if errors:
            print(f"\n⚠️  Would skip {len(errors)} rows with errors:")
            for error in errors[:3]:
                print(f"   {error}")
            if len(errors) > 3:
                print(f"   ... and {len(errors) - 3} more errors")
                
        return True

    try:
        conn = get_db_conn()
        cursor = conn.cursor()

        print("\nConnecting to PostgreSQL...")
        print(
            f"Database: {os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_DB', 'statsdb')}"
        )

        # Clear table if requested
        if clear_table:
            print("🗑️  Clearing existing gpu_classes data...")
            cursor.execute("TRUNCATE gpu_classes RESTART IDENTITY CASCADE;")
            print("✅ Table cleared")

        # Import GPU classes in batches
        if gpu_classes:
            print(f"\n📥 Importing {len(gpu_classes)} GPU classes...")

            # Prepare data for batch insert
            values = []
            for gpu in gpu_classes:
                values.append(
                    (
                        gpu["gpu_class_id"],
                        gpu["batch_price"],
                        gpu["low_price"],
                        gpu["medium_price"],
                        gpu["high_price"],
                        gpu["gpu_type"],
                        gpu["gpu_class_name"],
                        gpu["vram_gb"],
                    )
                )

            # Use upsert to handle conflicts
            insert_sql = """
                INSERT INTO gpu_classes 
                (gpu_class_id, batch_price, low_price, medium_price, high_price, 
                 gpu_type, gpu_class_name, vram_gb)
                VALUES %s
                ON CONFLICT (gpu_class_id) DO UPDATE SET
                    batch_price = EXCLUDED.batch_price,
                    low_price = EXCLUDED.low_price,
                    medium_price = EXCLUDED.medium_price,
                    high_price = EXCLUDED.high_price,
                    gpu_type = EXCLUDED.gpu_type,
                    gpu_class_name = EXCLUDED.gpu_class_name,
                    vram_gb = EXCLUDED.vram_gb
            """

            execute_values(cursor, insert_sql, values, template=None, page_size=1000)
            print(f"✅ Imported {len(gpu_classes)} GPU classes")

            # Show some stats
            cursor.execute("SELECT COUNT(*) FROM gpu_classes;")
            total_count = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT gpu_class_name, vram_gb, medium_price, gpu_type
                FROM gpu_classes 
                WHERE gpu_class_name IS NOT NULL 
                ORDER BY vram_gb DESC NULLS LAST, medium_price DESC NULLS LAST
                LIMIT 5;
            """
            )
            samples = cursor.fetchall()

            cursor.execute(
                """
                SELECT gpu_type, COUNT(*), AVG(medium_price) as avg_price
                FROM gpu_classes 
                WHERE gpu_type IS NOT NULL
                GROUP BY gpu_type 
                ORDER BY COUNT(*) DESC;
            """
            )
            type_stats = cursor.fetchall()

            print(f"\n📊 Database now contains {total_count} GPU classes")
            
            if samples:
                print("📋 Top GPU classes by VRAM and price:")
                for name, vram, price, gpu_type in samples:
                    vram_str = f"{vram}GB" if vram else "Unknown"
                    price_str = f"${price:.3f}" if price else "N/A"
                    type_str = gpu_type if gpu_type else "N/A"
                    print(f"   - {name} ({vram_str}) - {price_str} [{type_str}]")

            if type_stats:
                print("\n🏷️  GPU type breakdown:")
                for gpu_type, count, avg_price in type_stats:
                    avg_str = f"${avg_price:.3f}" if avg_price else "N/A"
                    print(f"   - {gpu_type}: {count} classes, {avg_str} avg price")

        if errors:
            print(f"\n⚠️  Skipped {len(errors)} rows with errors:")
            for error in errors[:5]:
                print(f"   {error}")
            if len(errors) > 5:
                print(f"   ... and {len(errors) - 5} more")

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
        description="Import GPU classes data from CSV file"
    )
    parser.add_argument("input_file", help="Input CSV file path")
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