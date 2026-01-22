#!/usr/bin/env python3
"""
Import node plans data from CSV file to PostgreSQL.

Usage:
    python import_node_plans.py input_file [--clear] [--dry-run]

    input_file: Path to CSV file with node plan data
    --clear: Truncate node_plan table before import
    --dry-run: Show what would be imported without actually doing it

Imports node plan data from CSV exports.

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


def ensure_json_import_file_records(cursor, json_import_file_ids, input_file_path):
    """Ensure json_import_file records exist for the given IDs."""
    if not json_import_file_ids:
        return

    # Get existing IDs
    cursor.execute(
        "SELECT id FROM json_import_file WHERE id = ANY(%s)",
        (list(json_import_file_ids),),
    )
    existing_ids = {row[0] for row in cursor.fetchall()}

    # Create missing records
    missing_ids = json_import_file_ids - existing_ids
    if missing_ids:
        print(
            f"📝 Creating {len(missing_ids)} missing json_import_file records: {sorted(missing_ids)}"
        )

        # Prepare values for batch insert
        base_filename = os.path.basename(input_file_path)
        values = []
        for file_id in sorted(missing_ids):
            if file_id == 0:
                filename = f"unknown_import.csv"
            else:
                filename = f"{base_filename}_batch_{file_id}"
            values.append((file_id, filename))

        # Insert with explicit ID values
        insert_sql = "INSERT INTO json_import_file (id, file_name) VALUES %s ON CONFLICT (id) DO NOTHING"
        execute_values(cursor, insert_sql, values, template=None, page_size=1000)

        print(f"✅ Created {len(missing_ids)} json_import_file records")


def parse_plan_row(row):
    """Parse CSV row into node plan data."""
    try:
        # CSV columns: id,org_name,node_id,json_import_file_id,start_at,stop_at,invoice_amount,usd_per_hour,gpu_class_id,ram,cpu
        return {
            "org_name": row[1].strip() if row[1] else None,
            "node_id": row[2].strip() if row[2] else None,
            "json_import_file_id": int(row[3]) if row[3] and row[3].strip() else None,
            "start_at": int(row[4]) if row[4] and row[4].strip() else None,
            "stop_at": int(row[5]) if row[5] and row[5].strip() else None,
            "invoice_amount": float(row[6]) if row[6] and row[6].strip() else None,
            "usd_per_hour": float(row[7]) if row[7] and row[7].strip() else None,
            "gpu_class_id": row[8].strip() if row[8] and row[8].strip() else None,
            "ram": float(row[9]) if row[9] and row[9].strip() else None,
            "cpu": float(row[10]) if row[10] and row[10].strip() else None,
        }
    except (IndexError, ValueError) as e:
        raise ValueError(f"Invalid row format: {e}")


def load_csv_file(file_path):
    """Load and validate CSV file."""
    plans = []
    errors = []

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)

            for row_num, row in enumerate(reader, 1):
                # Skip empty rows
                if not row or len(row) < 11:
                    if any(
                        cell.strip() for cell in row
                    ):  # Only report if row has content
                        errors.append(
                            f"Row {row_num}: Incomplete row (expected 11+ columns, got {len(row)})"
                        )
                    continue

                try:
                    plan = parse_plan_row(row)

                    # Basic validation
                    if not plan["node_id"]:
                        errors.append(f"Row {row_num}: Missing required node_id")
                        continue

                    # Validate timestamps if present
                    if plan["start_at"] and plan["stop_at"]:
                        if plan["start_at"] >= plan["stop_at"]:
                            errors.append(
                                f"Row {row_num}: Invalid time range (start_at >= stop_at)"
                            )
                            continue

                    plans.append(plan)

                except ValueError as e:
                    errors.append(f"Row {row_num}: {e}")
                except Exception as e:
                    errors.append(f"Row {row_num}: Unexpected error - {e}")

        print(f"📁 CSV file: {os.path.abspath(file_path)}")
        print(f"📊 Parsed {len(plans)} valid node plans")

        if errors:
            print(f"⚠️  {len(errors)} rows had errors")

        return plans, errors

    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return None, None
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return None, None


def import_node_plans(file_path, clear_table=False, dry_run=False):
    """Import node plans data from CSV file."""
    # Load CSV file
    plans, errors = load_csv_file(file_path)
    if plans is None:
        return False

    if not plans:
        print("⚠️  No node plan data to import")
        return True

    if dry_run:
        print(f"\n🔍 DRY RUN - Would import {len(plans)} node plans:")
        for i, plan in enumerate(plans[:5]):  # Show first 5
            duration_hours = 0
            if plan["start_at"] and plan["stop_at"]:
                duration_hours = (plan["stop_at"] - plan["start_at"]) / (1000 * 60 * 60)

            print(
                f"   {i+1}. {plan['org_name']}/{plan['node_id'][:8]}... - "
                f"${plan['invoice_amount']:.2f} ({duration_hours:.1f}h) GPU: {plan['gpu_class_id'][:8] if plan['gpu_class_id'] else 'N/A'}..."
            )
        if len(plans) > 5:
            print(f"   ... and {len(plans) - 5} more")

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
            print("🗑️  Clearing existing node plan data...")
            cursor.execute("TRUNCATE node_plan RESTART IDENTITY CASCADE;")
            print("✅ Table cleared")

        # Import plans in batches
        if plans:
            print(f"\n📥 Importing {len(plans)} node plans...")

            # Collect all json_import_file_ids that will be referenced
            json_import_file_ids = {
                plan["json_import_file_id"]
                for plan in plans
                if plan["json_import_file_id"] is not None
            }

            # Ensure all referenced json_import_file records exist
            if json_import_file_ids:
                ensure_json_import_file_records(cursor, json_import_file_ids, file_path)

            # Prepare data for batch insert
            values = []
            for plan in plans:
                values.append(
                    (
                        plan["org_name"],
                        plan["node_id"],
                        plan["json_import_file_id"],
                        plan["start_at"],
                        plan["stop_at"],
                        plan["invoice_amount"],
                        plan["usd_per_hour"],
                        plan["gpu_class_id"],
                        plan["ram"],
                        plan["cpu"],
                    )
                )

            # Use upsert to handle conflicts (node_plan has auto-incrementing primary key)
            insert_sql = """
                INSERT INTO node_plan 
                (org_name, node_id, json_import_file_id, start_at, stop_at, 
                 invoice_amount, usd_per_hour, gpu_class_id, ram, cpu)
                VALUES %s
                ON CONFLICT DO NOTHING
            """

            execute_values(cursor, insert_sql, values, template=None, page_size=1000)
            print(f"✅ Imported {len(plans)} node plans")

            # Show some stats
            cursor.execute("SELECT COUNT(*) FROM node_plan;")
            total_count = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT 
                    org_name, 
                    COUNT(*) as plan_count,
                    SUM(invoice_amount) as total_amount,
                    AVG(usd_per_hour) as avg_hourly_rate
                FROM node_plan 
                WHERE org_name IS NOT NULL
                GROUP BY org_name 
                ORDER BY plan_count DESC
                LIMIT 10;
            """
            )
            org_stats = cursor.fetchall()

            cursor.execute(
                """
                SELECT 
                    gpu_class_id, 
                    COUNT(*) as plan_count,
                    SUM(invoice_amount) as total_amount,
                    AVG(usd_per_hour) as avg_hourly_rate
                FROM node_plan 
                WHERE gpu_class_id IS NOT NULL AND gpu_class_id != ''
                GROUP BY gpu_class_id 
                ORDER BY plan_count DESC
                LIMIT 5;
            """
            )
            gpu_stats = cursor.fetchall()

            print(f"\n📊 Database now contains {total_count} node plans")

            if org_stats:
                print("\n📋 Top organizations by plan count:")
                for org, count, total, avg_rate in org_stats:
                    print(
                        f"   - {org}: {count:,} plans, ${total:,.2f} total, ${avg_rate:.3f}/hr avg"
                    )

            if gpu_stats:
                print("\n🖥️  GPU class breakdown:")
                for gpu_class, count, total, avg_rate in gpu_stats:
                    gpu_short = (
                        gpu_class[:12] + "..." if len(gpu_class) > 15 else gpu_class
                    )
                    print(
                        f"   - {gpu_short}: {count:,} plans, ${total:,.2f} total, ${avg_rate:.3f}/hr avg"
                    )

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
    parser = argparse.ArgumentParser(description="Import node plan data from CSV file")
    parser.add_argument("input_file", help="Input CSV file path")
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing node_plan table before import",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without doing it",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Node Plan Import Tool")
    print("=" * 60)

    if not os.path.exists(args.input_file):
        print(f"❌ Input file not found: {args.input_file}")
        sys.exit(1)

    if args.clear and not args.dry_run:
        response = input(
            "⚠️  This will DELETE all existing node plan data. Continue? (y/N): "
        )
        if response.lower() != "y":
            print("Import cancelled.")
            sys.exit(0)

    success = import_node_plans(args.input_file, args.clear, args.dry_run)

    if success:
        if args.dry_run:
            print("\n🔍 Dry run completed - no data was modified")
        else:
            print("\n🎉 Node plan import completed successfully!")
    else:
        print("\n💥 Import failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
