#!/usr/bin/env python3
"""
Import transactions data from CSV file to PostgreSQL.

Usage:
    python import_transactions.py input_file [--clear] [--dry-run]

    input_file: Path to CSV file with transaction data
    --clear: Truncate glm_transactions table before import
    --dry-run: Show what would be imported without actually doing it

Imports transaction data from CSV exports.

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


def parse_transaction_row(row):
    """Parse CSV row into transaction data."""
    try:
        # CSV columns: id,tx_hash,block_number,block_timestamp,from_address,to_address,value_wei,value_glm,gas_used,gas_price_wei,tx_type,created_at
        return {
            "tx_hash": row[1].strip(),
            "block_number": int(row[2]) if row[2] else None,
            "block_timestamp": row[3].strip(),
            "from_address": row[4].lower().strip(),
            "to_address": row[5].lower().strip(),
            "value_wei": row[6].strip(),
            "value_glm": float(row[7]) if row[7] else 0.0,
            "gas_used": int(row[8]) if row[8] and row[8].strip() else None,
            "gas_price_wei": row[9].strip() if row[9] and row[9].strip() else None,
            "tx_type": row[10].strip(),
        }
    except (IndexError, ValueError) as e:
        raise ValueError(f"Invalid row format: {e}")


def load_csv_file(file_path):
    """Load and validate CSV file."""
    transactions = []
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
                    # Skip rows with invalid transaction hashes
                    tx_hash = row[1].strip()
                    if not tx_hash.startswith("0x") or len(tx_hash) < 66:
                        errors.append(
                            f"Row {row_num}: Invalid transaction hash: {tx_hash}"
                        )
                        continue

                    transaction = parse_transaction_row(row)

                    # Basic validation
                    if (
                        not transaction["tx_hash"]
                        or not transaction["from_address"]
                        or not transaction["to_address"]
                    ):
                        errors.append(f"Row {row_num}: Missing required fields")
                        continue

                    transactions.append(transaction)

                except ValueError as e:
                    errors.append(f"Row {row_num}: {e}")
                except Exception as e:
                    errors.append(f"Row {row_num}: Unexpected error - {e}")

        print(f"📁 CSV file: {os.path.abspath(file_path)}")
        print(f"📊 Parsed {len(transactions)} valid transactions")

        if errors:
            print(f"⚠️  {len(errors)} rows had errors")

        return transactions, errors

    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return None, None
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return None, None


def import_transactions(file_path, clear_table=False, dry_run=False):
    """Import transactions data from CSV file."""
    # Load CSV file
    transactions, errors = load_csv_file(file_path)
    if transactions is None:
        return False

    if not transactions:
        print("⚠️  No transaction data to import")
        return True

    if dry_run:
        print(f"\n🔍 DRY RUN - Would import {len(transactions)} transactions:")
        for i, tx in enumerate(transactions[:5]):  # Show first 5
            print(
                f"   {i+1}. {tx['tx_hash'][:16]}... - {tx['value_glm']:.6f} GLM ({tx['tx_type']})"
            )
        if len(transactions) > 5:
            print(f"   ... and {len(transactions) - 5} more")

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
            print("🗑️  Clearing existing transaction data...")
            cursor.execute("TRUNCATE glm_transactions RESTART IDENTITY CASCADE;")
            print("✅ Table cleared")

        # Import transactions in batches
        if transactions:
            print(f"\n📥 Importing {len(transactions)} transactions...")

            # Prepare data for batch insert
            values = []
            for tx in transactions:
                values.append(
                    (
                        tx["tx_hash"],
                        tx["block_number"],
                        tx["block_timestamp"],
                        tx["from_address"],
                        tx["to_address"],
                        tx["value_wei"],
                        tx["value_glm"],
                        tx["gas_used"],
                        tx["gas_price_wei"],
                        tx["tx_type"],
                    )
                )

            # Use upsert to handle conflicts
            insert_sql = """
                INSERT INTO glm_transactions 
                (tx_hash, block_number, block_timestamp, from_address, to_address, 
                 value_wei, value_glm, gas_used, gas_price_wei, tx_type)
                VALUES %s
                ON CONFLICT (tx_hash) DO UPDATE SET
                    block_number = EXCLUDED.block_number,
                    block_timestamp = EXCLUDED.block_timestamp,
                    from_address = EXCLUDED.from_address,
                    to_address = EXCLUDED.to_address,
                    value_wei = EXCLUDED.value_wei,
                    value_glm = EXCLUDED.value_glm,
                    gas_used = EXCLUDED.gas_used,
                    gas_price_wei = EXCLUDED.gas_price_wei,
                    tx_type = EXCLUDED.tx_type
            """

            execute_values(cursor, insert_sql, values, template=None, page_size=1000)
            print(f"✅ Imported {len(transactions)} transactions")

            # Show some stats
            cursor.execute("SELECT COUNT(*) FROM glm_transactions;")
            total_count = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT tx_type, COUNT(*), SUM(value_glm) 
                FROM glm_transactions 
                GROUP BY tx_type 
                ORDER BY COUNT(*) DESC;
            """
            )
            stats = cursor.fetchall()

            print(f"\n📊 Database now contains {total_count} transactions")
            if stats:
                print("📋 Transaction type breakdown:")
                for tx_type, count, total_glm in stats:
                    print(
                        f"   - {tx_type}: {count:,} transactions, {total_glm:,.2f} GLM"
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
    parser = argparse.ArgumentParser(
        description="Import transaction data from CSV file"
    )
    parser.add_argument("input_file", help="Input CSV file path")
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing glm_transactions table before import",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without doing it",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Transaction Import Tool")
    print("=" * 60)

    if not os.path.exists(args.input_file):
        print(f"❌ Input file not found: {args.input_file}")
        sys.exit(1)

    if args.clear and not args.dry_run:
        response = input(
            "⚠️  This will DELETE all existing transaction data. Continue? (y/N): "
        )
        if response.lower() != "y":
            print("Import cancelled.")
            sys.exit(0)

    success = import_transactions(args.input_file, args.clear, args.dry_run)

    if success:
        if args.dry_run:
            print("\n🔍 Dry run completed - no data was modified")
        else:
            print("\n🎉 Transaction import completed successfully!")
    else:
        print("\n💥 Import failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
