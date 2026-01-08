#!/usr/bin/env python3
"""
Import city geographic data from JSON file to PostgreSQL.

Usage:
    python import_geo_data.py input_file [--clear]

Simple script to import city data from JSON backup files.
"""

import argparse
import json
import os
import sys
from shared_geo_db import save_geo_data_to_database


def import_city_data_from_json(file_path, clear_existing=True):
    """
    Load city data from JSON file and import to database.

    Args:
        file_path: Path to JSON export file
        clear_existing: Whether to clear existing city data before import

    Returns:
        Number of records imported, or 0 if failed
    """
    # Load JSON file
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return 0
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON file: {e}")
        return 0
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return 0

    # Extract city data based on export file structure
    city_data = None

    # Handle different possible JSON structures
    if "tables" in data and "city_snapshots" in data["tables"]:
        # New export format with tables structure
        city_data = data["tables"]["city_snapshots"]["data"]
    elif "data" in data:
        # Simple export format with direct data array
        city_data = data["data"]
    elif isinstance(data, list):
        # Direct array of city records
        city_data = data

    if not city_data:
        print("❌ No city data found in JSON file")
        return 0

    print(f"📍 Found {len(city_data)} city records")

    # Import using shared database function
    try:
        imported_count = save_geo_data_to_database(
            city_data=city_data, clear_existing=clear_existing, use_bulk_insert=True
        )
        print(f"✅ Successfully imported {imported_count} city records")
        return imported_count
    except Exception as e:
        print(f"❌ Database import failed: {e}")
        return 0


def main():
    parser = argparse.ArgumentParser(description="Import city data from JSON file")
    parser.add_argument("input_file", help="Input JSON file path")
    parser.add_argument(
        "--clear", action="store_true", help="Clear existing city data before import"
    )

    args = parser.parse_args()

    print("=" * 50)
    print("City Data Import Tool")
    print("=" * 50)

    if not os.path.exists(args.input_file):
        print(f"❌ Input file not found: {args.input_file}")
        sys.exit(1)

    if args.clear:
        response = input(
            "⚠️  This will DELETE all existing city data. Continue? (y/N): "
        )
        if response.lower() != "y":
            print("Import cancelled.")
            sys.exit(0)

    imported = import_city_data_from_json(args.input_file, args.clear)

    if imported > 0:
        print(f"\n🎉 Import completed successfully! ({imported} records)")
    else:
        print("\n💥 Import failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
