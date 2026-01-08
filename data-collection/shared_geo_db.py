#!/usr/bin/env python3
"""
Shared database functions for city geographic data operations.

Contains common PostgreSQL insertion logic used by both get_geo_data.py 
and import_geo_data.py to avoid code duplication.
"""

import os
from datetime import datetime, timezone
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


def safe_float(val):
    """Safely convert value to float, return None if invalid."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def clear_geo_tables(cursor, tables=None):
    """Clear specified geographic tables."""
    if tables is None:
        tables = ["city_snapshots"]

    for table in tables:
        print(f"Clearing {table}...")
        cursor.execute(f"DELETE FROM {table}")


def insert_city_snapshots(cursor, city_data, timestamp=None):
    """Insert city snapshot data into PostgreSQL."""
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    skipped_cities = []
    inserted_count = 0

    for city_record in city_data:
        # Handle different possible key names
        city_name = (
            city_record.get("city")
            or city_record.get("city_name")
            or city_record.get("name")
        )
        count = city_record.get("count", 0)
        lat = safe_float(city_record.get("lat"))
        lon = safe_float(city_record.get("lon") or city_record.get("long"))

        if city_name and lat is not None and lon is not None:
            cursor.execute(
                """
                INSERT INTO city_snapshots (ts, name, count, lat, long)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ts, name) DO UPDATE
                SET count = EXCLUDED.count,
                    lat = EXCLUDED.lat,
                    long = EXCLUDED.long
                """,
                (timestamp, city_name, count, lat, lon),
            )
            inserted_count += 1
        else:
            skipped_cities.append(city_record)

    print(f"Inserted {inserted_count} city records")

    if skipped_cities:
        print(f"Skipped {len(skipped_cities)} cities with missing coordinates:")
        for city in skipped_cities[:5]:  # Show first 5
            print(f"  - {city}")
        if len(skipped_cities) > 5:
            print(f"  ... and {len(skipped_cities) - 5} more")

    return inserted_count


def bulk_insert_city_snapshots(cursor, city_data, timestamp=None):
    """Bulk insert city snapshot data using execute_values for better performance."""
    if not city_data:
        return 0

    if timestamp is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # Prepare data tuples, filtering out invalid records
    valid_records = []
    skipped_count = 0

    for city_record in city_data:
        city_name = (
            city_record.get("city")
            or city_record.get("city_name")
            or city_record.get("name")
        )
        count = city_record.get("count", 0)
        lat = safe_float(city_record.get("lat"))
        lon = safe_float(city_record.get("lon") or city_record.get("long"))

        if city_name and lat is not None and lon is not None:
            valid_records.append((timestamp, city_name, count, lat, lon))
        else:
            skipped_count += 1

    if valid_records:
        execute_values(
            cursor,
            """
            INSERT INTO city_snapshots (ts, name, count, lat, long)
            VALUES %s
            ON CONFLICT (ts, name) DO UPDATE
            SET count = EXCLUDED.count,
                lat = EXCLUDED.lat,
                long = EXCLUDED.long
            """,
            valid_records,
            template=None,
            page_size=1000,
        )

    print(f"Bulk inserted {len(valid_records)} city records")
    if skipped_count > 0:
        print(f"Skipped {skipped_count} records with missing coordinates")

    return len(valid_records)


def save_geo_data_to_database(
    city_data=None, clear_existing=True, use_bulk_insert=True
):
    """
    Main function to save geographic data to PostgreSQL database.

    Args:
        city_data: List of city records with name, count, lat, lon
        clear_existing: Whether to clear existing data before insert
        use_bulk_insert: Whether to use bulk insert for better performance
    """
    conn = get_db_conn()

    try:
        with conn:
            with conn.cursor() as cursor:

                # Clear existing data if requested
                if clear_existing:
                    tables_to_clear = []
                    if city_data:
                        tables_to_clear.append("city_snapshots")
                    if tables_to_clear:
                        clear_geo_tables(cursor, tables_to_clear)

                # Insert city data
                total_inserted = 0
                if city_data:
                    if use_bulk_insert and len(city_data) > 10:
                        total_inserted += bulk_insert_city_snapshots(cursor, city_data)
                    else:
                        total_inserted += insert_city_snapshots(cursor, city_data)

                print(f"Total records inserted: {total_inserted}")

    finally:
        conn.close()

    return total_inserted
