import os
from collections import Counter
from datetime import datetime, timedelta, timezone
import requests
import time
import json
import csv
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv
import psycopg2
from shared_geo_db import save_geo_data_to_database


def get_database_connection():
    """Initialize database connections"""
    load_dotenv()

    # PostgreSQL connection
    pg_conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
    )

    # MongoDB connection
    mongo_user = os.getenv("MONGOUSER")
    mongo_password = os.getenv("MONGOPASS")
    mongo_name = os.getenv("DBNAME")
    mongo_url = os.getenv("MONGO_URL")

    connection_string = f"mongodb+srv://{mongo_user}:{mongo_password}@{mongo_url}/"
    mongo_client = MongoClient(connection_string)
    mongo_db = mongo_client[mongo_name]

    return pg_conn, mongo_db


def get_node_data(
    filter_is_running=False, filter_has_workload=False, filter_organizations=[]
):
    """Fetch and filter node data from MongoDB"""
    _, mongo_db = get_database_connection()

    date_cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )

    min_sel_ver_num = int(os.getenv("MIN_SEL", "2003009"))
    min_download = 10  # in Mbps

    # Use datetime object for MongoDB comparison instead of string
    date_cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=24)

    node_query = {
        "updated_at.DateTime": {"$gt": date_cutoff_dt},
        "sel_ver_num": {"$gte": min_sel_ver_num},
        "container_ready": True,
        "os_arch": "x64",
        "$or": [
            {
                "ip.ndm_download": {"$gt": min_download},
            },
            {"is_datacenter": True},
        ],
    }

    node_projection = {
        "node_id": 1,
        "cpu_cores": 1,
        "wsl_memory": 1,
        "available_disk": 1,
        "gpus.card_name": 1,
        "gpus.vram": 1,
        "ncw_status": 1,
        "ncw_detail": 1,
        "ip.country_code": 1,
        "ip.city": 1,
        "ip.ndm_download": 1,
        "ip.ndm_upload": 1,
        "ip.ip_address": 1,
        "is_running": 1,
        "instances.workload_id": 1,
        "instances.last_instance_state": 1,
    }

    collection = mongo_db["nodes"]
    node_results = collection.find(node_query, node_projection)
    node_list = list(node_results)

    if filter_organizations:
        workload_query = {}
        workload_projection = {
            "replica_count": 1,
            "min_disk": 1,
            "min_cpu": 1,
            "gpu": 1,
            "min_ram": 1,
            "instances.status": 1,
            "workload_id": 1,
            "organization_name": 1,
            "organization_id": 1,
        }

        collection = mongo_db["workloads"]
        workload_results = collection.find(workload_query, workload_projection)
        workload_list = list(workload_results)

        # Create workload lookup by workload_id
        workload_lookup = {}
        for workload in workload_list:
            workload_id = workload.get("workload_id")
            if workload_id:
                workload_lookup[workload_id] = {
                    "organization_id": workload.get("organization_id"),
                    "organization_name": workload.get("organization_name"),
                }

        # Add organization info to nodes based on instances.workload_id
        for node in node_list:
            instances = node.get("instances", [])
            node["organization_names"] = set()
            node["organization_ids"] = set()

            for instance in instances:
                workload_id = instance.get("workload_id")
                if workload_id and workload_id in workload_lookup:
                    org_info = workload_lookup[workload_id]
                    if org_info["organization_id"]:
                        node["organization_ids"].add(org_info["organization_id"])
                    if org_info["organization_name"]:
                        node["organization_names"].add(org_info["organization_name"])

            # Convert sets to lists for easier handling
            node["organization_ids"] = list(node["organization_ids"])
            node["organization_names"] = list(node["organization_names"])

    # Process and filter nodes
    city_counter = Counter()

    for node in node_list:
        # Apply optional filters
        if filter_has_workload:
            instances = node.get("instances", [])
            if not instances or not any(
                instance.get("workload_id") for instance in instances
            ):
                continue

        if filter_is_running:
            if not node.get("is_running", False):
                continue

        if filter_organizations:
            # Check if node has any of the specified organizations
            node_org_ids = node.get("organization_ids", [])
            node_org_names = node.get("organization_names", [])
            if not any(
                org_id in filter_organizations for org_id in node_org_ids
            ) and not any(
                org_name in filter_organizations for org_name in node_org_names
            ):
                continue

        city = node.get("ip", {}).get("city", None)
        if city:
            city_counter[city] += 1

    return city_counter


def load_geocode_caches():
    """Load existing geocode caches"""
    GEOCODE_CITY_CACHE_PATH = Path("./data/city_geocode_cache.json")
    if GEOCODE_CITY_CACHE_PATH.exists():
        with open(GEOCODE_CITY_CACHE_PATH, "r", encoding="utf-8") as f:
            geocode_city_cache = json.load(f)
    else:
        geocode_city_cache = {}

    return geocode_city_cache


def geocode_city(city_name, geocode_city_cache):
    """Geocode a city name using OpenStreetMap Nominatim API"""
    if city_name in geocode_city_cache:
        return geocode_city_cache[city_name]
    if city_name == "N/A" or not city_name:
        return None

    url = f"https://nominatim.openstreetmap.org/search?city={city_name}&format=json&limit=1"
    try:
        resp = requests.get(url, headers={"User-Agent": "SaladCloudStats/1.0"})
        if resp.status_code == 200:
            data = resp.json()
            if data:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                geocode_city_cache[city_name] = {"lat": lat, "lon": lon}
                time.sleep(2)  # Be polite to API
                return geocode_city_cache[city_name]
    except Exception as e:
        print(f"Geocoding error for {city_name}: {e}")
    geocode_city_cache[city_name] = None
    return None


def add_lat_long_to_data(city_counter):
    """Add latitude and longitude coordinates to location data"""
    geocode_city_cache = load_geocode_caches()

    # Process cities
    output_rows_city = []
    total_cities = len(city_counter)
    print(f"Geocoding {total_cities} cities...")
    for idx, (city, count) in enumerate(city_counter.items(), 1):
        print(f"[{idx}/{total_cities}] Geocoding: {city}")
        geo = geocode_city(city, geocode_city_cache)
        if geo:
            output_rows_city.append(
                {"city": city, "count": count, "lat": geo["lat"], "lon": geo["lon"]}
            )
        else:
            output_rows_city.append(
                {"city": city, "count": count, "lat": "", "lon": ""}
            )
    print("City geocoding complete.")

    # Update geocode caches
    GEOCODE_CITY_CACHE_PATH = Path("./data/city_geocode_cache.json")

    with open(GEOCODE_CITY_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(geocode_city_cache, f, ensure_ascii=False, indent=2)
    return output_rows_city


def save_data_to_files(output_rows_city):
    """Save processed data to JSON file for transfer/import"""
    # Write city data JSON for transfer/import
    export_data = {
        "export_metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "get_geo_data.py",
            "record_count": len(output_rows_city),
            "description": "City node distribution data for dashboard import",
        },
        "data": output_rows_city,
    }

    with open("./data/city_data_export.json", "w", encoding="utf-8") as f:
        json.dump(export_data, f, ensure_ascii=False, indent=2)

    print(f"Exported {len(output_rows_city)} city records to city_data_export.json")


def save_data_to_database(output_rows_city):
    """Save processed data to PostgreSQL database using shared database functions"""
    print("Saving city data to database...")
    save_geo_data_to_database(
        city_data=output_rows_city, clear_existing=True, use_bulk_insert=True
    )


def main(filter_is_running=False, filter_has_workload=False, filter_organizations=[]):
    """Main function that orchestrates the entire data processing pipeline"""
    print("Starting geographic data processing...")

    # 1. Get the data
    print("1. Fetching node data from MongoDB...")
    city_counter = get_node_data(
        filter_is_running, filter_has_workload, filter_organizations
    )

    # 2. Add lat/long coordinates to the data
    print("2. Adding latitude/longitude coordinates...")
    output_rows_city = add_lat_long_to_data(city_counter)

    # 3. Save the data to files
    print("3. Saving data to CSV files...")
    save_data_to_files(output_rows_city)

    # 4. Put the data in the database
    print("4. Saving data to PostgreSQL database...")
    save_data_to_database(output_rows_city)

    print("Geographic data processing complete!")


if __name__ == "__main__":
    main(filter_is_running=False, filter_has_workload=False, filter_organizations=[])
