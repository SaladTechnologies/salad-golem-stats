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


def main(filter_is_running=False, filter_has_workload=False, filter_organizations=[]):
    # Load .env variables
    load_dotenv()  # reads .env from current directory

    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
    )

    mongo_user = os.getenv("MONGOUSER")
    mongo_password = os.getenv("MONGOPASS")
    mongo_name = os.getenv("DBNAME")
    mongo_url = os.getenv("MONGO_URL")

    def get_database(dbname=mongo_name):

        CONNECTION_STRING = f"mongodb+srv://{mongo_user}:{mongo_password}@{mongo_url}/"
        # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
        client = MongoClient(CONNECTION_STRING)

        return client[dbname]

    ##################
    # GET DATA
    ##################
    ts = (datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    date_cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )

    min_download = 10  # in Mbps

    node_query = {
        "updated_at": {"$gt": {"DateTime": date_cutoff}},
        "sel_ver_num": {"$gte": min_sel_ver_num},
        "container_ready": True,
        "os_arch": "x64",
        "$or": [
            {
                "ip": {"$gt": {"ndm_download": min_download}},
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

    db = get_database()
    collection = db["nodes"]
    node_results = collection.find(node_query, node_projection)

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
            "organization_id": 1,  # NEED TO TEST
        }

        collection = db["workloads"]
        workload_results = collection.find(workload_query, workload_projection)

        node_list = list(node_results)
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
            node["instance_organization_names"] = set()
            node["instance_organization_ids"] = set()

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

    ##################
    ### PROCESS DATA
    ##################

    # City and country counts
    city_counter = Counter()
    country_counter = Counter()
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
        country = node.get("ip", {}).get("country_code", None)
        if city:
            city_counter[city] += 1
        if country:
            country_counter[country] += 1

    # Process to lat / long
    # Geocode cache file
    GEOCODE_CITY_CACHE_PATH = Path("./data/city_geocode_cache.json")
    if GEOCODE_CITY_CACHE_PATH.exists():
        with open(GEOCODE_CITY_CACHE_PATH, "r", encoding="utf-8") as f:
            geocode_city_cache = json.load(f)
    else:
        geocode_city_cache = {}

    GEOCODE_COUNTRY_CACHE_PATH = Path("./data/country_geocode_cache.json")
    if GEOCODE_COUNTRY_CACHE_PATH.exists():
        with open(GEOCODE_COUNTRY_CACHE_PATH, "r", encoding="utf-8") as f:
            geocode_country_cache = json.load(f)
    else:
        geocode_country_cache = {}

    def geocode_city(city_name):
        if city_name in geocode_city_cache:
            return geocode_city_cache[city_name]
        if city_name == "N/A" or not city_name:
            return None
        # Use OpenStreetMap Nominatim API
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

    def geocode_country_code(country_code):
        if country_code in geocode_country_cache:
            return geocode_country_cache[country_code]
        if country_code == "N/A" or not country_code:
            return None
        # Use OpenStreetMap Nominatim API
        url = f"https://nominatim.openstreetmap.org/search?country={country_code}&format=json&limit=1"
        try:
            resp = requests.get(url, headers={"User-Agent": "SaladCloudStats/1.0"})
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    lat = float(data[0]["lat"])
                    lon = float(data[0]["lon"])
                    geocode_country_cache[country_code] = {"lat": lat, "lon": lon}
                    time.sleep(2)  # Be polite to API
                    return geocode_country_cache[country_code]
        except Exception as e:
            print(f"Geocoding error for {country_code}: {e}")
        geocode_country_cache[country_code] = None
        return None

    # Prepare output with geocoding
    output_rows_city = []
    total_cities = len(city_counter)
    print(f"Geocoding {total_cities} cities...")
    for idx, (city, count) in enumerate(city_counter.items(), 1):
        print(f"[{idx}/{total_cities}] Geocoding: {city}")
        geo = geocode_city(city)
        if geo:
            output_rows_city.append(
                {"city": city, "count": count, "lat": geo["lat"], "lon": geo["lon"]}
            )
        else:
            output_rows_city.append(
                {"city": city, "count": count, "lat": "", "lon": ""}
            )
    print("City geocoding complete.")

    output_rows_country = []
    total_countries = len(country_counter)
    print(f"Geocoding {total_countries} countries...")
    for idx, (country, count) in enumerate(country_counter.items(), 1):
        print(f"[{idx}/{total_countries}] Geocoding: {country}")
        geo = geocode_country_code(country)
        if geo:
            output_rows_country.append(
                {
                    "country": country,
                    "count": count,
                    "lat": geo["lat"],
                    "lon": geo["lon"],
                }
            )
        else:
            output_rows_country.append(
                {"country": country, "count": count, "lat": "", "lon": ""}
            )
    print("Country geocoding complete.")

    # Update geocode caches
    with open(GEOCODE_CITY_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(geocode_city_cache, f, ensure_ascii=False, indent=2)

    with open(GEOCODE_COUNTRY_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(geocode_country_cache, f, ensure_ascii=False, indent=2)

    ################################
    # SAVE PROCESSED DATA
    ################################

    # Write CSV with lat/lon
    with open("./data/node_count_by_city.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["city", "count", "lat", "lon"])
        writer.writeheader()
        for row in output_rows_city:
            writer.writerow({k: row.get(k, "") for k in writer.fieldnames})

    # Save node count by country_code to a CSV file
    with open(
        "node_count_by_country.csv", "w", newline="", encoding="utf-8"
    ) as f_country:
        writer = csv.DictWriter(
            f_country, fieldnames=["country", "count", "lat", "lon"]
        )
        writer.writeheader()
        for row in output_rows_country:
            writer.writerow({k: row.get(k, "") for k in writer.fieldnames})

    ######################
    # to database
    ######################

    def safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    skipped_cities = []
    with conn:
        with conn.cursor() as cur:
            for loc in output_rows_city:
                lat = safe_float(loc["lat"])
                lon = safe_float(loc["lon"])
                if lat is not None and lon is not None:
                    cur.execute(
                        """
                        INSERT INTO city_snapshots (ts, name, count, lat, long)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (ts, name) DO UPDATE
                        SET count = EXCLUDED.count,
                            lat = EXCLUDED.lat,
                            long = EXCLUDED.long
                        """,
                        (ts, loc["city"], loc["count"], lat, lon),
                    )
                else:
                    skipped_cities.append(loc)
    conn.close()

    if skipped_cities:
        print("Skipped cities with missing coordinates:")
        for loc in skipped_cities:
            print(loc)


if __name__ == "__main__":

    main(filter_is_running=False, filter_has_workload=False, filter_organizations=[])
