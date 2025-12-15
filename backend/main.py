# Helper for metric endpoints
def get_metric_trend(metric_name, response_key, period):
    now = datetime.utcnow()
    if period == "day":
        since = now - timedelta(days=365)
    elif period == "week":
        since = now - timedelta(weeks=365)
    elif period == "month":
        since = now - timedelta(days=365)
    else:
        since = now - timedelta(days=365)
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT ts, value FROM metrics_scalar
                WHERE metric_name = %s AND ts >= %s
                ORDER BY ts ASC
                """,
                (metric_name, since.isoformat())
            )
            rows = cur.fetchall()
            if rows:
                return {response_key: [{"ts": r[0], "value": r[1]} for r in rows]}
            raise HTTPException(status_code=404, detail=f"No data found for {metric_name}")

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import uvicorn
from typing import List, Optional
import os
import psycopg2
import json
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv


app = FastAPI()

load_dotenv() 
origins = os.environ.get("FRONTEND_ORIGINS", "").split(",")

print("Loaded origins:", os.environ.get("FRONTEND_ORIGINS"))


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # list of allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Database connection setup (PostgreSQL example)
def get_db_conn():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432))
    )

# Models for POST endpoints
class LoadStats(BaseModel):
    node_id: str
    cpu_load: float
    memory_load: float
    timestamp: Optional[str] = None

from datetime import datetime, timedelta

# For random/placeholder data
import random
from fastapi import Request
from typing import Any

# Total CPU Cores
@app.get("/metrics/total_cpu_cores")
def get_total_cpu_cores(period: str = Query("day", enum=["day", "week", "month"], description="Time period: day, week, or month")):
    return get_metric_trend("total_cores", "total_cpu_cores", period)

# Total Memory
@app.get("/metrics/total_memory")
def get_total_memory(period: str = Query("day", enum=["day", "week", "month"], description="Time period: day, week, or month")):
    return get_metric_trend("total_memory", "total_memory", period)

# Total Nodes
@app.get("/metrics/total_nodes")
def get_total_nodes(period: str = Query("day", enum=["day", "week", "month"], description="Time period: day, week, or month")):
    return get_metric_trend("total_nodes", "total_nodes", period)

# Total Disk
@app.get("/metrics/total_disk")
def get_total_disk(period: str = Query("day", enum=["day", "week", "month"], description="Time period: day, week, or month")):
    return get_metric_trend("total_disk", "total_disk", period)

# Running Replica Count
@app.get("/metrics/running_replica_count")
def get_running_replica_count(period: str = Query("day", enum=["day", "week", "month"], description="Time period: day, week, or month")):
    return get_metric_trend("running_replica_count", "running_replica_count", period)

# Running Disk
@app.get("/metrics/running_min_disk")
def get_running_min_disk(period: str = Query("day", enum=["day", "week", "month"], description="Time period: day, week, or month")):
    return get_metric_trend("running_min_disk", "running_min_disk", period)

# Running CPU
@app.get("/metrics/running_min_cpu")
def get_running_min_cpu(period: str = Query("day", enum=["day", "week", "month"], description="Time period: day, week, or month")):
    return get_metric_trend("running_min_cpu", "running_min_cpu", period)

# Running Memory
@app.get("/metrics/running_min_ram")
def get_running_min_ram(period: str = Query("day", enum=["day", "week", "month"], description="Time period: day, week, or month")):
    return get_metric_trend("running_min_ram", "running_min_ram", period)
        


@app.get("/metrics/cpu_cores")
def get_cpu_cores(period: str = Query("day", enum=["day", "week", "month"], description="Time period: day, week, or month")):
    now = datetime.utcnow()
    if period == "day":
        since = now - timedelta(days=365)
    elif period == "week":
        since = now - timedelta(weeks=365)
    elif period == "month":
        since = now - timedelta(days=365)
    else:
        since = now - timedelta(days=365)
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts, value FROM metrics_scalar
                WHERE metric_name = 'total_cores' AND ts >= %s
                ORDER BY ts ASC
                """,
                (since.isoformat(),)
            )
            rows = cur.fetchall()
            if rows:
                return {"cpu_cores": [{"ts": r[0], "value": r[1]} for r in rows]}
            raise HTTPException(status_code=404, detail="No data found for period")


@app.get("/metrics/city_counts")
def get_city_counts():
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, count, lat, long 
                FROM city_snapshots 
                WHERE ts = (SELECT MAX(ts) FROM city_snapshots)
            """)
            rows = cur.fetchall()
            return [
                {"city": r[0], "count": r[1], "lat": r[2], "lon": r[3]} for r in rows
            ]

@app.get("/metrics/country_counts")
def get_country_counts():
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name, count, lat, long FROM country_snapshots ORDER BY ts DESC LIMIT 1000")
            rows = cur.fetchall()
            return [
                {"country": r[0], "count": r[1], "lat": r[2], "lon": r[3]} for r in rows
            ]

@app.post("/metrics/load")
def post_load_stats(stats: LoadStats):
    # Example: insert into a table called node_load_stats
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO node_load_stats (node_id, cpu_load, memory_load, ts)
                VALUES (%s, %s, %s, NOW())
                """,
                (stats.node_id, stats.cpu_load, stats.memory_load)
            )
    return {"status": "ok"}


@app.get("/metrics/transactions")
def get_transactions(
    limit: int = Query(10, ge=1, le=100),
    start: Optional[str] = Query(None, description="Start datetime ISO8601 (default: 1 day ago)"),
    end: Optional[str] = Query(None, description="End datetime ISO8601 (default: now)")
):
    """
    Returns a list of placeholder transaction records for demo/testing.
    """
    # Parse start/end or use defaults
    now = datetime.utcnow()
    if end:
        end_dt = datetime.fromisoformat(end)
    else:
        end_dt = now
    if start:
        start_dt = datetime.fromisoformat(start)
    else:
        start_dt = end_dt - timedelta(days=1)

    # Generate placeholder transactions
    providers = [
        "0x0B220b82F3eA3B7F6d9A1D8ab58930C064A2b5Bf",
        "0xA1B2c3D4E5F678901234567890abcdef12345678",
        "0xBEEF1234567890abcdef1234567890ABCDEF1234"
    ]
    requesters = [
        "0xD50f254E7E6ABe1527879c2E4E23B9984c783295",
        "0xC0FFEE1234567890abcdef1234567890ABCDEF12",
        "0xDEADBEEF1234567890abcdef1234567890ABCDEF"
    ]
    gpus = ["RTX 4090", "RTX 4080", "RTX 3090", "RTX 3060", "A100", "Other"]
    txs = [
        "0xe3f9e48f556dbec85b0031ddbb157893eb4f4bb1564577a7f36ef19834790986",
        "0xabc1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab",
        "0xdef9876543210abcdef1234567890abcdef1234567890abcdef1234567890cd"
    ]

    transactions = []
    total_seconds = int((end_dt - start_dt).total_seconds())
    for i in range(limit):
        # Random timestamp in range
        ts_offset = random.randint(0, max(1, total_seconds))
        ts = (start_dt + timedelta(seconds=ts_offset)).replace(microsecond=0)
        duration_minutes = random.randint(5, 120)
        duration = timedelta(minutes=duration_minutes)
        transactions.append({
            "ts": ts.isoformat(),
            "provider_wallet": random.choice(providers),
            "requester_wallet": random.choice(requesters),
            "tx": random.choice(txs),
            "gpu": random.choice(gpus),
            "ram": random.choice([8192, 16384, 20480, 32768, 65536]),
            "vcpus": random.choice([4, 8, 16, 32]),
            "duration": str(duration),
            "invoiced_glm": round(random.uniform(0.5, 10.0), 2),
            "invoiced_dollar": round(random.uniform(0.1, 5.0), 2),
        })
    return {"transactions": transactions}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)