
from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from pydantic import BaseModel
import uvicorn
import os
import psycopg2
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from datetime import datetime, timedelta
import random

load_dotenv()

app = FastAPI()

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

def find_data(metric: str, period: str):
    now = datetime.utcnow() 
    if (metric in [
                    "total_time_seconds",
                    "total_invoice_amount",
                    "total_ram_hours",
                    "total_cpu_hours",
                    "total_transaction_count"
                    ]
        ): 
        table = "hourly_gpu_stats"
        if (period == "day" or period == "week"):
            ts_col = "hour"
        else: 
            ts_col = "day"
    
    elif metric in [
                    "unique_node_count",
                    "unique_node_ram",
                    "unique_node_cpu"
                    ]:  
        if (period == "day" or period == "week"):
            table = "hourly_distinct_counts"
            ts_col = "hour"
        else:
            table = "daily_distinct_counts"
            ts_col = "day"
    else:
        print(metric)

    if period == "day":
        since = now - timedelta(days=1)
    elif period == "week":
        since = now - timedelta(weeks=1)
    elif period == "month":
        since = now - timedelta(days=31)

    return( {"since": since, "table": table, "ts_col": ts_col} )


def get_metrics_by_gpu(
    metric: str,
    period: str = "day"
):

    # query for all the GPUs in the time range. 
    # find the totals by GPU group


    # This function will fetch metrics grouped by GPU
    now = datetime.utcnow()
    if period == "day":
        since = now - timedelta(days=1)
    elif period == "week":
        since = now - timedelta(weeks=1)
    elif period == "month":
        since = now - timedelta(days=31)
    else:
        since = now - timedelta(days=1)

    query = f"""
        SELECT gpu_group, {metric}, {period} FROM hourly_gpu_stats
        WHERE {period} >= %s
        GROUP BY gpu_group, {period}
        ORDER BY {period} ASC
    """

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (since,))
            rows = cur.fetchall()
            result = {}
            for r in rows:
                gpu_group = r[0]
                if gpu_group not in result:
                    result[gpu_group] = []
                result[gpu_group].append({"ts": r[2], "value": r[1]})
            return result


def get_metrics(
    metric: str,
    period: str = "day",
    gpu: str = "all"
):
    """
    Returns a time series for the given metric (for the 'all' gpu_group), as a list of {ts, value} dicts.
    Allowed metrics: total_time_seconds, total_invoice_amount, total_ram_hours, total_cpu_hours, total_transaction_count
    """

    print(metric)
   
    data_info = find_data(metric=metric, period=period)
    table = data_info["table"]
    ts_col = data_info["ts_col"]
    since = data_info["since"]

    params = (gpu, since)

    if table == "hourly_gpu_stats" and ts_col == "day":
        query = f"""
            SELECT DATE(hour) as day, SUM({metric}) as value
            FROM {table}
            WHERE gpu_group = %s AND hour >= %s
            GROUP BY day
            ORDER BY {ts_col} ASC
        """
    else:
        query = f"""
        SELECT {ts_col}, {metric} FROM {table}
        WHERE gpu_group = %s AND {ts_col} >= %s
        ORDER BY {ts_col} ASC
    """

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            return {metric: [{"ts": r[0], "value": r[1]} for r in rows]}

# Generalized endpoint for hourly/daily GPU stats (for 'all' group)
@app.get("/metrics/stats")
def assemble_metrics(
    period: str = Query("day", enum=["day", "week", "month"], description="Time period: day, week, or month, default: day"),
    gpu: str = Query("all", description="GPUs to consider: all, any_gpu, no_gpu, specific gpu class, or any GUID (default: all)")
):
    """
    Returns a time series for the given metric (for the specified gpu_group or GUID), as a list of {ts, value} dicts.
    Allowed metrics: total_time_seconds, total_invoice_amount, total_ram_hours, total_cpu_hours, total_transaction_count
    """
    metrics = [
        "total_time_seconds",
        "total_invoice_amount",
        "total_ram_hours",
        "total_cpu_hours",
        "total_transaction_count",
        "unique_node_count",
        "unique_node_ram",
        "unique_node_cpu"
    ]

    allowed_periods = ["day", "week", "month"]

    if period not in allowed_periods:
        raise HTTPException(status_code=400, detail=f"Invalid period. Allowed: {allowed_periods}")
    
    assembled_metrics = {}
    for metric in metrics:
        assembled_metrics[metric] = get_metrics(
            metric=metric,
            period=period,
            gpu=gpu
        )[metric]
    return assembled_metrics

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
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)