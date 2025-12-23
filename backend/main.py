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
from datetime import timezone

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
        port=int(os.getenv("POSTGRES_PORT", 5432)),
    )


# Load GPU class names once at startup
gpu_class_names = {}
try:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT gpu_class_id, gpu_class_name FROM gpu_classes")
            for row in cur.fetchall():
                gpu_class_names[row[0]] = row[1]
except Exception as e:
    print(f"Warning: Could not load gpu_class_names at startup: {e}")


def get_query_parameters(metric: str, period: str):

    now = datetime.now(timezone.utc)
    if metric in [
        "total_time_seconds",
        "total_time_hours",
        "total_invoice_amount",
        "total_ram_hours",
        "total_cpu_hours",
        "total_transaction_count",
    ]:
        table = "hourly_gpu_stats"
        if period == "day":
            ts_col = "hour"
        else:
            ts_col = "day"

    elif metric in ["unique_node_count", "unique_node_ram", "unique_node_cpu"]:
        if period == "day":
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
    elif period == "two_weeks":
        since = now - timedelta(weeks=2)
    elif period == "month":
        since = now - timedelta(days=31)

    if ts_col == "day":
        since = since.replace(hour=0, minute=0, second=0, microsecond=0)
    return {"since": since, "table": table, "ts_col": ts_col}


def get_metrics_by_gpu(metric: str, period: str = "month", group_by: str = "gpu"):

    # query for all the GPUs in the time range.
    # find the totals by GPU group

    query_info = get_query_parameters(metric=metric, period=period)
    table = query_info["table"]
    ts_col = query_info["ts_col"]
    since = query_info["since"]

    if table == "hourly_gpu_stats" and ts_col == "day":
        query = f"""
            SELECT gpu_group, DATE(hour) as day, SUM({metric}) as value
            FROM {table}
            WHERE gpu_group NOT IN ('all', 'any_gpu', 'no_gpu') AND hour >= %s
            GROUP BY gpu_group, day
            ORDER BY gpu_group, day ASC
        """
        params = (since,)
    else:
        query = f"""
        SELECT gpu_group, {ts_col}, {metric} FROM {table}
        WHERE gpu_group NOT IN ('all', 'any_gpu', 'no_gpu') AND {ts_col} >= %s
        ORDER BY {ts_col} ASC
    """
        params = (since,)

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

            # Extract unique sorted timestamps for labels
            labels = sorted({r[1] if len(r) > 2 else r[0] for r in rows})

            if group_by == "vram":
                # Build a mapping from gpu_group to VRAM (as string, e.g., '24 GB')
                gpu_to_vram = {}
                for gpu_id, name in gpu_class_names.items():
                    import re

                    match = re.search(r"\((\d+\s*[gG][bB])\)", name)
                    if match:
                        vram = match.group(1).replace(" ", "")  # e.g., '24GB'
                    else:
                        vram = "Unknown"
                    gpu_to_vram[gpu_id] = vram

                # Aggregate by VRAM, aligned to labels
                vram_group_sums = {}
                vram_entries = {}
                for r in rows:
                    gpu_group = r[0]
                    ts = r[1] if len(r) > 2 else r[0]
                    value = r[2] if len(r) > 2 else r[1]
                    vram = gpu_to_vram.get(gpu_group, "Unknown")
                    vram_group_sums[vram] = vram_group_sums.get(vram, 0) + (value or 0)
                    if vram not in vram_entries:
                        vram_entries[vram] = {}
                    vram_entries[vram][ts] = vram_entries[vram].get(ts, 0) + (value or 0)

                # Find top 5 VRAM groups
                top_vram_groups = sorted(vram_group_sums.items(), key=lambda x: x[1], reverse=True)[
                    :5
                ]
                top_vram_names = set(v for v, _ in top_vram_groups)
                output = []
                # Prepare data arrays aligned to labels
                for vram in top_vram_names:
                    data = [vram_entries[vram].get(ts, 0) for ts in labels]
                    output.append({"label": vram, "data": data})
                # Aggregate all other VRAMs into 'Other', aligned to labels
                other_data = [0 for _ in labels]
                label_index = {ts: i for i, ts in enumerate(labels)}
                for vram in vram_entries:
                    if vram not in top_vram_names:
                        for ts, v in vram_entries[vram].items():
                            idx = label_index[ts]
                            other_data[idx] += v
                if any(other_data):
                    output.append({"label": "Other", "data": other_data})

            else:
                # group by GPU
                group_sums = {}
                for r in rows:
                    gpu_group = r[0]
                    value = r[2] if len(r) > 2 else r[1]
                    group_sums[gpu_group] = group_sums.get(gpu_group, 0) + (value or 0)
                # Find the top 5 gpu_groups by sum
                top_groups = sorted(group_sums.items(), key=lambda x: x[1], reverse=True)[:5]
                top_group_names = set(g for g, _ in top_groups)
                output = []
                # Prepare group_entries and other_entries as arrays aligned to labels
                group_entries = {g: [0 for _ in labels] for g in top_group_names}
                other_entries = [0 for _ in labels]
                label_index = {ts: i for i, ts in enumerate(labels)}
                for r in rows:
                    gpu_group = r[0]
                    ts = r[1] if len(r) > 2 else r[0]
                    value = r[2] if len(r) > 2 else r[1]
                    idx = label_index[ts]
                    if gpu_group in top_group_names:
                        group_entries[gpu_group][idx] += value or 0
                    else:
                        other_entries[idx] += value or 0
                # Only send the readable name as the group
                for gpu_group in top_group_names:
                    label = gpu_class_names.get(gpu_group, None)
                    if label is None:
                        print(
                            f"[WARNING] gpu_group '{gpu_group}' not found in gpu_class_names, using fallback."
                        )
                        label = gpu_group
                    output.append({"label": label, "data": group_entries[gpu_group]})
                if any(other_entries):
                    output.append({"label": "Other", "data": other_entries})

            return {metric: {"labels": labels, "datasets": output}}


def get_metrics(metric: str, period: str = "week", gpu: str = "all"):
    """
    Returns a time series for the given metric (for the 'all' gpu_group), as a list of {ts, value} dicts.
    Allowed metrics: total_time_hours, total_invoice_amount, total_ram_hours, total_cpu_hours, total_transaction_count
    """

    query_info = get_query_parameters(metric=metric, period=period)
    table = query_info["table"]
    ts_col = query_info["ts_col"]
    since = query_info["since"]

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
            return {metric: [{"x": r[0], "y": r[1]} for r in rows]}


@app.get("/metrics/stats")
def get_stats_summary(
    period: str = Query(
        "week",
        enum=["week", "two_weeks", "month"],
        description="Time period: week, two_weeks, or month, default: day",
    ),
    gpu: str = Query(
        "all",
        description="GPUs to consider: all, any_gpu, no_gpu, specific gpu class, or any GUID (default: all)",
    ),
):
    """
    Returns a dict of summed values for each metric over the provided period and gpu group.
    Metrics: total_time_hours, total_invoice_amount, unique_node_count, total_transaction_count
    """
    metrics = [
        "total_time_hours",
        "total_invoice_amount",
        "unique_node_count",
        "total_transaction_count",
    ]
    allowed_periods = ["day", "week", "month"]
    if period not in allowed_periods:
        raise HTTPException(status_code=400, detail=f"Invalid period. Allowed: {allowed_periods}")

    results = {}
    for metric in metrics:
        query_info = get_query_parameters(metric=metric, period=period)
        table = query_info["table"]
        ts_col = query_info["ts_col"]
        since = query_info["since"]
        params = (gpu, since)
        if table == "hourly_gpu_stats" and ts_col == "day":
            query = f"""
                SELECT SUM({metric}) as value
                FROM {table}
                WHERE gpu_group = %s AND hour >= %s
            """
        else:
            query = f"""
                SELECT SUM({metric}) as value
                FROM {table}
                WHERE gpu_group = %s AND {ts_col} >= %s
            """
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                results[metric] = row[0] if row and row[0] is not None else 0
    return results


# Generalized endpoint for hourly/daily GPU stats (for 'all' group)
@app.get("/metrics/trends")
def assemble_metrics(
    period: str = Query(
        "week",
        enum=["week", "two_weeks", "month"],
        description="Time period: week, two_weeks, or month, default: week",
    ),
    gpu: str = Query(
        "all",
        description="GPUs to consider: all, any_gpu, no_gpu, specific gpu class, or any GUID (default: all)",
    ),
):
    """
    Returns a time series for the given metric (for the specified gpu_group or GUID), as a list of {ts, value} dicts.
    Allowed metrics: total_time_hours, total_invoice_amount, total_ram_hours, total_cpu_hours, total_transaction_count
    """
    metrics = [
        "total_time_hours",
        "total_invoice_amount",
        "total_ram_hours",
        "total_cpu_hours",
        "total_transaction_count",
        "unique_node_count",
    ]

    gpu_metrics = [
        "unique_node_count",
        "total_time_hours",
    ]
    vram_metrics = [
        "unique_node_count",
        "total_time_hours",
    ]

    allowed_periods = ["week", "two_weeks", "month"]

    if period not in allowed_periods:
        raise HTTPException(status_code=400, detail=f"Invalid period. Allowed: {allowed_periods}")

    assembled_metrics = {}
    for metric in metrics:
        assembled_metrics[metric] = get_metrics(metric=metric, period=period, gpu=gpu)[metric]

    for metric in gpu_metrics:
        assembled_metrics["gpu_" + metric] = get_metrics_by_gpu(metric=metric, period=period)[
            metric
        ]

    for metric in vram_metrics:
        assembled_metrics["vram_" + metric] = get_metrics_by_gpu(
            metric=metric, period=period, group_by="vram"
        )[metric]

    return assembled_metrics


@app.get("/metrics/city_counts")
def get_city_counts():
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name, count, lat, long 
                FROM city_snapshots 
                WHERE ts = (SELECT MAX(ts) FROM city_snapshots)
            """
            )
            rows = cur.fetchall()
            return [{"city": r[0], "count": r[1], "lat": r[2], "lon": r[3]} for r in rows]


@app.get("/metrics/transactions")
def get_transactions(
    limit: int = Query(10, ge=1, le=100),
    cursor: Optional[str] = Query(
        None, description="Cursor for pagination (ISO8601 timestamp, default: latest)"
    ),
    direction: str = Query(
        "next", enum=["next", "prev"], description="Pagination direction: next or prev"
    ),
):
    """
    Returns a list of placeholder transaction records for demo/testing, with cursor-based pagination.
    """
    # Query the placeholder_transactions table with cursor-based pagination
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            # Count total transactions
            cur.execute("SELECT COUNT(*) FROM placeholder_transactions")
            total = cur.fetchone()[0]

            # Build the base query
            base_query = """
                SELECT ts, provider_wallet, requester_wallet, tx, gpu, ram, vcpus, duration, invoiced_glm, invoiced_dollar
                FROM placeholder_transactions
            """
            order = "DESC" if direction == "next" else "ASC"
            where = []
            params = []
            if cursor:
                op = "<" if direction == "next" else ">"
                where.append(f"ts {op} %s")
                params.append(cursor)
            if where:
                base_query += " WHERE " + " AND ".join(where)
            base_query += f" ORDER BY ts {order} LIMIT %s"
            params.append(limit)

            cur.execute(base_query, params)
            rows = cur.fetchall()

            # Always return newest first (descending)
            if direction == "prev":
                rows = rows[::-1]

            page_transactions = []
            for r in rows:
                page_transactions.append(
                    {
                        "ts": r[0].isoformat() if hasattr(r[0], "isoformat") else str(r[0]),
                        "provider_wallet": r[1],
                        "requester_wallet": r[2],
                        "tx": r[3],
                        "gpu": r[4],
                        "ram": r[5],
                        "vcpus": r[6],
                        "duration": str(r[7]),
                        "invoiced_glm": float(r[8]),
                        "invoiced_dollar": float(r[9]),
                    }
                )

            # Set next/prev cursors
            next_cursor = (
                page_transactions[-1]["ts"]
                if page_transactions and len(page_transactions) == limit and direction == "next"
                else None
            )
            prev_cursor = (
                page_transactions[0]["ts"]
                if page_transactions and cursor is not None and direction == "next"
                else None
            )

            return {
                "transactions": page_transactions,
                "next_cursor": next_cursor,
                "prev_cursor": prev_cursor,
                "total": total,
            }


@app.get("/metrics/gpu_stats")
def gpu_stats(
    period: str = Query(
        "week",
        enum=["week", "two_weeks", "month"],
        description="Time period: week, two_weeks, or month, default: week",
    ),
    metric: str = Query(
        "total_time_hours",
        description="Metric to return (default: total_time_hours)",
    ),
):
    """
    Returns GPU metrics for the specified period and metric.
    """
    return get_metrics_by_gpu(metric=metric, period=period)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
