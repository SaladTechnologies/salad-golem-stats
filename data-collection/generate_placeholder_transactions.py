import os
import random
import psycopg2
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()


def get_db_conn():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
    )


import secrets


def random_eth_address():
    return "0x" + secrets.token_hex(20)


def random_tx_hash():
    return "0x" + secrets.token_hex(32)


def generate_placeholder_transactions(total_fake=103, window_days=31):
    now = datetime.now(timezone.utc)
    end_dt = now
    start_dt = end_dt - timedelta(days=window_days)

    # Generate 300 unique provider addresses
    providers = [random_eth_address() for _ in range(300)]
    requesters = [random_eth_address() for _ in range(10)]
    gpus = ["RTX 4090", "RTX 4080", "RTX 3090", "RTX 3060", "A100", "Other"]

    transactions = []
    for i in range(total_fake):
        ts = (
            start_dt + timedelta(seconds=i * (window_days * 24 * 3600) // total_fake)
        ).replace(microsecond=0)
        duration_minutes = random.randint(5, 120)
        duration = timedelta(minutes=duration_minutes)
        transactions.append(
            {
                "ts": ts,
                "provider_wallet": random.choice(providers),
                "requester_wallet": random.choice(requesters),
                "tx": random_tx_hash(),
                "gpu": random.choice(gpus),
                "ram": random.choice([8192, 16384, 20480, 32768, 65536]),
                "vcpus": random.choice([4, 8, 16, 32]),
                "duration": str(duration),
                "invoiced_glm": round(random.uniform(0.5, 10.0), 2),
                "invoiced_dollar": round(random.uniform(0.1, 5.0), 2),
            }
        )
    return transactions


def insert_transactions(transactions):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            for tx in transactions:
                cur.execute(
                    """
                    INSERT INTO placeholder_transactions
                    (ts, provider_wallet, requester_wallet, tx, gpu, ram, vcpus, duration, invoiced_glm, invoiced_dollar)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        tx["ts"],
                        tx["provider_wallet"],
                        tx["requester_wallet"],
                        tx["tx"],
                        tx["gpu"],
                        tx["ram"],
                        tx["vcpus"],
                        tx["duration"],
                        tx["invoiced_glm"],
                        tx["invoiced_dollar"],
                    ),
                )
            conn.commit()


def main():
    transactions = generate_placeholder_transactions()
    insert_transactions(transactions)
    print(f"Inserted {len(transactions)} placeholder transactions.")


if __name__ == "__main__":
    main()
