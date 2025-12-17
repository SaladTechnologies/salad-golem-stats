import psycopg2
import os
from dotenv import load_dotenv

load_dotenv() 


def get_db_conn():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432))
    )

def print_postgres_tables_info():
    conn = get_db_conn()
    cur = conn.cursor()
    # Get all table names
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    tables = cur.fetchall()
    for (table_name,) in tables:
        print(f"Table: {table_name}")
        # Get columns
        cur.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        columns = cur.fetchall()
        print("  Columns:")
        for col_name, col_type in columns:
            print(f"    - {col_name} ({col_type})")
        # Get row count
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cur.fetchone()[0]
        print(f"  Number of rows: {count}")

        # Print last 5 entries (if any rows)
        if count > 0:
            # Get column names for printing
            col_names = [col[0] for col in columns]
            # Try to use 'ORDER BY id DESC' if 'id' column exists, else just use no order
            if 'id' in col_names:
                order_clause = 'ORDER BY id DESC'
            elif 'ts' in col_names:
                order_clause = 'ORDER BY ts DESC'
            else:
                order_clause = ''
            query = f"SELECT * FROM {table_name} {order_clause} LIMIT 5;"
            cur.execute(query)
            rows = cur.fetchall()
            print("  Last 5 entries:")
            for row in rows:
                # Print as dict for clarity
                print("    ", dict(zip(col_names, row)))
        print()
    cur.close()
    conn.close()

# Usage:
print_postgres_tables_info()