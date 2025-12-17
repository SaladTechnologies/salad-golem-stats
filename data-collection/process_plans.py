import psycopg2
from dotenv import load_dotenv
import sqlite3
import os

load_dotenv() 

def get_db_conn():
	return psycopg2.connect(
		dbname=os.getenv("POSTGRES_DB"),
		user=os.getenv("POSTGRES_USER"),
		password=os.getenv("POSTGRES_PASSWORD"),
		host=os.getenv("POSTGRES_HOST", "localhost"),
		port=int(os.getenv("POSTGRES_PORT", 5432))
	)

def insert_hourly_gpu_stats(data):
	conn = get_db_conn()
	cur = conn.cursor()
	cur.executemany('''
		INSERT INTO hourly_gpu_stats (hour, gpu_group, total_time_seconds, total_invoice_amount, total_ram_hours, total_cpu_hours, total_transaction_count)
		VALUES (%s, %s, %s, %s, %s, %s, %s)
	''', data)
	conn.commit()
	cur.close()
	conn.close()

def insert_hourly_distinct_counts(data):
	conn = get_db_conn()
	cur = conn.cursor()
	cur.executemany('''
		INSERT INTO hourly_distinct_counts (hour, gpu_group, unique_node_count, unique_node_ram, unique_node_cpu)
		VALUES (%s, %s, %s, %s, %s)
	''', data)
	conn.commit()
	cur.close()
	conn.close()

def insert_daily_distinct_counts(data):
	conn = get_db_conn()
	cur = conn.cursor()
	cur.executemany('''
		INSERT INTO daily_distinct_counts (day, gpu_group, unique_node_count, unique_node_ram, unique_node_cpu)
		VALUES (%s, %s, %s, %s, %s)
	''', data)
	conn.commit()
	cur.close()
	conn.close()


# Construct the path to the database
db_path = os.path.join( '.', 'data', 'plans.db')
db_path = os.path.abspath(db_path)

conn = sqlite3.connect(db_path)

print(f"Connected to database at {db_path}")

# List all tables in the database
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables:")
for table in tables:
	table_name = table[0]
	print(f"- {table_name}")
	# List keys (columns) for each table
	cursor.execute(f"PRAGMA table_info({table_name});")
	columns = cursor.fetchall()
	print("  Columns:")
	for col in columns:
		# col[1] is the column name, col[2] is the type
		print(f"    - {col[1]} ({col[2]})")

print("\n Resource Usage in node_plan:")
query = '''
	WITH base AS (
		SELECT
			strftime('%Y-%m-%d %H:00:00', stop_at / 1000, 'unixepoch') AS hour,
			node_id,
			(stop_at - start_at) / 1000 AS duration,
			CASE WHEN gpu_class_id IS NULL OR gpu_class_id = '' THEN 'no_gpu' ELSE gpu_class_id END AS gpu_group,
			invoice_amount,
			ram,
			cpu
		FROM node_plan
	),
	base_with_resource_hours AS (
		SELECT	
			*,
			ram * duration AS ram_hours,
			cpu * duration AS cpu_hours
		FROM base
	), 
	breakdown AS (
		SELECT hour, gpu_group,
			SUM(duration) AS total_time_seconds,
			SUM(invoice_amount) AS total_invoice_amount,
			SUM(ram_hours) AS total_ram_hours,
			SUM(cpu_hours) AS total_cpu_hours,
			COUNT(*) AS total_transaction_count
		FROM base_with_resource_hours
		GROUP BY hour, gpu_group
	),
	gpu_total AS (
		SELECT hour, 'any_gpu' as gpu_group,
			SUM(duration) AS total_time_seconds,
			SUM(invoice_amount) AS total_invoice_amount	,
			SUM(ram_hours) AS total_ram_hours,
			SUM(cpu_hours) AS total_cpu_hours,
			COUNT(*) AS total_transaction_count
		FROM base_with_resource_hours
		WHERE gpu_group != 'no_gpu'
		GROUP BY hour
	),
	total AS (
		SELECT hour, 'all' as total_group,
			SUM(duration) AS total_time_seconds,
			SUM(invoice_amount) AS total_invoice_amount,
			SUM(ram_hours) AS total_ram_hours,
			SUM(cpu_hours) AS total_cpu_hours,
			COUNT(*) AS total_transaction_count
		FROM base_with_resource_hours
		GROUP BY hour
	)
	SELECT * FROM total
	UNION ALL
	SELECT * FROM gpu_total
	UNION ALL
	SELECT * FROM breakdown
	ORDER BY hour, gpu_group;
'''

try:
	cursor = conn.cursor()
	cursor.execute(query)
	results = cursor.fetchall()
	for row in results:
		print(f"Hour: {row[0]}, GPU Group: {row[1]}, Total time running (s): {row[2]}, Total invoice amount: {row[3]}, Total RAM hours: {row[4]}, Total CPU hours: {row[5]}, Total transactions: {row[6]}")

	# Insert hourly GPU stats into Postgres
	hourly_gpu_stats_data = [
		(
			row[0], # hour (timestamp)
			row[1], # gpu_group
			row[2], # total_time_seconds
			row[3], # total_invoice_amount
			row[4], # total_ram_hours
			row[5], # total_cpu_hours
			row[6], # total_transaction_count
		) for row in results
	]
	insert_hourly_gpu_stats(hourly_gpu_stats_data)

	# Daily distinct node counts with max RAM/CPU

	#### THIS WOULD BE BETTER WITH NODE WSL RAM AND CPU!!!!

	print("\nDaily distinct by GPU class (with max total RAM/CPU):")
	distinct_day_query = '''
		WITH per_node_day AS (
			SELECT
				strftime('%Y-%m-%d', stop_at / 1000, 'unixepoch') AS day,
				node_id,
				CASE WHEN gpu_class_id IS NULL OR gpu_class_id = '' THEN 'no_gpu' ELSE gpu_class_id END AS gpu_group,
				MAX(ram) AS max_ram,
				MAX(cpu) AS max_cpu
			FROM node_plan
			GROUP BY day, node_id, gpu_group
		),
		all_nodes AS (
			SELECT day, 'all' AS gpu_group, COUNT(DISTINCT node_id) AS unique_node_count, SUM(max_ram) AS total_max_ram, SUM(max_cpu) AS total_max_cpu
			FROM per_node_day
			GROUP BY day
		),
		any_gpu AS (
			SELECT day, 'any_gpu' AS gpu_group, COUNT(DISTINCT node_id) AS unique_node_count, SUM(max_ram) AS total_max_ram, SUM(max_cpu) AS total_max_cpu
			FROM per_node_day
			WHERE gpu_group != 'no_gpu'
			GROUP BY day
		),
		no_gpu AS (
			SELECT day, 'no_gpu' AS gpu_group, COUNT(DISTINCT node_id) AS unique_node_count, SUM(max_ram) AS total_max_ram, SUM(max_cpu) AS total_max_cpu
			FROM per_node_day
			WHERE gpu_group = 'no_gpu'
			GROUP BY day
		),
		by_gpu_class AS (
			SELECT day, gpu_group, COUNT(DISTINCT node_id) AS unique_node_count, SUM(max_ram) AS total_max_ram, SUM(max_cpu) AS total_max_cpu
			FROM per_node_day
			WHERE gpu_group != 'no_gpu'
			GROUP BY day, gpu_group
		)
		SELECT * FROM all_nodes
		UNION ALL
		SELECT * FROM any_gpu
		UNION ALL
		SELECT * FROM no_gpu
		UNION ALL
		SELECT * FROM by_gpu_class
		ORDER BY day, gpu_group;
	'''

	cursor.execute(distinct_day_query)
	distinct_day_results = cursor.fetchall()
	for row in distinct_day_results[-50:]:
		print(f"Day: {row[0]}, GPU Group: {row[1]}, Unique Nodes: {row[2]}, Total Max RAM: {row[3]}, Total Max CPU: {row[4]}")

	# Insert daily distinct counts into Postgres (if schema supports extra columns)
	daily_distinct_data = [
	(
		row[0], # day (date)
	    row[1], # gpu_group
	    row[2], # unique_node_count
	    row[3], # total_max_ram
	    row[4], # total_max_cpu
	) for row in distinct_day_results
	]

	insert_daily_distinct_counts(daily_distinct_data)

	# Compute and print sum of max(ram) and max(cpu) per node per day, grouped by gpu_group
	print("\nDaily sum of max RAM and CPU per node, by group:")
	distinct_hour_query = '''
		WITH per_node_day AS (
			SELECT
				strftime('%Y-%m-%d', stop_at / 1000, 'unixepoch') AS day,
				node_id,
				CASE WHEN gpu_class_id IS NULL OR gpu_class_id = '' THEN 'no_gpu' ELSE gpu_class_id END AS gpu_group,
				MAX(ram) AS max_ram,
				MAX(cpu) AS max_cpu
			FROM node_plan
			GROUP BY day, node_id, gpu_group
		),
		all_nodes AS (
			SELECT day, 'all' AS gpu_group, COUNT(DISTINCT node_id) AS unique_node_count, SUM(max_ram) AS total_max_ram, SUM(max_cpu) AS total_max_cpu
			FROM per_node_day
			GROUP BY day
		),
		any_gpu AS (
			SELECT day, 'any_gpu' AS gpu_group, COUNT(DISTINCT node_id) AS unique_node_count, SUM(max_ram) AS total_max_ram, SUM(max_cpu) AS total_max_cpu
			FROM per_node_day
			WHERE gpu_group != 'no_gpu'
			GROUP BY day
		),
		no_gpu AS (
			SELECT day, 'no_gpu' AS gpu_group, COUNT(DISTINCT node_id) AS unique_node_count, SUM(max_ram) AS total_max_ram, SUM(max_cpu) AS total_max_cpu
			FROM per_node_day
			WHERE gpu_group = 'no_gpu'
			GROUP BY day
		),
		by_gpu_class AS (
			SELECT day, gpu_group, COUNT(DISTINCT node_id) AS unique_node_count, SUM(max_ram) AS total_max_ram, SUM(max_cpu) AS total_max_cpu
			FROM per_node_day
			WHERE gpu_group != 'no_gpu'
			GROUP BY day, gpu_group
		)
		SELECT * FROM all_nodes
		UNION ALL
		SELECT * FROM any_gpu
		UNION ALL
		SELECT * FROM no_gpu
		UNION ALL
		SELECT * FROM by_gpu_class
		ORDER BY day, gpu_group;
	'''
	cursor.execute(distinct_hour_query)
	distinct_hour_results = cursor.fetchall()
	for row in distinct_hour_results[-50:]:
		print(f"Day: {row[0]}, GPU Group: {row[1]}, Unique Node Count: {row[2]}, Total Max RAM: {row[3]}, Total Max CPU: {row[4]}")

	# Insert daily distinct counts into Postgres
	hour_distinct_data = [
		(
			row[0], # day (date)
			row[1], # gpu_group
			row[2], # unique_node_count
			row[3], # ram 
			row[4], # cpu
		) for row in distinct_hour_results
	]
	insert_hourly_distinct_counts(hour_distinct_data)

	cursor.close()

except Exception as e:
	print(f"Error querying node_plan: {e}")

