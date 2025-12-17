# Open a connection to the SQLite database in data/plans.db
import sqlite3
import os

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

# Extract the number of unique node_ids per stop_at by day from node_plan
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
		SELECT hour, 'gpu_total' as gpu_group,
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
		SELECT hour, 'total' as total_group,
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

	# Additional: daily distinct node counts by GPU class, any-gpu, and no-gpu

	# Hourly distinct node counts
	print("\nHourly distinct by GPU class:")
	distinct_hour_query = '''
		WITH per_node_hour AS (
			SELECT
				strftime('%Y-%m-%d %H:00:00', stop_at / 1000, 'unixepoch') AS hour,
				node_id,
				CASE WHEN gpu_class_id IS NULL OR gpu_class_id = '' THEN 'no_gpu' ELSE gpu_class_id END AS gpu_group
			FROM node_plan
			GROUP BY hour, node_id
		),
		all_nodes AS (
			SELECT hour, 'all' AS gpu_group, COUNT(DISTINCT node_id) AS unique_node_count
			FROM per_node_hour
			GROUP BY hour
		),
		any_gpu AS (
			SELECT hour, 'any_gpu' AS gpu_group, COUNT(DISTINCT node_id) AS unique_node_count
			FROM per_node_hour
			WHERE gpu_group != 'no_gpu'
			GROUP BY hour
		),
		no_gpu AS (
			SELECT hour, 'no_gpu' AS gpu_group, COUNT(DISTINCT node_id) AS unique_node_count
			FROM per_node_hour
			WHERE gpu_group = 'no_gpu'
			GROUP BY hour
		),
		by_gpu_class AS (
			SELECT hour, gpu_group, COUNT(DISTINCT node_id) AS unique_node_count
			FROM per_node_hour
			WHERE gpu_group != 'no_gpu'
			GROUP BY hour, gpu_group
		)
		SELECT * FROM all_nodes
		UNION ALL
		SELECT * FROM any_gpu
		UNION ALL
		SELECT * FROM no_gpu
		UNION ALL
		SELECT * FROM by_gpu_class
		ORDER BY hour, gpu_group;
	'''
	cursor.execute(distinct_hour_query)
	distinct_hour_results = cursor.fetchall()
	for row in distinct_hour_results[-50:]:
		print(f"Hour: {row[0]}, GPU Group: {row[1]}, Unique Nodes: {row[2]}")

	# Daily distinct node counts
	print("\nDaily distinct by GPU class:")
	distinct_day_query = '''
		WITH per_node_day AS (
			SELECT
				strftime('%Y-%m-%d', stop_at / 1000, 'unixepoch') AS day,
				node_id,
				CASE WHEN gpu_class_id IS NULL OR gpu_class_id = '' THEN 'no_gpu' ELSE gpu_class_id END AS gpu_group
			FROM node_plan
			GROUP BY day, node_id
		),
		all_nodes AS (
			SELECT day, 'all' AS gpu_group, COUNT(DISTINCT node_id) AS unique_node_count
			FROM per_node_day
			GROUP BY day
		),
		any_gpu AS (
			SELECT day, 'any_gpu' AS gpu_group, COUNT(DISTINCT node_id) AS unique_node_count
			FROM per_node_day
			WHERE gpu_group != 'no_gpu'
			GROUP BY day
		),
		no_gpu AS (
			SELECT day, 'no_gpu' AS gpu_group, COUNT(DISTINCT node_id) AS unique_node_count
			FROM per_node_day
			WHERE gpu_group = 'no_gpu'
			GROUP BY day
		),
		by_gpu_class AS (
			SELECT day, gpu_group, COUNT(DISTINCT node_id) AS unique_node_count
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
		print(f"Day: {row[0]}, GPU Group: {row[1]}, Unique Nodes: {row[2]}")

	cursor.close()
except Exception as e:
	print(f"Error querying node_plan: {e}")

