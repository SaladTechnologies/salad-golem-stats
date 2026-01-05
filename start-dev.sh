#!/bin/bash
set -e  # stop if any command fails

echo "Starting Postgres and Redis..."
cd db
docker-compose up -d

# Wait a few seconds for DB to be ready
echo "Waiting for Postgres and Redis to accept connections..."
sleep 5

# Apply init migration
echo "Applying DB migrations..."
docker exec -i dev-postgres psql -U devuser -d statsdb -f /migrations/001_init.sql
# docker exec dev-postgres pg_dump -U devuser -d statsdb > statsdb_dump.sql

echo "Starting backend..."
cd ../backend
# optional: activate venv
source ./venv/bin/activate
python main.py & BACKEND_PID=$!

#echo "Starting frontend..."
cd ../frontend
npm run dev & FRONTEND_PID=$!
