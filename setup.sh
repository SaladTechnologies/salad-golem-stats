#!/bin/bash
# Setup script for Salad Stats Dashboard
# This script initializes the database with all required data

set -e

echo "========================================"
echo "Salad Stats Dashboard Setup"
echo "========================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Step 1: Start services
echo -e "${YELLOW}Step 1: Starting Docker services...${NC}"
docker compose up -d --build
echo -e "${GREEN}Services started.${NC}"
echo ""

# Step 2: Wait for database to be healthy
echo -e "${YELLOW}Step 2: Waiting for database to be ready...${NC}"
until docker compose exec -T db pg_isready -U devuser -d statsdb > /dev/null 2>&1; do
    echo "  Waiting for PostgreSQL..."
    sleep 2
done
echo -e "${GREEN}Database is ready.${NC}"
echo ""

# Step 3: Load the base data dump (statsdb_dump.sql)
echo -e "${YELLOW}Step 3: Loading base data dump...${NC}"
docker compose exec -T db psql -U devuser -d statsdb -f /data/statsdb_dump.sql > /dev/null 2>&1 || true
echo -e "${GREEN}Base data loaded.${NC}"
echo ""

# Step 4: Apply migrations (including new plans tables)
echo -e "${YELLOW}Step 4: Applying database migrations...${NC}"
for migration in db/migrations/*.sql; do
    if [ -f "$migration" ]; then
        filename=$(basename "$migration")
        echo "  Applying $filename..."
        docker compose exec -T db psql -U devuser -d statsdb -f "/data/migrations/$filename" > /dev/null 2>&1 || true
    fi
done
echo -e "${GREEN}Migrations applied.${NC}"
echo ""

# Step 5: Import plans.db data
echo -e "${YELLOW}Step 5: Importing plans data from SQLite...${NC}"
if [ -f "db/plans.db" ]; then
    # Check if Python and dependencies are available
    if command -v python3 &> /dev/null; then
        cd data-collection
        pip install -q psycopg2-binary python-dotenv 2>/dev/null || true
        python3 import_plans_db.py --clear 2>&1 | tail -5
        cd ..
        echo -e "${GREEN}Plans data imported.${NC}"
    else
        echo -e "${YELLOW}Warning: Python3 not found. Skipping plans import.${NC}"
        echo "  Run manually: cd data-collection && python import_plans_db.py --clear"
    fi
else
    echo -e "${YELLOW}Warning: db/plans.db not found. Skipping plans import.${NC}"
fi
echo ""

# Step 6: Clear Redis cache
echo -e "${YELLOW}Step 6: Clearing Redis cache...${NC}"
docker compose exec -T redis redis-cli FLUSHALL > /dev/null 2>&1 || true
echo -e "${GREEN}Cache cleared.${NC}"
echo ""

echo "========================================"
echo -e "${GREEN}Setup complete!${NC}"
echo "========================================"
echo ""
echo "Services running:"
echo "  Frontend: http://localhost:5173"
echo "  Backend:  http://localhost:8000"
echo "  Postgres: localhost:5432"
echo "  Redis:    localhost:6379"
echo ""
echo "To view logs: docker compose logs -f"
echo "To stop:      docker compose down"
echo ""
