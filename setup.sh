#!/bin/bash
# Setup script for Salad Stats Dashboard
# This script initializes the database with schema and imports data

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

# Step 3: Apply migrations (creates schema)
echo -e "${YELLOW}Step 3: Applying database migrations...${NC}"
for migration in db/migrations/*.sql; do
    if [ -f "$migration" ]; then
        filename=$(basename "$migration")
        echo "  Applying $filename..."
        docker compose exec -T db psql -U devuser -d statsdb -f "/data/migrations/$filename" > /dev/null 2>&1 || true
    fi
done
echo -e "${GREEN}Migrations applied.${NC}"
echo ""

# Step 4: Clear Redis cache
echo -e "${YELLOW}Step 4: Clearing Redis cache...${NC}"
docker compose exec -T redis redis-cli FLUSHALL > /dev/null 2>&1 || true
echo -e "${GREEN}Cache cleared.${NC}"
echo ""

echo "========================================"
echo -e "${GREEN}Setup complete!${NC}"
echo "========================================"
echo ""
echo "Services running:"
echo "  Frontend:      http://localhost:5173"
echo "  Backend:       http://localhost:8000"
echo "  Postgres:      localhost:5432"
echo "  Redis:         localhost:6379"
echo "  Plan Importer: Running (imports from MixPanel every 6 hours)"
echo ""
echo "To populate additional data, run:"
echo "  cd data-collection"
echo "  python get_gpu_classes.py            # GPU class reference data"
echo "  python get_geo_data.py               # City geo snapshots"
echo "  python generate_placeholder_transactions.py  # Demo transactions"
echo ""
echo "To view logs: docker compose logs -f"
echo "To stop:      docker compose down"
echo ""
