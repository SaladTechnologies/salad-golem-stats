#!/bin/bash

echo "Tearing down test environment..."

# Move to project root
cd "$(dirname "$0")/../.."

# Parse arguments
REMOVE_VOLUMES=false
if [ "$1" = "--clean" ] || [ "$1" = "-c" ]; then
  REMOVE_VOLUMES=true
fi

# Stop docker services
echo "Stopping PostgreSQL and Redis..."
docker-compose stop db redis

if [ "$REMOVE_VOLUMES" = true ]; then
  echo "Removing volumes and test data..."
  docker-compose down -v
  echo "✓ Test environment completely removed (including data)"
else
  echo "✓ Test services stopped (data preserved)"
  echo ""
  echo "To completely remove test data and volumes, run:"
  echo "  npm run teardown-tests -- --clean"
fi
