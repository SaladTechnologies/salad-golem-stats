#!/bin/bash
set -e

echo "Setting up test environment..."

# Start docker services from project root
cd "$(dirname "$0")/../.."
echo "Starting PostgreSQL and Redis..."
docker-compose up -d db redis

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
docker-compose exec -T db sh -c 'until pg_isready -U devuser; do sleep 1; done'

# Create test database if it doesn't exist
echo "Creating test database (if needed)..."
docker-compose exec -T db psql -U devuser -d statsdb -tc "SELECT 1 FROM pg_database WHERE datname = 'statsdb_test'" | grep -q 1 || \
  docker-compose exec -T db psql -U devuser -d statsdb -c "CREATE DATABASE statsdb_test"

echo "✓ Test environment ready!"
echo ""
echo "You can now run:"
echo "  npm test                 - Run all tests"
echo "  npm run test:unit        - Run unit tests only"
echo "  npm run test:integration - Run integration tests only"
