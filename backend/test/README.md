# Integration Tests

This directory contains integration tests that run against a real PostgreSQL database.

## Prerequisites

1. PostgreSQL running locally (or configure via environment variables)
2. A test database created (default: `statsdb_test`)

## Setup

### Quick Setup (Recommended)

Run the automated setup script:

```bash
npm run setup-tests
```

This will:
- Start PostgreSQL and Redis with docker-compose
- Wait for services to be ready
- Create the test database automatically

### Manual Setup (Alternative)

If you prefer manual setup:

```bash
createdb statsdb_test
```

Or if using Docker/docker-compose (from project root):

```bash
docker-compose up -d db redis
docker exec -it salad-stats-db psql -U devuser -d statsdb -c "CREATE DATABASE statsdb_test;"
```

### Configure Environment (optional)

Copy `.env.test.example` to `.env.test` and customize if needed:

```bash
cp .env.test.example .env.test
```

The tests will use these defaults if not configured:
- `TEST_POSTGRES_HOST`: localhost
- `TEST_POSTGRES_PORT`: 5432
- `TEST_POSTGRES_DB`: statsdb_test
- `TEST_POSTGRES_USER`: devuser
- `TEST_POSTGRES_PASSWORD`: devpass

## Running Tests

Run all tests (unit + integration):
```bash
npm test
```

Run only integration tests:
```bash
npm run test:integration
```

Run only unit tests:
```bash
npm run test:unit
```

Watch mode (all tests):
```bash
npm run test:watch
```

## Cleanup

Stop test services (preserves data):
```bash
npm run teardown-tests
```

Stop test services and remove all data/volumes:
```bash
npm run teardown-tests -- --clean
```

## Configuration

Integration tests use environment variables with `TEST_` prefix. Defaults:

- `TEST_POSTGRES_HOST`: localhost
- `TEST_POSTGRES_PORT`: 5432
- `TEST_POSTGRES_DB`: statsdb_test
- `TEST_POSTGRES_USER`: devuser
- `TEST_POSTGRES_PASSWORD`: devpass

Override these in your environment or `.env` file:

```bash
export TEST_POSTGRES_DB=my_test_db
npm run test:integration
```

## What's Tested

### Golem Stats Integration Tests (`golemStats.integration.test.ts`)

- **Authentication**: Bearer token validation, timing-safe comparison
- **GET /v1/network/stats**: Current network snapshot
  - Response structure validation
  - Computing nodes match online nodes
  - Non-zero resource values
  - Timestamp accuracy
- **GET /v1/network/stats/historical**: Historical data
  - Network stats (VM and VM-NVIDIA)
  - Utilization data (720 points at 30-second intervals)
  - Computing daily totals
  - Data point structure validation
  - Chronological ordering

## Test Structure

- `dbSetup.ts`: Database utilities for setup, teardown, and test data insertion
- `fixtures.ts`: Test data fixtures (node plans, GPU classes)
- `*.integration.test.ts`: Integration test suites

## Notes

- Tests automatically setup and teardown the database schema
- Each test gets a clean database state with fresh fixtures
- 48-hour data offset is applied to align with production `planMetrics` logic
- Integration tests have 30-second timeouts to accommodate database operations
