# Salad Stats Dashboard

A dashboard for visualizing network statistics for SaladCloud's testing on the Golem Network. It includes a React frontend, a TypeScript/Node backend, PostgreSQL database, Redis caching, and a plan importer service for ingesting data from MixPanel.

## Getting Started

### Prerequisites

- Docker and Docker Compose

### Quick Start

Run the setup script to start all services and load data:

```bash
./setup.sh
```

This will:
1. Start all Docker services
2. Apply all database migrations
3. Clear the Redis cache

The plan-importer service will automatically fetch and import plan data from MixPanel on startup and every 6 hours.

Services will be available at:
- **Frontend:** http://localhost:5173
- **Backend:** http://localhost:8000
- **PostgreSQL:** localhost:5432
- **Redis:** localhost:6379
- **Plan Importer:** Runs in background, importing data every 6 hours

### Manual Setup

If you prefer manual setup:

```bash
# Start services
docker compose up -d --build

# Apply migrations
for f in db/migrations/*.sql; do
  docker compose exec db psql -U devuser -d statsdb -f "/data/migrations/$(basename $f)"
done
```

### Common Commands

```bash
docker compose up           # Start all services
docker compose up -d        # Start in background
docker compose down         # Stop all services
docker compose logs -f      # View logs
docker compose logs backend # View backend logs only

# Clear Redis cache
docker compose exec redis redis-cli FLUSHALL

# Access PostgreSQL
docker compose exec db psql -U devuser -d statsdb
```

## Project Structure

```
├── docker-compose.yaml       # Development (hot reload)
├── backend/                  # TypeScript/Node API (Fastify)
│   ├── src/
│   │   ├── index.ts          # App entry point
│   │   ├── config.ts         # Environment configuration
│   │   ├── routes/           # API route handlers
│   │   ├── services/         # Business logic
│   │   ├── db/               # Database connection
│   │   ├── cache/            # Redis caching
│   │   └── types/            # TypeScript interfaces
│   ├── Dockerfile
│   └── package.json
├── frontend/                 # React app (Vite)
│   ├── src/
│   ├── Dockerfile
│   ├── nginx.conf            # Production nginx config
│   └── package.json
├── plan-importer/            # MixPanel to PostgreSQL importer
│   ├── src/
│   │   ├── index.ts          # Service entry point
│   │   ├── config.ts         # Environment configuration
│   │   ├── mixpanel.ts       # MixPanel JQL API client
│   │   ├── planner.ts        # PostgreSQL import logic
│   │   ├── db.ts             # Database connection
│   │   └── logger.ts         # Pino logger
│   ├── jql/                  # MixPanel JQL query files
│   ├── Dockerfile
│   └── package.json
├── db/
│   └── migrations/           # SQL migration files
└── data-collection/          # Python scripts for data ingestion
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /metrics/plans` | Plan metrics with time series |
| `GET /metrics/geo_counts` | H3 hexagon-aggregated geo data |
| `GET /metrics/transactions` | Paginated transaction records |

### `/metrics/plans`

Query parameters:
- `period`: `6h`, `24h`, `7d`, `30d`, `90d`, `total` (default: `7d`)

Response includes:
- `totals`: active_nodes, total_fees, compute_hours, core_hours, ram_hours, gpu_hours
- `time_series`: Hourly/daily breakdown of all metrics
- `gpu_hours_by_model`: GPU compute hours grouped by model
- `gpu_hours_by_vram`: GPU compute hours grouped by VRAM
- `active_nodes_by_gpu_model`: Active nodes grouped by GPU model
- `active_nodes_by_vram`: Active nodes grouped by VRAM
- `*_ts`: Time series versions of GPU breakdowns for stacked charts

**Time series calculation:** Metrics use overlap-based calculation - compute hours, core hours, RAM hours, and GPU hours are distributed across time buckets based on when jobs were actually running (not just when they completed). This provides smooth, accurate time series data. Fees are attributed to the bucket when the job completed.

Note: Data has a 2-day offset (data not yet processed by Golem is excluded).

## Environment Variables

Each service has its own `.env` file. Copy the example files to get started:

```bash
cp backend/.env.example backend/.env
cp frontend/.env.example frontend/.env
cp plan-importer/.env.example plan-importer/.env
```

### Backend (`backend/.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `FRONTEND_ORIGINS` | `http://localhost:5173` | CORS allowed origins (comma-separated) |
| `POSTGRES_USER` | `devuser` | Database username |
| `POSTGRES_PASSWORD` | `devpass` | Database password |
| `POSTGRES_HOST` | `localhost` | Database host |
| `POSTGRES_DB` | `statsdb` | Database name |
| `POSTGRES_PORT` | `5432` | Database port |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis database number |
| `CACHE_TTL_GEO` | `86400` | Geo counts endpoint cache TTL (seconds) |
| `CACHE_TTL_TRANSACTIONS` | `60` | Transactions endpoint cache TTL |
| `CACHE_TTL_PLAN_STATS` | `3600` | Plan stats endpoint cache TTL |
| `CACHE_WARMER_ENABLED` | `true` | Enable proactive cache warming |
| `CACHE_WARMER_INTERVAL_RATIO` | `0.8` | Warm interval as ratio of TTL (0.8 = every 48 min for 1hr TTL) |

### Frontend (`frontend/.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `VITE_STATS_API_URL` | `http://localhost:8000` | Backend API URL |

> **Note:** When running with Docker Compose, the `POSTGRES_HOST` and `REDIS_HOST` values are automatically overridden to use container networking (`db` and `redis` respectively).

### Plan Importer (`plan-importer/.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | Database host |
| `POSTGRES_PORT` | `5432` | Database port |
| `POSTGRES_DB` | `statsdb` | Database name |
| `POSTGRES_USER` | `devuser` | Database user |
| `POSTGRES_PASSWORD` | `devpass` | Database password |
| `MIXPANEL_API_KEY` | (required) | MixPanel API key for JQL queries |
| `DATA_DIRECTORY` | `./data` | Directory for JSON file staging |
| `MINIMUM_DURATION` | `600000` | Minimum plan duration in ms (10 min) |
| `IMPORT_INTERVAL` | `21600000` | Import interval in ms (6 hours) |

### Data Collection (`data-collection/.env`)

Required for running data ingestion scripts. Note that some scripts require internal Salad credentials and are primarily for internal use.

```
POSTGRES_USER=devuser
POSTGRES_PASSWORD=devpass
POSTGRES_HOST=localhost
POSTGRES_DB=statsdb
POSTGRES_PORT=5432
```

## Development

### Running without Docker

If you prefer to run services locally:

```bash
# Start only db and redis
docker compose up -d db redis

# Run backend
cd backend
npm install
npm run dev

# Run frontend (in another terminal)
cd frontend
npm install
npm run dev
```

### Building for Production

```bash
# Backend
cd backend
npm run build

# Frontend
cd frontend
npm run build
```
