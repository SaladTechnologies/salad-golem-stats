# Salad Stats Dashboard

A dashboard for visualizing network statistics for SaladCloud's testing on the Golem Network. It includes a React frontend, a TypeScript/Node backend, PostgreSQL database, and Redis caching.

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
2. Load the base data dump (`statsdb_dump.sql`)
3. Apply all database migrations
4. Import plans data from `db/plans.db`
5. Clear the Redis cache

Services will be available at:
- **Frontend:** http://localhost:5173
- **Backend:** http://localhost:8000
- **PostgreSQL:** localhost:5432
- **Redis:** localhost:6379

### Manual Setup

If you prefer manual setup:

```bash
# Start services
docker compose up -d --build

# Load base data
docker compose exec db psql -U devuser -d statsdb -f /data/statsdb_dump.sql

# Apply migrations
docker compose exec db psql -U devuser -d statsdb -f /data/migrations/002_plans_tables.sql

# Import plans data (requires Python)
cd data-collection
pip install psycopg2-binary python-dotenv
python import_plans_db.py --clear
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

### Production Deployment

```bash
docker compose -f docker-compose.prod.yaml up -d --build
```

## Project Structure

```
├── docker-compose.yaml       # Development (hot reload)
├── docker-compose.prod.yaml  # Production (built images)
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
├── db/
│   └── migrations/           # SQL migration files
└── data-collection/          # Python scripts for data ingestion
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /metrics/plans` | Plan metrics with time series (primary endpoint) |
| `GET /metrics/stats` | Summary statistics (legacy) |
| `GET /metrics/trends` | Time series data (legacy) |
| `GET /metrics/geo_counts` | H3 hexagon-aggregated geo data |
| `GET /metrics/transactions` | Paginated transaction records |

### `/metrics/plans` (Primary)

Query parameters:
- `period`: `6h`, `24h`, `7d`, `30d`, `90d`, `total` (default: `7d`)

Response includes:
- `totals`: active_nodes, total_fees, compute_hours, transactions, core_hours, ram_hours, gpu_hours
- `time_series`: Hourly/daily breakdown of all metrics
- `gpu_hours_by_model`: GPU compute hours grouped by model
- `gpu_hours_by_vram`: GPU compute hours grouped by VRAM
- `active_nodes_by_gpu_model`: Active nodes grouped by GPU model
- `active_nodes_by_vram`: Active nodes grouped by VRAM

Note: Data has a 2-day offset (data not yet processed by Golem is excluded).

### Legacy Endpoints

Query parameters for `/metrics/stats` and `/metrics/trends`:
- `period`: `day`, `week`, `two_weeks`, `month`
- `gpu`: `all`, `any_gpu`, `no_gpu`, or specific GPU class ID

## Environment Variables

Each service has its own `.env` file. Copy the example files to get started:

```bash
cp backend/.env.example backend/.env
cp frontend/.env.example frontend/.env
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
| `CACHE_TTL_STATS` | `3600` | Stats endpoint cache TTL (seconds) |
| `CACHE_TTL_TRENDS` | `3600` | Trends endpoint cache TTL |
| `CACHE_TTL_TRANSACTIONS` | `60` | Transactions endpoint cache TTL |

### Frontend (`frontend/.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `VITE_STATS_API_URL` | `http://localhost:8000` | Backend API URL |

> **Note:** When running with Docker Compose, the `POSTGRES_HOST` and `REDIS_HOST` values are automatically overridden to use container networking (`db` and `redis` respectively).

### Data Collection (`data-collection/.env`)

Required for running data ingestion scripts:

```
POSTGRES_USER=devuser
POSTGRES_PASSWORD=devpass
POSTGRES_HOST=localhost
POSTGRES_DB=statsdb
POSTGRES_PORT=5432

# MongoDB credentials (for get_data.py)
MONGOUSER=
MONGOPASS=
MONGO_URL=matrix-production-1.qltsap.mongodb.net
DBNAME=matrix

# Strapi credentials
STRAPIURL=https://cms-api.salad.io
STRAPIID=
STRAPIPW=
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
