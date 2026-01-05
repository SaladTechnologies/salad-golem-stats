# Salad Stats Dashboard

A dashboard for visualizing network statistics for SaladCloud's testing on the Golem Network. It includes a React frontend, a TypeScript/Node backend, PostgreSQL database, and Redis caching.

## Getting Started

### Prerequisites

- Docker and Docker Compose

### Running the Application

```bash
docker compose up
```

This starts all services with hot reload:
- **Frontend:** http://localhost:5173
- **Backend:** http://localhost:8000
- **PostgreSQL:** localhost:5432
- **Redis:** localhost:6379

Database migrations run automatically on first start.

### Loading Dev Data

To load the sample database dump:

```bash
docker compose exec db psql -U devuser -d statsdb -f /data/statsdb_dump.sql
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
| `GET /metrics/stats` | Summary statistics for a time period |
| `GET /metrics/trends` | Time series data with GPU/VRAM breakdowns |
| `GET /metrics/city_counts` | Geolocated node counts |
| `GET /metrics/geo_counts` | H3 hexagon-aggregated geo data |
| `GET /metrics/transactions` | Paginated transaction records |
| `GET /metrics/gpu_stats` | GPU-specific metric breakdowns |

Query parameters:
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
| `CACHE_TTL_CITY` | `86400` | City/geo endpoint cache TTL |
| `CACHE_TTL_TRANSACTIONS` | `60` | Transactions endpoint cache TTL |
| `CACHE_TTL_GPU` | `3600` | GPU stats endpoint cache TTL |

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
