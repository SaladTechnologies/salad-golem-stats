# Salad Stats Backend

TypeScript/Node.js API built with Fastify.

## Requirements

- Node.js 18+
- PostgreSQL 15+
- Redis 7+

## Development

```bash
npm install
npm run dev
```

The dev server runs at http://localhost:8000 with hot reload.

## Production Build

```bash
npm run build
npm start
```

## Docker

Build the image:

```bash
docker build -t salad-stats-backend .
```

Run the container:

```bash
docker run -p 8000:8000 \
  -e POSTGRES_HOST=your-db-host \
  -e POSTGRES_USER=your-user \
  -e POSTGRES_PASSWORD=your-password \
  -e POSTGRES_DB=statsdb \
  -e REDIS_HOST=your-redis-host \
  -e FRONTEND_ORIGINS=https://your-frontend.com \
  salad-stats-backend
```

## Deployment

This is a stateless container that can be deployed to any container service (AWS ECS, Google Cloud Run, Railway, Render, etc.).

### Container Settings

| Setting | Value |
|---------|-------|
| Port | `8000` |
| Health check | `GET /health` |
| Memory | 512MB recommended |

### Required Services

- **PostgreSQL** - Primary data store
- **Redis** - Caching layer

Configure connections via environment variables below.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8000` | Server port |
| `POSTGRES_HOST` | `localhost` | Database host |
| `POSTGRES_PORT` | `5432` | Database port |
| `POSTGRES_DB` | `statsdb` | Database name |
| `POSTGRES_USER` | `devuser` | Database user |
| `POSTGRES_PASSWORD` | `devpass` | Database password |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis database |
| `FRONTEND_ORIGINS` | `http://localhost:5173` | CORS allowed origins (comma-separated) |
| `CACHE_TTL_GEO` | `86400` | Geo endpoint cache TTL (seconds) |
| `CACHE_TTL_TRANSACTIONS` | `60` | Transactions endpoint cache TTL |
| `CACHE_TTL_PLAN_STATS` | `3600` | Plan stats endpoint cache TTL |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /metrics/plans` | Plan metrics with time series |
| `GET /metrics/geo_counts` | H3 hexagon aggregated geo data |
| `GET /metrics/transactions` | Paginated transactions |
