# Plan Importer

TypeScript service that imports SaladCloud plan data from MixPanel directly into PostgreSQL.

## Requirements

- Node.js 22+
- PostgreSQL 15+
- MixPanel API key

## Development

```bash
npm install
npm run dev
```

The service will:
1. Fetch earnings data from MixPanel JQL API for the previous 2 days
2. Import the data directly into PostgreSQL
3. Repeat every 6 hours

## Production Build

```bash
npm run build
npm start
```

## Docker

Build the image:

```bash
docker build -t salad-stats-plan-importer .
```

Run the container:

```bash
docker run \
  -e POSTGRES_HOST=your-db-host \
  -e POSTGRES_USER=your-user \
  -e POSTGRES_PASSWORD=your-password \
  -e POSTGRES_DB=statsdb \
  -e MIXPANEL_API_KEY=your-mixpanel-key \
  salad-stats-plan-importer
```

## Deployment

This is a stateless container that runs continuously, importing data on a schedule. It can be deployed to any container service (AWS ECS, Google Cloud Run, Railway, Render, etc.).

### Container Settings

| Setting | Value |
|---------|-------|
| Health check | Process running |
| Memory | 256MB recommended |

### Required Services

- **PostgreSQL** - Primary data store
- **MixPanel** - Data source (requires API key)

Configure connections via environment variables below.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | Database host |
| `POSTGRES_PORT` | `5432` | Database port |
| `POSTGRES_DB` | `statsdb` | Database name |
| `POSTGRES_USER` | `devuser` | Database user |
| `POSTGRES_PASSWORD` | `devpass` | Database password |
| `MIXPANEL_API_KEY` | (required) | MixPanel API key for JQL queries |
| `MIXPANEL_JQL_URL` | `https://mixpanel.com/api/query/jql` | MixPanel JQL endpoint |
| `DATA_DIRECTORY` | `./data` | Directory for JSON file staging |
| `MINIMUM_DURATION` | `600000` | Minimum plan duration in ms (10 min) |
| `IMPORT_INTERVAL` | `21600000` | Import interval in ms (6 hours) |

## How It Works

1. **Fetch**: Queries MixPanel JQL API for "Workload Earning" events grouped by organization, container group, and machine
2. **Stage**: Saves JSON responses to `data/pending/` directory
3. **Import**: Processes each JSON file, inserting plans into PostgreSQL with:
   - Organization name
   - Node ID
   - Start/stop timestamps
   - Invoice amount and hourly rate
   - GPU class, CPU, and RAM allocations
4. **Archive**: Moves processed files to `data/imported/` (or `data/failed/` on error)

## Database Tables

The importer creates and populates:

- `json_import_file` - Tracks imported JSON files (prevents duplicates)
- `node_plan` - Individual node work plans with earnings data
