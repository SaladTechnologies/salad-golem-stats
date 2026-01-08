# Data Collection Scripts Documentation

This directory contains Python scripts for collecting, processing, and importing data for the Salad Stats Dashboard. These scripts handle various aspects of data pipeline management, from importing historical data to fetching real-time node information.

## Overview

The data collection system supports the dashboard by:
- Importing historical plan data from SQLite to PostgreSQL
- Fetching current node geographic distribution for the 3D globe
- Managing GPU class information from Strapi CMS
- Generating test transaction data for development

## Prerequisites

### Environment Setup

1. **Install Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Variables** (create `.env` file):
   ```bash
   # PostgreSQL Connection
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DB=statsdb
   POSTGRES_USER=devuser
   POSTGRES_PASSWORD=devpass

   # MongoDB Connection (for node data)
   MONGOUSER=your_mongo_username
   MONGOPASS=your_mongo_password
   DBNAME=your_database_name
   MONGO_URL=your_mongo_cluster_url

   # Strapi CMS (for GPU classes)
   STRAPIURL=https://your-strapi-instance.com/api
   STRAPIID=your_strapi_username
   STRAPIPW=your_strapi_password

   # Node filtering
   MIN_SEL=2004000 minimum node selector version
   ```

## Scripts Documentation

### 1. `import_plans_db.py`

**Purpose**: Imports historical plan data from SQLite database to PostgreSQL for dashboard metrics.

**Usage**:
```bash
python import_plans_db.py [--clear]
```

**Options**:
- `--clear`: Truncate existing tables before import (recommended for clean imports)

**What it does**:
- Connects to SQLite database (`../db/plans.db`)
- Imports three tables in dependency order:
  1. `json_import_file` - Import file metadata
  2. `node_plan` - Individual node plan records
  3. `node_plan_job` - Job execution details within plans
- Uses batched imports (5000 records) for efficiency
- Handles conflicts with upsert logic
- Updates PostgreSQL sequences after import

**Database Tables Created/Updated**:
- `json_import_file`: Tracks data import sources
- `node_plan`: Core plan data with node info, timing, resources
- `node_plan_job`: Individual job executions within plans

**Example Output**:
```
Importing json_import_file...
  Total rows to import: 150
  Progress: 150/150 (100%)
  Imported 150 rows

Importing node_plan...
  Total rows to import: 45231
  Progress: 45231/45231 (100%)
  Imported 45231 rows
```

---

### 2. `get_geo_data.py`

**Purpose**: Fetches current node distribution data for the 3D globe visualization.

**Usage**:
```bash
python get_geo_data.py
```

**What it does**:
- Connects to MongoDB to fetch live node data
- Filters nodes based on:
  - Last seen within 24 hours
  - Minimum selector version (MIN_SEL)
  - Optional: running status, workload presence, organization
- Geocodes node cities using OpenStreetMap Nominatim API
- Caches geocoding results locally to avoid API rate limits
- Saves aggregated city-level data to PostgreSQL `city_snapshots` table
- Clears old data before inserting new snapshots

**Key Functions**:
- `get_node_data()`: Fetches and filters MongoDB node records
- `add_lat_long_to_data()`: Geocodes city locations with caching
- `save_data_to_database()`: Stores aggregated data in PostgreSQL
- `clear_existing_data()`: Removes old city snapshots

**Geocoding Cache**:
- `data/city_geocode_cache.json`: Persistent cache of city coordinates
- Reduces API calls and improves performance
- Uses OpenStreetMap Nominatim with 1-second rate limiting

**Example Output**:
```
Processing 15847 nodes...
City cache hit rate: 85.3%
Made 127 new geocoding requests
Cleared existing city snapshots
Saved 1,247 city records to database
```

---

### 3. `get_gpu_classes.py`

**Purpose**: Synchronizes GPU class information from Strapi CMS to PostgreSQL.

**Usage**:
```bash
python get_gpu_classes.py
```

**What it does**:
- Authenticates with Strapi CMS using JWT
- Fetches GPU class definitions including:
  - GPU model names
  - VRAM specifications
  - Performance characteristics
  - Pricing information
- Updates PostgreSQL `gpu_classes` table
- Maps UUID identifiers to human-readable names

**Key Functions**:
- `getStrapiJwt()`: Authenticates and retrieves JWT token
- `getGpuClasses()`: Fetches GPU class data from Strapi API
- Database upsert with conflict resolution

**Data Fields Synchronized**:
- `gpu_class_id`: UUID identifier
- `gpu_class_name`: Human-readable model name
- `vram_gb`: Video memory capacity
- `compute_capability`: Performance metrics

---

### 4. `generate_placeholder_transactions.py`

**Purpose**: Creates synthetic transaction data for development and testing.

**Usage**:
```bash
python generate_placeholder_transactions.py
```

**What it does**:
- Generates realistic fake transaction records
- Creates random but consistent:
  - Ethereum addresses for providers/requesters
  - Transaction hashes
  - GPU assignments from predefined list
  - Timestamps across specified window
  - Fee amounts and durations
- Inserts data into `transactions` table
- Useful for testing dashboard functionality without real transaction data

**Generated Data**:
- 103 transactions by default
- 31-day time window
- 300 unique provider addresses
- 10 requester addresses
- 6 GPU types (RTX 4090, RTX 4080, etc.)

---

### 5. `import_gpu_classes.py`

**Purpose**: Imports GPU class definitions from JSON backup files to PostgreSQL.

**Usage**:
```bash
python import_gpu_classes.py input_file [--clear] [--dry-run]
```

**Options**:
- `--clear`: Truncate gpu_classes table before import
- `--dry-run`: Show what would be imported without actually doing it

**What it does**:
- Loads GPU class data from JSON export files
- Creates gpu_classes table if it doesn't exist
- Supports upsert operations to handle conflicts
- Useful for environment synchronization (dev → staging → prod)

---

### 6. `import_geo_data.py`

**Purpose**: Simple tool to import city data from JSON backup files to PostgreSQL.

**Usage**:
```bash
python import_geo_data.py input_file [--clear]
```

**Options**:
- `--clear`: Clear existing city data before import

**What it does**:
- Loads city data from JSON files (flexible format parsing)
- Uses shared database functions for optimal performance
- Bulk inserts for efficiency
- Focused on city_snapshots table only

---

## Data Flow Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   SQLite DB     │───▶│  import_plans    │───▶│   PostgreSQL    │
│   (plans.db)    │    │     _db.py       │    │  (Plan Metrics) │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌──────────────────┐           │
│   MongoDB       │───▶│   get_geo_       │───────────┤
│  (Node Data)    │    │     data.py      │           │
└─────────────────┘    └──────────────────┘           │
                                                        │
┌─────────────────┐    ┌──────────────────┐           │
│   Strapi CMS    │───▶│  get_gpu_        │───────────┤
│ (GPU Classes)   │    │    classes.py    │           │
└─────────────────┘    └──────────────────┘           │
                                                        │
                       ┌──────────────────┐           │
                       │   generate_      │───────────┤
                       │  placeholder     │           │
                       │ transactions.py  │           │
                       └──────────────────┘           │
                                                        ▼
                                               ┌─────────────────┐
                                               │   Dashboard     │
                                               │     API         │
                                               │  (Backend)      │
                                               └─────────────────┘
```

## Database Schema

### Tables Populated by Scripts:

- **`city_snapshots`**: Geographic node distribution (from `get_geo_data.py`)
- **`gpu_classes`**: GPU specifications (from `get_gpu_classes.py`) 
- **`node_plan`**: Historical plan data (from `import_plans_db.py`)
- **`node_plan_job`**: Job execution details (from `import_plans_db.py`)
- **`transactions`**: Payment records (from `generate_placeholder_transactions.py`)

## Troubleshooting

### Common Issues:

1. **Database Connection Errors**:
   - Verify `.env` file contains correct credentials
   - Check PostgreSQL/MongoDB services are running
   - Test network connectivity to remote databases

2. **Geocoding Rate Limits**:
   - Script automatically handles 1-second delays
   - Cache reduces repeated API calls
   - Consider running during off-peak hours for large datasets

3. **Import Failures**:
   - Use `--clear` flag for clean imports
   - Check SQLite database file exists at expected path
   - Verify PostgreSQL migration has run (creates required tables)

4. **Strapi Authentication**:
   - Verify Strapi credentials in `.env`
   - Check Strapi instance is accessible
   - Ensure JWT authentication is enabled

## Maintenance

### Regular Tasks:
- Run `get_geo_data.py` hourly/daily for fresh node distribution
- Update `get_gpu_classes.py` when new GPU models are added
- Re-import plans data when new historical data is available
- Monitor geocoding cache size and performance

### Performance Optimization:
- Geocoding cache significantly improves `get_geo_data.py` performance
- Batched imports in `import_plans_db.py` handle large datasets efficiently
- Consider database indexing for frequently queried time ranges

## Security Notes

- Store sensitive credentials in `.env` file (excluded from git)
- Use read-only database credentials where possible
- Rate limit external API calls appropriately
- Validate and sanitize any user-provided organization filters