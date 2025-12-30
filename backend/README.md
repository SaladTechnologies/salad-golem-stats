# Salad Stats Backend (FastAPI)

## Setup

1. Install dependencies:

    pip install -r requirements.txt

2. Set environment variables for PostgreSQL connection (or use a .env file):

    POSTGRES_DB=your_db
    POSTGRES_USER=your_user
    POSTGRES_PASSWORD=your_password
    POSTGRES_HOST=localhost
    POSTGRES_PORT=5432

3. Run the server:


## Clearing the Redis Cache

To clear all keys from the Redis cache, use the provided script:

     python clear_redis_cache.py

You can set the following environment variables to configure the Redis connection (defaults shown):

     REDIS_HOST=localhost
     REDIS_PORT=6379
     REDIS_DB=0

The script requires the `redis` Python package (already included in requirements.txt).
    uvicorn main:app --reload

- `GET /metrics/country_counts` — country node counts (with lat/lon)
- `POST /metrics/load` — submit node loading stats (see `LoadStats` model)

Edit `main.py` to add more endpoints as needed.
