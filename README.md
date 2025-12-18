# Stats Salad Dashboard

This project provides a dashboard for visualizing network statistics for SaladCloud's testing on the Golem Network. It includes a frontend React application, a Python FastAPI backend, and a PostgreSQL database.

## Current Project Status

This is a summary of the current state of the project based on recent development notes.

*   **Frontend (`/frontend`):** The UI is functional but requires some polish on component arrangement and titles.
    *   **Globe:** The globe visualization is working and displays daily active nodes from a snapshot. This could be updated to show live data or only nodes participating in the test, but the current implementation avoids a sparse-looking map.
    *   **Transaction Table:** The data in this table is currently mocked. It needs UI improvements like pagination and the ability to limit results, which has been deferred until the data source is finalized.
    *   **Trend Graphs:** The graphs are functional, but the organization, labeling, and choice of what to plot need further refinement. Tooltips are a planned minor addition.
*   **Backend (`/backend`):** The FastAPI backend is functional for its current purpose, serving data to the frontend from the database.
*   **Data (`/db`, `/data-collection`):** The database contains processed data. However, there are suspected discrepancies in the data (e.g., incorrect units or scaling) originating from the initial Mixpanel pull or processing scripts. This has not been fully investigated as the data sources are expected to change.

## Getting Started

### Prerequisites

*   A shell environment to run the `.sh` script (like Git Bash on Windows, or any standard Linux/macOS shell).
*   Docker and Docker Compose to run the PostgreSQL database.
*   Node.js and npm for the frontend.
*   Python for the backend.

### Running the Application

The easiest way to get everything running is to use the provided script.

1.  **Set up Environment Variables:** Before running, you must create the required `.env` files as detailed in the "Environment Variables" section below.
2.  **Run the script:**
    ```bash
    ./start-dev.sh
    ```
    This script will:
    *   Start the PostgreSQL database using Docker Compose.
    *   Start the FastAPI backend with Uvicorn.
    *   Start the React frontend development server.

## Environment Variables

You will need to create the following `.env` files in their respective directories.

### 1. Frontend (`/frontend/.env`)

This file configures the React application.

```
VITE_STATS_API_URL="http://localhost:8000"
```

### 2. Backend (`/backend/.env`)

This file configures the FastAPI server and its database connection.

```
FRONTEND_ORIGINS=http://localhost:5173,http://127.0.0.1:5173,http://localhost:5174,http://127.0.0.1:5174
POSTGRES_USER=devuser
POSTGRES_PASSWORD=devpass
POSTGRES_HOST=localhost
POSTGRES_DB=statsdb
POSTGRES_PORT=5432
```

### 3. Data Collection (`/data-collection/.env`)

This file configures the scripts used for data gathering and processing.

```
# Postgres connection for writing data
POSTGRES_USER=devuser
POSTGRES_PASSWORD=devpass
POSTGRES_HOST=localhost
POSTGRES_DB=statsdb
POSTGRES_PORT=5432

# MongoDB and Strapi credentials for get_data.py (out of date)
MONGOUSER=
MONGOPASS=
MONGO_URL="matrix-production-1.qltsap.mongodb.net"
DBNAME="matrix"
MIN_SEL=2003009
STRAPIURL="https://cms-api.salad.io"
STRAPIID=
STRAPIPW=
```

## Project Structure

*   **/backend:** The FastAPI application that serves data via a REST API.
    *   `main.py`: The main application file.
    *   **Endpoints:**
        *   `/metrics/city_counts`: Provides geolocated node counts for the globe visualization.
        *   `/metrics/transactions`: Serves mocked transaction data.
        *   `/metrics/stats`: Provides aggregated data for the trend graphs. Currently returns metrics like `total_time_seconds`, `total_invoice_amount`, `unique_node_count`, etc.
*   **/db:** Contains the `docker-compose.yaml` to spin up the PostgreSQL database and a `/migrations` directory with the schema.
*   **/data-collection:** Contains Python scripts for pulling data from various sources (e.g., Mixpanel, MongoDB) and processing it into the PostgreSQL database.
*   **/frontend:** A React application built with Vite that consumes the backend API and displays the dashboard.

```