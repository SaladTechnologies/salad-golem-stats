#!/bin/bash
# stop-dev.sh - Stop backend, frontend, and database for Stats Salad

# Stop backend (Python)
BACKEND_PID=$(ps aux | grep '[p]ython main.py' | awk '{print $2}')
if [ -n "$BACKEND_PID" ]; then
  echo "Stopping backend (PID $BACKEND_PID)..."
  kill $BACKEND_PID
else
  echo "No backend process found."
fi

# Stop frontend (npm/react-scripts)
FRONTEND_PID=$(ps aux | grep '[n]pm start\|[n]pm run dev\|[r]eact-scripts start' | awk '{print $2}')
if [ -n "$FRONTEND_PID" ]; then
  echo "Stopping frontend (PID $FRONTEND_PID)..."
  kill $FRONTEND_PID
else
  echo "No frontend process found."
fi

# Stop Postgres Docker
cd db
if docker-compose ps | grep -q 'dev-postgres'; then
  echo "Stopping Postgres Docker container..."
  docker-compose down
else
  echo "No Postgres Docker container running."
fi
