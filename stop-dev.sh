#!/bin/bash
# stop-dev.sh - Stop backend, frontend, and database for Stats Salad

# Stop backend (Python)

kill $FBACKEND_PID
BACKEND_PID=$(ps aux | grep '[p]ython main.py' | awk '{print $2}')
if [ -n "$BACKEND_PID" ]; then
  echo "Stopping backend (PID $BACKEND_PID)..."
  kill $BACKEND_PID
else
  echo "No backend process found."
fi

# Stop all frontend dev servers (npm, vite, react-scripts, node)
kill $FRONTEND_PID
FRONTEND_PIDS=$(ps aux | grep -E 'vite|npm|node|react-scripts' | grep -v grep | awk '{print $2}')
if [ -n "$FRONTEND_PIDS" ]; then
  echo "Stopping all frontend dev servers (PIDs $FRONTEND_PIDS)..."
  kill $FRONTEND_PIDS
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
