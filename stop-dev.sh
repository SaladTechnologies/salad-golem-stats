#!/bin/bash
# stop-dev.sh - Stop backend, frontend, and database for Stats Salad

# Function to safely kill processes
safe_kill() {
    local pid=$1
    local name=$2
    if [ -n "$pid" ]; then
        echo "Stopping $name (PID $pid)..."
        # Try graceful termination first
        kill -TERM $pid 2>/dev/null
        sleep 2
        # Check if process still exists, force kill if needed
        if kill -0 $pid 2>/dev/null; then
            echo "Force killing $name (PID $pid)..."
            kill -KILL $pid 2>/dev/null
        fi
    fi
}

# Detect OS
OS_TYPE=$(uname -s)

if [[ "$OS_TYPE" == "Darwin" || "$OS_TYPE" == "Linux" ]]; then
  # macOS or Linux
  
  # Kill any process using port 8000 (backend server) - more reliable method
  PORT_8000_PID=$(lsof -ti :8000 2>/dev/null)
  if [ -n "$PORT_8000_PID" ]; then
    safe_kill "$PORT_8000_PID" "process on port 8000"
  fi

  # Kill any process using port 5173 (Vite dev server)
  PORT_5173_PID=$(lsof -ti :5173 2>/dev/null)
  if [ -n "$PORT_5173_PID" ]; then
    safe_kill "$PORT_5173_PID" "process on port 5173"
  fi

  # Stop backend (Python) - more specific search
  BACKEND_PID=$(pgrep -f "python.*main\.py" | head -1)
  if [ -n "$BACKEND_PID" ]; then
    safe_kill "$BACKEND_PID" "backend server"
  else
    echo "No backend process found."
  fi

  # Stop frontend dev servers more carefully (avoid VS Code processes)
  # Look for vite processes specifically
  VITE_PIDS=$(pgrep -f "vite" | grep -v "$$")
  for pid in $VITE_PIDS; do
    # Check if it's actually a dev server (not VS Code extension)
    if ps -p $pid -o command= | grep -q "vite.*serve\|vite.*dev"; then
      safe_kill "$pid" "Vite dev server"
    fi
  done

  # Look for npm dev processes specifically 
  NPM_DEV_PIDS=$(pgrep -f "npm.*dev\|npm.*start" | grep -v "$$")
  for pid in $NPM_DEV_PIDS; do
    safe_kill "$pid" "npm dev server"
  done
else
  # Windows (Git Bash, WSL, or Cygwin)
  echo "Stopping Windows-side node.exe processes..."
  powershell.exe -NoProfile -Command \
    "Get-Process node -ErrorAction SilentlyContinue | Where-Object {$_.CommandLine -like '*dev*' -or $_.CommandLine -like '*vite*'} | Stop-Process -Force"
fi

# Stop Postgres and Redis Docker
cd "$(dirname "$0")/db" || exit 1
if docker-compose ps 2>/dev/null | grep -q 'dev-postgres\|dev-redis'; then
  echo "Stopping Docker containers (Postgres and Redis)..."
  docker-compose down
else
  echo "No Docker containers running."
fi

echo "Development environment stopped."