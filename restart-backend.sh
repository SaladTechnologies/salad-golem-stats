#!/bin/bash
set -e # stop on errors


echo "Attempting to stop the backend..."

# Detect OS
OS_TYPE=$(uname -s)

if [[ "$OS_TYPE" == "Darwin" || "$OS_TYPE" == "Linux" ]]; then
    # macOS or Linux
    pkill -f "uvicorn main:app" || true
    pkill -f "python.*main.py" || true
    # Kill any process using port 8000 (backend server)
    PORT_8000_PID=$(lsof -ti :8000)
    if [ -n "$PORT_8000_PID" ]; then
        echo "Killing process on port 8000 (PID $PORT_8000_PID)..."
        kill $PORT_8000_PID
    fi
else
    # Windows (Git Bash, WSL, or Cygwin)
    echo "Stopping Windows-side python.exe/uvicorn processes..."
    powershell.exe -NoProfile -Command \
        "Get-Process python,uvicorn -ErrorAction SilentlyContinue | Stop-Process -Force"
fi

echo "Restarting backend..."
cd backend

# Activate virtual environment if it exists
if [ -f "./venv/bin/activate" ]; then
    source ./venv/bin/activate
fi

# Start the backend in the background
python main.py & BACKEND_PID=$!

echo "Backend restarted successfully."
