#!/bin/bash

# Set error handling
set -e
set -x

# Create log directories
LOG_DIR="/tmp/yarn_monitor_bootstrap"
APP_LOG_DIR="/tmp/application_logs"
mkdir -p "$LOG_DIR" "$APP_LOG_DIR"
LOG_FILE="$LOG_DIR/yarn_log_bootstrap.log"

# Redirect output to log file
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Bootstrap script started"

# Install required packages in parallel
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Installing required packages"
sudo yum install -y python3-pip git &
PID1=$!

# Install Python packages in parallel while yum installs
python3 -m pip install --upgrade pip &
PID2=$!

# Wait for package installations to complete
wait $PID1
wait $PID2

# Install Python dependencies with retries
MAX_RETRIES=3
for i in $(seq 1 $MAX_RETRIES); do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Installing Python packages (attempt $i/$MAX_RETRIES)"
  if python3 -m pip install elasticsearch watchdog boto3 psycopg2-binary awscli; then
    break
  fi
  if [ $i -eq $MAX_RETRIES ]; then
    echo "Failed to install Python packages after $MAX_RETRIES attempts"
    exit 1
  fi
  sleep 5
done

# Clone repository with retry
for i in $(seq 1 $MAX_RETRIES); do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cloning yarn_monitor (attempt $i/$MAX_RETRIES)"
  if git clone https://github.com/dev-redakai/yarn_monitor.git /home/hadoop/yarn_monitor; then
    break
  fi
  if [ $i -eq $MAX_RETRIES ]; then
    echo "Failed to clone repository after $MAX_RETRIES attempts"
    exit 1
  fi
  sleep 5
done

# Start monitoring script in background with nohup
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting log monitoring script"
# Install nohup if not present
if ! command -v nohup &> /dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Installing nohup"
    sudo yum install -y coreutils
fi

# Verify nohup is available
if ! command -v nohup &> /dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Failed to install nohup, exiting"
    exit 1
fi

# Start monitoring script with nohup
nohup python3 /home/hadoop/yarn_monitor/yarn_log_monitor_with_conf.py /home/hadoop/yarn_monitor/config/setup_conf.json > "$LOG_DIR/monitor.log" 2>&1 &
