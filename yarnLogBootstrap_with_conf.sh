#!/bin/bash

# Create a central log folder for bootstrap logs
LOG_DIR="/tmp/yarn_monitor_bootstrap"
mkdir -p "$LOG_DIR"
mkdir -p "/tmp/application_logs"
LOG_FILE="$LOG_DIR/yarn_log_bootstrap.log"

# Redirect all output to the log file
exec > "$LOG_FILE" 2>&1

echo "Bootstrap script started at $(date)"

# Update the package list and install Python's package manager (pip)
echo "Updating package list and installing Python's package manager (pip)"
sudo yum update -y
sudo yum install -y python3-pip
sudo yum install -y git

# Upgrade pip to the latest version
echo "Upgrading pip to the latest version"
python3 -m pip install --upgrade pip

# Install the required packages
echo "Installing the required packages"
python3 -m pip install elasticsearch watchdog boto3 psycopg2-binary awscli

# Clone the yarn_monitor repository
echo "Cloning the yarn_monitor package from GitHub"
git clone https://github.com/dev-redakai/yarn_monitor.git /home/hadoop/yarn_monitor

# Save logs for the Python monitoring script
echo "Starting the log monitoring script, logs will be saved to $LOG_FILE"
python3 /home/hadoop/yarn_monitor/yarn_log_monitor_with_conf.py /home/hadoop/yarn_monitor/config/setup_conf.json &
