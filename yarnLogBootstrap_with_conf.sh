#!/bin/bash

# Update the package list and install Python's package manager (pip)
echo "Updating package list and installing Python's package manager (pip)"
sudo yum update -y
sudo yum install -y python3-pip
sudo yum install -y git

# Upgrade pip to the latest version
echo "Upgrading pip to the latest version"
sudo python3 -m pip install --upgrade pip

# Install the required packages
echo "Installing the required packages"
sudo python3 -m pip install elasticsearch watchdog boto3 psycopg2-binary

# Download yarn_monitor package from git
echo "Downloading yarn_monitor package from git"
git clone https://github.com/dev-redakai/yarn_monitor.git ~/yarn_monitor

# Start the script
echo "Starting the log monitoring script"
sudo python3 ~/yarn_monitor/yarn_log_monitor_with_conf.py ~/yarn_monitor/config/setup_conf.json &