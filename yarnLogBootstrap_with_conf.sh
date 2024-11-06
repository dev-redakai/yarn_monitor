#!/bin/bash
sudo pip3 install elasticsearch watchdog boto3 psycopg2-binary

# Copy your log monitoring script
sudo rm /usr/local/bin/yarn_log_monitor_with_conf.py
sudo cp yarn_log_monitor_with_conf.py /usr/local/bin/ # TODO: Change this to the correct path
# Start the script
sudo python3 /usr/local/bin/yarn_log_monitor_with_conf.py /home/redakai/spark_workspace/yarn_monitor/setup_conf.json &