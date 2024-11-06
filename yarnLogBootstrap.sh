#!/bin/bash
sudo pip3 install elasticsearch watchdog 

# Optional: Configure environment variables
export SPARK_LOGS_DIR=/home/hadoop/logs/userlogs # TODO: Change this to the correct directory
export ES_HOST=localhost # TODO: Change this to the correct host
export ES_PORT=9200 # TODO: Change this to the correct port
export ES_INDEX=yarn_logs # TODO: Change this to the correct index
export ES_SCHEME=https
export ES_USERNAME=elastic
export ES_PASSWORD=A4DtL95L1TWUQwPbfuiG
export YARN_LOG_DIR=/home/hadoop/hadoop/logs/userlogs
export YARN_LOG_FILE=/home/redakai/spark_workspace/yarn_monitor/log/yarn_log_monitor.log
export ES_API_KEY="WkQweTZKSUJYSzBlSGktSUpVS1I6MEJkV1RXS1NUQk9VdFNvcW5uOTdDdw=="
export ES_INDEX=yarn_logs

echo "ES_HOST: $ES_HOST"
echo "ES_PORT: $ES_PORT"
echo "ES_USERNAME: $ES_USERNAME"
echo "ES_PASSWORD: $ES_PASSWORD"
echo "ES_API_KEY: $ES_API_KEY"
echo "YARN_LOG_DIR: $YARN_LOG_DIR"
echo "YARN_LOG_FILE: $YARN_LOG_FILE"
echo "ES_INDEX: $ES_INDEX"
echo "ES_SCHEME: $ES_SCHEME"
echo "SPARK_LOGS_DIR: $SPARK_LOGS_DIR"

# Copy your log monitoring script
sudo rm /usr/local/bin/yarn_log_monitor.py
sudo cp yarn_log_monitor.py /usr/local/bin/ # TODO: Change this to the correct path
# Start the script
sudo python3 /usr/local/bin/yarn_log_monitor.py &