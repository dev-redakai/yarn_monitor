import os
import time
import json
import logging, datetime
import subprocess, boto3
import threading
from typing import List, Dict, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from utility.log_storage_backend import LogStorageBackend
from utility.elasticsearch_backend import ElasticsearchBackend
from utility.s3_backend import S3Backend
from utility.postgres_backend import PostgresBackend

class SparkLogHandler:
    def __init__(self, logs_dir: str, storage_backends: List[LogStorageBackend], emr_client, log_level: str = 'INFO'):
        """
        Initialize SparkLogHandler with multiple storage backends and EMR client
        """
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.logs_dir = logs_dir
        self.storage_backends = storage_backends
        self.emr_client = emr_client
        self.running_steps = {}
        self.lock = threading.Lock()

    def fetch_running_steps(self, cluster_id: str):
        """
        Fetch running steps from EMR.
        """
        steps = self.emr_client.list_steps(ClusterId=cluster_id, StepStates=['PENDING', 'RUNNING'])['Steps']
        with self.lock:
            self.running_steps = {step['Id']: time.time() for step in steps}
            

    def upload_logs(self, step_id: str, cluster_id: str):
        """
        Continuously upload logs for a specific step ID.
        """
        step_log_dir = os.path.join(self.logs_dir, f"s-{step_id}")
        self.logger.info(f"Monitoring logs for step: {step_id}")

        while True:
            for log_type in ['controller', 'stderr', 'stdout', 'syslog']:
                log_file = os.path.join(step_log_dir, log_type)
                if os.path.exists(log_file):
                    with open(log_file, 'r') as file:
                        log_content = file.read()
                        log_metadata = {
                            "step_id": step_id,
                            "log_type": log_type,
                            "log_content": log_content,
                            "timestamp": time.time()
                        }
                        for backend in self.storage_backends:
                            backend.store_log(log_metadata, cluster_id)

            # Stop monitoring if step is not running and 1 minute has passed
            with self.lock:
                if step_id not in self.running_steps and time.time() - self.running_steps.get(step_id, 0) > 60:
                    break

            time.sleep(10)
            
    def process_log(self, log_path: str, cluster_id: str):
        """
        Process a single log file and delegate to upload_logs for handling updates.
        """
        # Extract step ID and log type from the path
        parts = log_path.split(os.sep)
        step_id = next((p for p in parts if p.startswith('s-')), None)
        log_type = os.path.basename(log_path)

        if step_id and log_type:
            self.logger.info(f"Processing log file: {log_path}")
            # Delegate to upload_logs to handle updates for this step/log
            self.upload_logs(step_id, cluster_id)

    def monitor_steps(self, cluster_id: str):
        """
        Monitor and handle logs for all running steps.
        """
        while True:
            self.fetch_running_steps(cluster_id)

            for step_id in list(self.running_steps):
                thread = threading.Thread(target=self.upload_logs, args=(step_id, cluster_id, ))
                thread.start()

            time.sleep(10)  # Check for new steps every 10 seconds

class SparkLogFileHandler(FileSystemEventHandler):
    """
    Watchdog event handler for detecting file changes.
    """
    def __init__(self, log_handler: SparkLogHandler, cluster_id: str):
        self.log_handler = log_handler
        self.cluster_id = cluster_id

    def on_created(self, event):
        if not event.is_directory:
            self.log_handler.logger.info(f"New file detected: {event.src_path}")
            self.log_handler.process_log(event.src_path, self.cluster_id)

    def on_modified(self, event):
        if not event.is_directory:
            self.log_handler.logger.info(f"File modified: {event.src_path}")
            self.log_handler.process_log(event.src_path, self.cluster_id)


def fetch_cluster_id() -> str:
    """
    Fetch the current EMR cluster ID using a bash command to parse job-flow.json.
    """
    try:
        # Path to the job-flow.json file
        json_file_path = '/mnt/var/lib/info/job-flow.json'
        
        # Read and parse the JSON file
        with open(json_file_path, 'r') as file:
            job_flow_data = json.load(file)
            
        # Extract the jobFlowId
        cluster_id = job_flow_data.get('jobFlowId')
        if not cluster_id:
            raise ValueError("Cluster ID is empty")
        logging.info(f"Fetched cluster ID: {cluster_id}")
        return cluster_id
    except Exception as e:
        logging.error(f"Failed to fetch cluster ID: {e}")
        raise RuntimeError("Unable to determine cluster ID")


def main(config_file: str):
    """
    Main function to start log monitoring with configuration from file
    
    :param config_file: Path to JSON configuration file
    """
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
            
        # Fetch the current cluster ID
        cluster_id = fetch_cluster_id()
        
        # Initialize storage backends based on configuration
        storage_backends = []
        
        if config.get('elasticsearch', {}).get('enabled', False):
            storage_backends.append(ElasticsearchBackend(config['elasticsearch']))
            
        if config.get('s3', {}).get('enabled', False):
            storage_backends.append(S3Backend(config['s3'], cluster_id))
            
        if config.get('postgres', {}).get('enabled', False):
            storage_backends.append(PostgresBackend(config['postgres']))
        
        if not storage_backends:
            raise ValueError("No storage backends enabled in configuration")
        
        # Initialize EMR client
        emr_client = boto3.client('emr', region_name=config['s3']['region'])

        # Initialize log handler with configured backends
        # Start monitoring
        log_handler = SparkLogHandler(
            logs_dir=config['logs_dir'],
            storage_backends=storage_backends,
            emr_client=emr_client,
            log_level=config.get('log_level', 'INFO')
        )
        threading.Thread(target=log_handler.monitor_steps, args=(cluster_id,)).start()

        # Set up file system observer with more robust error handling
        event_handler = SparkLogFileHandler(log_handler, cluster_id)
        observer = Observer()
        observer.schedule(event_handler, config['logs_dir'], recursive=True)

        observer.start()
        print(f"Monitoring {config['logs_dir']} for new log files...")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        observer.stop()
        observer.join()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python yarn_log_monitor_with_conf.py <config_file>")
        sys.exit(1)
        
    main(sys.argv[1])
