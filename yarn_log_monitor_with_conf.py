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

'''
class SparkLogHandler:
    def __init__(self, logs_dir: str, storage_backends: List[LogStorageBackend], log_level: str = 'INFO'):
        """
        Initialize SparkLogHandler with multiple storage backends
        
        :param logs_dir: Directory containing Spark/Hadoop user logs
        :param storage_backends: List of storage backend instances
        :param log_level: Logging level
        """
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.logs_dir = logs_dir
        self.storage_backends = storage_backends
        
        # Initialize all storage backends
        for backend in storage_backends:
            if not backend.initialize():
                self.logger.error(f"Failed to initialize {backend.__class__.__name__}")
                
    def _parse_log_metadata(self, log_path: str) -> Dict[str, Any]:
        """
        Parse log metadata from Airflow/Hadoop log file path
        
        Expected structure:
        /var/log/hadoop/steps/s-{STEP_ID}/{log_type}/...
        
        :param log_path: Path to log file
        :return: Dictionary containing parsed metadata
        """
        try:
            print(f"Parsing log metadata for {log_path}")
            # Split path into components
            parts = log_path.split(os.sep)
            
            # Extract step ID (format: s-XXXXXXXXXXXXXXXXX)
            step_id = next((p for p in parts if p.startswith('s-')), None)
            
            # Determine log type (controller, stderr, stdout, syslog)
            log_type = next((p for p in parts if p in ['controller', 'stderr', 'stdout', 'syslog']), None)
            
            # Read log content with retries
            log_content = ''
            max_retries = 5
            retry_count = 0

            while log_content=='' and retry_count < max_retries:
                try:
                    with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                        log_content = f.read()

                    if log_content=='':
                        self.logger.debug(f"Empty log content on attempt {retry_count + 1}, retrying...")
                        print(f"Empty log content on attempt {retry_count + 1}, retrying...")
                        retry_count += 1
                        time.sleep(30)
                    else:
                        self.logger.debug(f"Successfully read log content on attempt {retry_count + 1}")
                        print(f"Successfully read log content on attempt {retry_count + 1}")
                except Exception as e:
                    self.logger.warning(f"Error reading log on attempt {retry_count + 1}: {str(e)}")
                    print(f"Error reading log on attempt {retry_count + 1}: {str(e)}")
                    retry_count += 1
                    time.sleep(30)

            if log_content=='':
                self.logger.warning(f"Failed to read log content after {retry_count} attempts")
                print(f"Failed to read log content after {retry_count} attempts")
                log_content = '##No Content##'
            self.logger.debug(f"Log content length: {len(log_content)}")
            print(f"Log content length: {len(log_content)}")
                
            # Get file stats
            file_stats = os.stat(log_path)
            
            # Build metadata dictionary
            metadata = {
                'file_path': log_path,
                'file_name': os.path.basename(log_path),
                'step_id': step_id,
                'log_type': log_type,
                'timestamp': time.time(),
                'created_at': file_stats.st_ctime,
                'modified_at': file_stats.st_mtime,
                'file_size': file_stats.st_size,
                'log_content': log_content,
                'metadata': {
                    'hadoop_root': '/var/log/hadoop',
                    'is_step_log': 'steps' in parts,
                    'log_directory': os.path.dirname(log_path),
                    'parent_directory': parts[-2] if len(parts) > 1 else None,
                }
            }
            
            # Add additional parser metadata for debugging
            metadata['parser_info'] = {
                'path_parts': parts,
                'parse_timestamp': datetime.datetime.now().isoformat(),
                'parser_version': '1.0'
            }
            
            # Log successful parsing
            self.logger.debug(f"Successfully parsed metadata for {log_path}: "
                            f"step_id={step_id}, log_type={log_type}")
            
            print(f"Metadata: {metadata}")
            
            return metadata

        except Exception as e:
            self.logger.error(f"Error parsing log metadata for {log_path}: {str(e)}", 
                            exc_info=True)
            return {}

    def index_log(self, log_path: str):
        """Index log file to all configured storage backends"""
        try:
            # Skip temporary or system files
            if any(p in log_path for p in ['.tmp', '.swp', '.bak']):
                self.logger.debug(f"Skipping temporary file: {log_path}")
                return
                
            # Parse metadata
            log_metadata = self._parse_log_metadata(log_path)
            
            if log_metadata:
                # Add some basic validation
                required_fields = ['step_id', 'log_type', 'log_content']
                missing_fields = [f for f in required_fields if not log_metadata.get(f)]
                
                if missing_fields:
                    self.logger.warning(f"Missing required fields {missing_fields} for {log_path}")
                    return
                    
                # Store in each backend
                for backend in self.storage_backends:
                    try:
                        success = backend.store_log(log_metadata)
                        if success:
                            self.logger.info(
                                f"Successfully stored log in {backend.__class__.__name__}: "
                                f"{log_path} (step_id={log_metadata['step_id']}, "
                                f"type={log_metadata['log_type']})"
                            )
                        else:
                            self.logger.error(
                                f"Failed to store log in {backend.__class__.__name__}: "
                                f"{log_path}"
                            )
                    except Exception as be:
                        self.logger.error(
                            f"Backend {backend.__class__.__name__} error for {log_path}: {str(be)}",
                            exc_info=True
                        )
            else:
                self.logger.warning(f"No metadata could be parsed for {log_path}")
                
        except Exception as e:
            self.logger.error(f"Error indexing log {log_path}: {str(e)}", exc_info=True)
'''

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
    def __init__(self, log_handler: SparkLogHandler):
        self.log_handler = log_handler

    def on_created(self, event):
        if not event.is_directory:
            self.log_handler.index_log(event.src_path)
            
            
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

        # Set up file system observer
        event_handler = SparkLogFileHandler(log_handler)
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
