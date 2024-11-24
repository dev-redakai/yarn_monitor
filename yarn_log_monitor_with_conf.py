import os
import time
import json,boto3
import logging
import datetime
import re
from typing import List, Dict, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from abc import ABC, abstractmethod


class LogStorageBackend(ABC):
    """Abstract base class for log storage backends"""
    
    @abstractmethod
    def store_log(self, log_metadata: Dict[str, Any]) -> bool:
        """Store log data in the backend"""
        pass

    @abstractmethod
    def initialize(self) -> bool:
        """Initialize the storage backend"""
        pass


class S3Backend(LogStorageBackend):
    def __init__(self, config: Dict[str, Any], cluster_id: str):
        self.bucket = config['bucket']
        self.prefix = config.get('prefix', 'yarn-logs')
        self.config = config
        self.cluster_id = cluster_id
        self.s3_client = None
        self.logger = logging.getLogger(__name__)

    def initialize(self) -> bool:
        try:
            self.s3_client = boto3.client(
                's3',
                region_name=self.config['region']
            )
            return True
        except Exception as e:
            self.logger.error(f"S3 initialization error: {e}")
            return False

    def store_log(self, log_metadata: Dict[str, Any]) -> bool:
        try:
            # Create S3 key based on application ID and timestamp
            timestamp = datetime.datetime.fromtimestamp(log_metadata['timestamp'])
            s3_key = f"{self.prefix}/{timestamp.strftime('log_date=%Y-%m-%d')}/cluster_id={self.cluster_id}/application_id={log_metadata['application_id']}/{log_metadata['file_name']}"
            
            # Store both raw log content and metadata
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=log_metadata['log_content']
            )
            
            # Store metadata as separate JSON file
            metadata_key = f"{s3_key}.metadata.json"
            metadata_content = {k: v for k, v in log_metadata.items() if k != 'log_content'}
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=metadata_key,
                Body=json.dumps(metadata_content)
            )
            
            return True
        except Exception as e:
            self.logger.error(f"Error storing log in S3: {e}")
            return False


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

    def _extract_application_id(self, log_content: str) -> str:
        """
        Extract Spark application ID from log content
        """
        # Pattern to match Spark application ID (application_XXXXXXXXXX_XXXX)
        app_id_pattern = r'(application_\d+_\d+)'
        
        match = re.search(app_id_pattern, log_content)
        if match:
            return match.group(1)
        
        return "unknown_application"
                
    def _parse_log_metadata(self, log_path: str) -> Dict[str, Any]:
        """
        Parse log metadata from Spark application log file
        """
        try:
            print(f"Parsing log metadata for {log_path}")
            # Split path into components
            parts = log_path.split(os.sep)
            
            # Determine log type (controller, stderr, stdout, syslog)
            log_type = next((p for p in parts if p in ['controller', 'stderr', 'stdout', 'syslog']), None)
            
            # Read log content with retries
            log_content = ''
            max_retries = 5
            retry_count = 0

            while log_content == '' and retry_count < max_retries:
                try:
                    with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                        log_content = f.read()

                    if log_content == '':
                        self.logger.debug(f"Empty log content on attempt {retry_count + 1}, retrying...")
                        print(f"Empty log content on attempt {retry_count + 1}, retrying...")
                        retry_count += 1
                        time.sleep(1)
                    else:
                        self.logger.debug(f"Successfully read log content on attempt {retry_count + 1}")
                        print(f"Successfully read log content on attempt {retry_count + 1}")
                except Exception as e:
                    self.logger.warning(f"Error reading log on attempt {retry_count + 1}: {str(e)}")
                    print(f"Error reading log on attempt {retry_count + 1}: {str(e)}")
                    retry_count += 1
                    time.sleep(1)

            if log_content == '':
                self.logger.warning(f"Failed to read log content after {retry_count} attempts")
                print(f"Failed to read log content after {retry_count} attempts")
                log_content = '##No Content##'
	    # Extract application ID from log content
            application_id = self._extract_application_id(log_content)

            self.logger.debug(f"Log content length: {len(log_content)}")
            print(f"Log content length: {len(log_content)}")
                
            # Get file stats
            file_stats = os.stat(log_path)
            
            # Build metadata dictionary
            metadata = {
                'file_path': log_path,
                'file_name': os.path.basename(log_path),
                'application_id': application_id,
                'log_type': log_type,
                'timestamp': time.time(),
                'created_at': file_stats.st_ctime,
                'modified_at': file_stats.st_mtime,
                'file_size': file_stats.st_size,
                'log_content': log_content,
                'metadata': {
                    'log_directory': os.path.dirname(log_path),
                    'parent_directory': os.path.basename(os.path.dirname(log_path)),
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
                            f"application_id={application_id}, log_type={log_type}")
            
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
                required_fields = ['application_id', 'log_content']
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
                                f"{log_path} (application_id={log_metadata['application_id']}, "
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


class SparkLogFileHandler(FileSystemEventHandler):
    def __init__(self, log_handler: SparkLogHandler):
        self.log_handler = log_handler

    def on_created(self, event):
        if not event.is_directory:
            self.log_handler.index_log(event.src_path)
            
    def on_modified(self, event):
        if not event.is_directory:
            self.log_handler.index_log(event.src_path)


def fetch_cluster_id() -> str:
    """
    Fetch the current EMR cluster ID using job-flow.json.
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
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Fetch the current cluster ID
        cluster_id = fetch_cluster_id()
        
        # Initialize storage backends based on configuration
        storage_backends = []
            
        if config.get('s3', {}).get('enabled', False):
            storage_backends.append(S3Backend(config['s3'], cluster_id))
        
        if not storage_backends:
            raise ValueError("No storage backends enabled in configuration")

        # Initialize log handler with configured backends
        log_handler = SparkLogHandler(
            logs_dir=config['logs_dir'],
            storage_backends=storage_backends,
            log_level=config.get('log_level', 'INFO')
        )

        # Set up file system observer
        event_handler = SparkLogFileHandler(log_handler)
        observer = Observer()
        observer.schedule(event_handler, config['logs_dir'], recursive=True)

        observer.start()
        print(f"Monitoring {config['logs_dir']} for new Spark application logs...")

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
