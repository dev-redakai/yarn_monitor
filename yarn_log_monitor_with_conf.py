import os
import time
import json
import logging, datetime
from typing import List, Dict, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from utility.log_storage_backend import LogStorageBackend
from utility.elasticsearch_backend import ElasticsearchBackend
from utility.s3_backend import S3Backend
from utility.postgres_backend import PostgresBackend


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
            # Split path into components
            parts = log_path.split(os.sep)
            
            # Extract step ID (format: s-XXXXXXXXXXXXXXXXX)
            step_id = next((p for p in parts if p.startswith('s-')), None)
            
            # Determine log type (controller, stderr, stdout, syslog)
            log_type = next((p for p in parts if p in ['controller', 'stderr', 'stdout', 'syslog']), None)
            
            # Read log content
            with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                log_content = f.read()
                
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
    def _parse_log_metadata(self, log_path: str) -> Dict[str, Any]:
        """Parse log metadata from file path"""
        try:
            parts = log_path.split(os.sep)
            app_id = next((p for p in parts if p.startswith('application_')), None)
            container_id = next((p for p in parts if p.startswith('container_')), None)

            with open(log_path, 'r', encoding='utf-8') as f:
                log_content = f.read()

            return {
                'file_path': log_path,
                'file_name': os.path.basename(log_path),
                'application_id': app_id,
                'container_id': container_id,
                'timestamp': time.time(),
                'log_content': log_content
            }
        except Exception as e:
            self.logger.error(f"Error parsing log metadata for {log_path}: {e}")
            return {}

    def index_log(self, log_path: str):
        """Index log file to all configured storage backends"""
        try:
            log_metadata = self._parse_log_metadata(log_path)
            if log_metadata:
                for backend in self.storage_backends:
                    success = backend.store_log(log_metadata)
                    if success:
                        self.logger.info(f"Successfully stored log in {backend.__class__.__name__}: {log_path}")
                    else:
                        self.logger.error(f"Failed to store log in {backend.__class__.__name__}: {log_path}")
        except Exception as e:
            self.logger.error(f"Error indexing log {log_path}: {e}")
'''

class SparkLogFileHandler(FileSystemEventHandler):
    def __init__(self, log_handler: SparkLogHandler):
        self.log_handler = log_handler

    def on_created(self, event):
        if not event.is_directory:
            self.log_handler.index_log(event.src_path)

def main(config_file: str):
    """
    Main function to start log monitoring with configuration from file
    
    :param config_file: Path to JSON configuration file
    """
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Initialize storage backends based on configuration
        storage_backends = []
        
        if config.get('elasticsearch', {}).get('enabled', False):
            storage_backends.append(ElasticsearchBackend(config['elasticsearch']))
            
        if config.get('s3', {}).get('enabled', False):
            storage_backends.append(S3Backend(config['s3']))
            
        if config.get('postgres', {}).get('enabled', False):
            storage_backends.append(PostgresBackend(config['postgres']))
        
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