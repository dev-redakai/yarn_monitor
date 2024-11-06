import os
import time
import json
import logging
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