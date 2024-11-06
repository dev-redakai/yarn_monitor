import os
import time
import json
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
from elasticsearch import Elasticsearch
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import boto3
import psycopg2
from psycopg2.extras import Json

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

class ElasticsearchBackend(LogStorageBackend):
    def __init__(self, config: Dict[str, Any]):
        self.es_client = None
        self.es_index = config['index']
        self.config = config
        self.logger = logging.getLogger(__name__)

    def initialize(self) -> bool:
        try:
            es_config = {
                'hosts': f"{self.config['scheme']}://{self.config['host']}:{self.config['port']}",
                'basic_auth': (self.config['user'], self.config['password'])
            }

            if self.config['scheme'] == 'https':
                es_config['verify_certs'] = False

            self.es_client = Elasticsearch(**es_config)
            
            if not self.es_client.indices.exists(index=self.es_index):
                index_mapping = {
                    "mappings": {
                        "properties": {
                            "file_path": {"type": "keyword"},
                            "application_id": {"type": "keyword"},
                            "container_id": {"type": "keyword"},
                            "timestamp": {"type": "date"},
                            "log_content": {"type": "text"}
                        }
                    }
                }
                self.es_client.indices.create(index=self.es_index, body=index_mapping)
                
            return True
        except Exception as e:
            self.logger.error(f"Elasticsearch initialization error: {e}")
            return False

    def store_log(self, log_metadata: Dict[str, Any]) -> bool:
        try:
            res = self.es_client.index(
                index=self.es_index,
                document=log_metadata
            )
            return res['result'] in ['created', 'updated']
        except Exception as e:
            self.logger.error(f"Error storing log in Elasticsearch: {e}")
            return False

class S3Backend(LogStorageBackend):
    def __init__(self, config: Dict[str, Any]):
        self.bucket = config['bucket']
        self.prefix = config.get('prefix', 'yarn-logs')
        self.config = config
        self.s3_client = None
        self.logger = logging.getLogger(__name__)

    def initialize(self) -> bool:
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config['aws_access_key_id'],
                aws_secret_access_key=self.config['aws_secret_access_key'],
                region_name=self.config['region']
            )
            return True
        except Exception as e:
            self.logger.error(f"S3 initialization error: {e}")
            return False

    def store_log(self, log_metadata: Dict[str, Any]) -> bool:
        try:
            # Create S3 key based on application ID and timestamp
            timestamp = datetime.fromtimestamp(log_metadata['timestamp'])
            s3_key = f"{self.prefix}/{timestamp.strftime('%Y/%m/%d')}/{log_metadata['application_id']}/{log_metadata['file_name']}"
            
            # Store both raw log content and metadata
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=log_metadata['log_content']
            )
            
            # Store metadata as separate JSON file
            metadata_key = f"{s3_key}.metadata.json"
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=metadata_key,
                Body=json.dumps(log_metadata)
            )
            
            return True
        except Exception as e:
            self.logger.error(f"Error storing log in S3: {e}")
            return False

class PostgresBackend(LogStorageBackend):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.conn = None
        self.logger = logging.getLogger(__name__)

    def initialize(self) -> bool:
        try:
            self.conn = psycopg2.connect(
                dbname=self.config['dbname'],
                user=self.config['user'],
                password=self.config['password'],
                host=self.config['host'],
                port=self.config['port']
            )
            
            # Create table if it doesn't exist
            with self.conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS yarn_logs (
                        id SERIAL PRIMARY KEY,
                        file_path VARCHAR(1024),
                        file_name VARCHAR(256),
                        application_id VARCHAR(256),
                        container_id VARCHAR(256),
                        timestamp TIMESTAMP,
                        log_content TEXT,
                        metadata JSONB
                    )
                """)
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Postgres initialization error: {e}")
            return False

    def store_log(self, log_metadata: Dict[str, Any]) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO yarn_logs 
                    (file_path, file_name, application_id, container_id, timestamp, log_content, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    log_metadata['file_path'],
                    log_metadata['file_name'],
                    log_metadata['application_id'],
                    log_metadata['container_id'],
                    datetime.fromtimestamp(log_metadata['timestamp']),
                    log_metadata['log_content'],
                    Json(log_metadata)
                ))
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error storing log in Postgres: {e}")
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
        print("Usage: python yarn_log_monitor.py <config_file>")
        sys.exit(1)
        
    main(sys.argv[1])