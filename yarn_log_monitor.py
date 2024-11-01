import os
import time
import json
import logging
from typing import List, Dict, Any
from elasticsearch import Elasticsearch
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class SparkLogHandler:
    def __init__(self, 
                 logs_dir: str, 
                 es_host: str, 
                 es_port: int, 
                 es_index: str,
                 es_scheme: str,
                 es_user: str,
                 es_password: str,
                 es_api_key: str = None,
                 log_level: str = 'INFO'):
        """
        Initialize SparkLogHandler for monitoring and indexing Spark logs.
        
        :param logs_dir: Directory containing Spark/Hadoop user logs
        :param es_host: Elasticsearch host
        :param es_port: Elasticsearch port
        :param es_index: Elasticsearch index name
        :param es_scheme: Elasticsearch connection scheme
        :param es_api_key: Elasticsearch API key for authentication
        :param log_level: Logging level
        """
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        # Configuration
        self.logs_dir = logs_dir
        self.es_index = es_index

        # Elasticsearch connection
        try:
            # Prepare Elasticsearch connection configuration
            es_config = {
                'hosts': f'{es_host}:{es_port}',
                'http_auth': (es_user, es_password)
            }

            # API Key Authentication
            # if not es_api_key:
            #     # Try to get API key from environment variable
            #     es_api_key = os.getenv('ES_API_KEY')

            # if not es_api_key:
            #     raise ValueError("Elasticsearch API key is required")

            # # Configure API key authentication
            # es_config['api_key'] = es_api_key

            # Optional: SSL context for https
            if es_scheme == 'https':
                es_config['verify_certs'] = False

            print("es_config before creating client ",es_config)
            # Create Elasticsearch client
            self.es_client = Elasticsearch(**es_config)
            
            # Verify connection
            cluster_info = self.es_client.info()
            self.logger.info(f"Connected to Elasticsearch cluster: {cluster_info['cluster_name']}")
            
            # Create index if not exists
            if not self.es_client.indices.exists(index=es_index):
                # Create index with dynamic mapping for flexible log indexing
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
                self.es_client.indices.create(index=es_index, body=index_mapping)
                self.logger.info(f"Created Elasticsearch index: {es_index}")
            
        except Exception as e:
            self.logger.error(f"Elasticsearch connection error: {e}")
            raise

    def _parse_log_metadata(self, log_path: str) -> Dict[str, Any]:
        """
        Extract metadata from log file path.
        
        :param log_path: Path to log file
        :return: Extracted metadata dictionary
        """
        try:
            # Split path components
            parts = log_path.split(os.sep)
            
            # Extract application and container IDs
            app_id = next((p for p in parts if p.startswith('application_')), None)
            container_id = next((p for p in parts if p.startswith('container_')), None)
            
            # Read log content
            with open(log_path, 'r', encoding='utf-8') as f:
                log_content = f.read()
            
            return {
                'file_path': log_path,
                'file_name': log_path.split(os.sep)[-1],
                'application_id': app_id,
                'container_id': container_id,
                'timestamp': time.time(),
                'log_content': log_content
            }
        
        except Exception as e:
            self.logger.error(f"Error parsing log metadata for {log_path}: {e}")
            return {}

    def index_log(self, log_path: str):
        """
        Index log file to Elasticsearch.
        
        :param log_path: Path to log file
        """
        try:
            log_metadata = self._parse_log_metadata(log_path)
            
            if log_metadata:
                # Index document to Elasticsearch with automatic ID generation
                res = self.es_client.index(
                    index=self.es_index, 
                    document=log_metadata
                )
                self.logger.info(f"Indexed log: {log_path}, ES Response: {res['result']}")
        
        except Exception as e:
            self.logger.error(f"Error indexing log {log_path}: {e}")

class SparkLogFileHandler(FileSystemEventHandler):
    def __init__(self, log_handler: SparkLogHandler):
        """
        Initialize file system event handler.
        
        :param log_handler: SparkLogHandler instance
        """
        self.log_handler = log_handler

    def on_created(self, event):
        """
        Handle file creation events.
        
        :param event: Filesystem event
        """
        if not event.is_directory:
            self.log_handler.index_log(event.src_path)

def main(logs_dir: str, 
         es_host: str = 'localhost', 
         es_port: int = 9200, 
         es_index: str = 'yarn_logs',
         es_scheme: str = 'https',
         es_user: str = 'elastic',
         es_password: str = None,
         es_api_key: str = None,
         poll_interval: int = 1):
    """
    Main function to start log monitoring.
    
    :param logs_dir: Directory containing Spark/Hadoop user logs
    :param es_host: Elasticsearch host
    :param es_port: Elasticsearch port
    :param es_index: Elasticsearch index name
    :param es_scheme: Elasticsearch connection scheme
    :param es_api_key: Elasticsearch API key
    :param poll_interval: Polling interval for file system events
    """
    log_handler = SparkLogHandler(
        logs_dir=logs_dir, 
        es_host=es_host, 
        es_port=es_port, 
        es_index=es_index,
        es_scheme=es_scheme,
        es_user=es_user,
        es_password=es_password,
        es_api_key=es_api_key
    )

    # Create file system observer
    event_handler = SparkLogFileHandler(log_handler)
    observer = Observer()
    observer.schedule(event_handler, logs_dir, recursive=True)
    
    try:
        observer.start()
        print(f"Monitoring {logs_dir} for new log files...")
        
        # Keep script running
        while True:
            time.sleep(poll_interval)
    
    except KeyboardInterrupt:
        observer.stop()
    
    observer.join()

if __name__ == "__main__":
    # Example usage with environment variables or default values
    import os

    main(
        logs_dir=os.getenv('SPARK_LOGS_DIR', '/home/redakai/hadoop/logs/userlogs'),
        es_host=os.getenv('ES_HOST', 'localhost'),
        es_port=int(os.getenv('ES_PORT', 9200)),
        es_index=os.getenv('ES_INDEX', 'yarn_logs'),
        es_scheme=os.getenv('ES_SCHEME', 'https'),
        es_user=os.getenv('ES_USER', 'elastic'),
        es_password=os.getenv('ES_PASSWORD'),
        es_api_key=os.getenv('ES_API_KEY')
    )