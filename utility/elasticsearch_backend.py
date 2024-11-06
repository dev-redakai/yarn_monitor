
import logging
from typing import Dict, Any
from elasticsearch import Elasticsearch
from utility.log_storage_backend import LogStorageBackend

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
