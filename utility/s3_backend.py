import logging
from typing import Dict, Any
import boto3, json
from datetime import datetime
from utility.log_storage_backend import LogStorageBackend

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
            # Create S3 key based on cluster ID, step ID, and timestamp
            timestamp = datetime.fromtimestamp(log_metadata['timestamp'])
            s3_key = f"{self.prefix}/{timestamp.strftime('log_date=%Y-%m-%d')}/{self.cluster_id}/{log_metadata['step_id']}/{log_metadata['log_type']}/{timestamp}.log"
            
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
