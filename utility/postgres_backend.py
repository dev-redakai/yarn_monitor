import logging
from typing import Dict, Any
import psycopg2
from datetime import datetime
from psycopg2.extras import Json
from utility.log_storage_backend import LogStorageBackend

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
