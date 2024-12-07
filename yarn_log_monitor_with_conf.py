import os
import sys
import time
import json
import boto3
import logging
import datetime
import re
import subprocess
from typing import List, Dict, Any, Optional
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LogFileHandler(FileSystemEventHandler):
    """Handler for monitoring log files"""

    def __init__(self, storage_backends: List['LogStorageBackend'], cluster_id: str):
        self.storage_backends = storage_backends
        self.cluster_id = cluster_id

    def on_created(self, event):
        self._handle_event(event)

    def on_modified(self, event):
        self._handle_event(event)

    def _handle_event(self, event):
        if event.is_directory or not event.src_path.endswith('.log'):
            return
        logger.info(f"Log file event detected: {event.src_path}")
        self._process_log_file(event.src_path)

    def _process_log_file(self, file_path: str):
        try:
            filename = os.path.basename(file_path)
            parts = filename.replace('.log', '').split('_')
            step_id = parts[0]
            application_id = '_'.join(parts[1:])
            log_content = self._read_log_file(file_path)
            errors = self._extract_errors(log_content)

            log_metadata = self._construct_log_metadata(
                file_path, filename, application_id, step_id, log_content, errors
            )

            self._store_logs(log_metadata, step_id, errors)

        except Exception as e:
            logger.error(f"Error processing log file {file_path}: {e}")

    def _read_log_file(self, file_path: str) -> str:
        with open(file_path, 'r') as f:
            return f.read()

    def _extract_errors(self, log_content: str) -> List[str]:
        error_patterns = [r'ERROR', r'Exception', r'Failed', r'FAILED']
        errors = []
        for pattern in error_patterns:
            matches = re.finditer(pattern, log_content, re.MULTILINE)
            for match in matches:
                line_start = log_content.rfind('\n', 0, match.start()) + 1
                line_end = log_content.find('\n', match.end())
                line_end = line_end if line_end != -1 else len(log_content)
                errors.append(log_content[line_start:line_end].strip())
        return errors

    def _construct_log_metadata(self, file_path: str, filename: str, application_id: str, step_id: str,
                                log_content: str, errors: List[str]) -> Dict[str, Any]:
        return {
            'file_path': file_path,
            'file_name': filename,
            'application_id': application_id,
            'step_id': step_id,
            'timestamp': time.time(),
            'log_date': datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d'),
            'cluster_id': self.cluster_id,
            'log_content': log_content,
            'errors': errors,
            'has_errors': len(errors) > 0
        }

    def _store_logs(self, log_metadata: Dict[str, Any], step_id: str, errors: List[str]):
        for backend in self.storage_backends:
            if not backend.store_log(log_metadata):
                logger.error(f"Failed to store logs in backend {backend.__class__.__name__}")
            else:
                logger.info(f"Successfully stored logs for step {step_id}")
                if errors:
                    logger.error(f"Found {len(errors)} errors in step {step_id}:")
                    for error in errors[:5]:
                        logger.error(f"  - {error}")


class LogStorageBackend(ABC):
    """Abstract base class for log storage backends"""

    @abstractmethod
    def store_log(self, log_metadata: Dict[str, Any]) -> bool:
        pass

    @abstractmethod
    def initialize(self) -> bool:
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
            self.s3_client = boto3.client('s3', region_name=self.config.get('region', 'us-east-1'))
            self.s3_client.list_buckets()
            return True
        except Exception as e:
            self.logger.error(f"S3 initialization error: {e}")
            return False

    def store_log(self, log_metadata: Dict[str, Any]) -> bool:
        if not self.s3_client:
            self.logger.error("S3 client not initialized")
            return False

        try:
            timestamp = datetime.datetime.fromtimestamp(log_metadata['timestamp'])
            s3_key = self._generate_s3_key(log_metadata, timestamp)
            self._store_log_content(log_metadata, s3_key)
            self._store_log_metadata(log_metadata, s3_key)
            self.logger.info(f"Successfully stored logs at s3://{self.bucket}/{s3_key}")
            return True
        except Exception as e:
            self.logger.error(f"Error storing log in S3: {e}")
            return False

    def _generate_s3_key(self, log_metadata: Dict[str, Any], timestamp: datetime.datetime) -> str:
        return (f"{self.prefix}/"
                f"step_id={log_metadata['step_id']}/"
                f"application_id={log_metadata['application_id']}/{log_metadata['file_name']}")

    def _store_log_content(self, log_metadata: Dict[str, Any], s3_key: str):
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=log_metadata['log_content']
        )

    def _store_log_metadata(self, log_metadata: Dict[str, Any], s3_key: str):
        metadata_key = f"{s3_key}.metadata.json"
        metadata_content = {k: v for k, v in log_metadata.items() if k != 'log_content'}
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=metadata_key,
            Body=json.dumps(metadata_content)
        )


class EMRCluster:
    def __init__(self):
        self.cluster_id = None
        self.emr_client = boto3.client('emr')

    def initialize(self) -> bool:
        try:
            self.cluster_id = self._fetch_cluster_id()
            return True
        except Exception as e:
            logger.error(f"Failed to initialize EMR cluster: {e}")
            return False

    def _fetch_cluster_id(self) -> str:
        try:
            json_file_path = '/mnt/var/lib/info/job-flow.json'
            with open(json_file_path, 'r') as file:
                job_flow_data = json.load(file)

            cluster_id = job_flow_data.get('jobFlowId')
            if not cluster_id:
                raise ValueError("Cluster ID is empty")
            logger.info(f"Fetched cluster ID: {cluster_id}")
            return cluster_id
        except Exception as e:
            logger.error(f"Failed to fetch cluster ID: {e}")
            raise RuntimeError("Unable to determine cluster ID")

    def get_running_steps(self) -> List[Dict[str, Any]]:
        try:
            response = self.emr_client.list_steps(
                ClusterId=self.cluster_id,
                StepStates=['RUNNING', 'PENDING', 'FAILED']
            )

            steps = response.get('Steps', [])
            return [
                {
                    'step_id': step['Id'],
                    'name': step['Name'],
                    'status': step['Status']['State'],
                    'created_at': step['Status']['Timeline']['CreationDateTime'],
                    'start_time': step['Status']['Timeline'].get('StartDateTime'),
                    'action_on_failure': step['ActionOnFailure']
                }
                for step in steps
            ]

        except Exception as e:
            logger.error(f"Error getting EMR steps: {e}")
            return []


class YarnLogManager:
    def __init__(self, logs_dir: str, storage_backends: List[LogStorageBackend], cluster_id: str):
        self.logs_dir = logs_dir
        self.storage_backends = storage_backends
        self.observer = Observer()
        self.event_handler = LogFileHandler(storage_backends, cluster_id)
        self.completed_steps = {}

    def start_monitoring(self):
        os.makedirs(self.logs_dir, exist_ok=True)
        self.observer.schedule(self.event_handler, self.logs_dir, recursive=False)
        self.observer.start()
        logger.info(f"Started monitoring directory: {self.logs_dir}")

    def stop_monitoring(self):
        self.observer.stop()
        self.observer.join()
        logger.info("Stopped monitoring")

    def extract_application_id(self, step_id: str) -> Optional[str]:
        step_id_log_path = "/var/log/hadoop/steps"
        log_dir = os.path.join(step_id_log_path, step_id)
        if not os.path.exists(log_dir):
            return None

        app_id_pattern = r'(application_\d+_\d+)'
        for root, _, files in os.walk(log_dir):
            for file in files:
                if file in ['controller', 'stderr', 'stdout', 'syslog']:
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                            log_content = f.read()
                        match = re.search(app_id_pattern, log_content)
                        if match:
                            return match.group(1)
                    except Exception as e:
                        logger.warning(f"Error reading file {file_path}: {e}")
        logger.info(f"No application ID found for step {step_id}")
        return None

    def fetch_logs(self, application_id: str, step_id: str) -> bool:
        try:
            output_file = os.path.join(self.logs_dir, f"{step_id}_{application_id}.log")
            logger.info(f"Fetching YARN logs for application {application_id}")
            cmd = f"yarn logs -applicationId {application_id}"
            process = subprocess.Popen(
                cmd.split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                logger.error(f"Error fetching logs: {stderr}")
                return False

            with open(output_file, 'w') as f:
                f.write(stdout)

            logger.info(f"Successfully saved YARN logs to {output_file}")
            return True

        except Exception as e:
            logger.error(f"Error fetching YARN logs: {e}")
            return False


def load_config(config_file: str) -> Dict[str, Any]:
    with open(config_file, 'r') as f:
        return json.load(f)


def initialize_emr_cluster() -> EMRCluster:
    emr = EMRCluster()
    if not emr.initialize():
        raise RuntimeError("Failed to initialize EMR cluster")
    return emr


def initialize_storage_backends(config: Dict[str, Any], emr: EMRCluster) -> List[LogStorageBackend]:
    storage_backends = []
    if config.get('s3', {}).get('enabled', False):
        s3_backend = S3Backend(config['s3'], emr._fetch_cluster_id())
        if s3_backend.initialize():
            storage_backends.append(s3_backend)
        else:
            raise RuntimeError("Failed to initialize S3 backend")
    if not storage_backends:
        raise ValueError("No storage backends enabled in configuration")
    return storage_backends


def monitor_emr_steps(emr: EMRCluster, log_manager: YarnLogManager):
    logger.info("Starting EMR steps monitoring")
    previous_running_steps = set()
    while True:
        logger.debug("Fetching current running steps")
        current_steps = emr.get_running_steps()
        current_running_steps = {step['step_id'] for step in current_steps}
        completed_steps = previous_running_steps - current_running_steps

        if completed_steps:
            logger.info(f"Found {len(completed_steps)} newly completed steps")
        for step_id in completed_steps:
            logger.debug(f"Recording completion time for step {step_id}")
            log_manager.completed_steps[step_id] = time.time()

        all_steps_to_process = current_steps + [
            {'step_id': step_id} for step_id, completion_time in list(log_manager.completed_steps.items())
            if time.time() - completion_time <= 60
        ]
        logger.debug(f"Processing {len(all_steps_to_process)} steps")

        for step in all_steps_to_process:
            application_id = log_manager.extract_application_id(step['step_id'])
            if application_id:
                logger.info(f"Step ID: {step['step_id']}, Application ID: {application_id}")
                log_manager.fetch_logs(application_id, step['step_id'])
            else:
                logger.warning(f"No application ID found for step {step['step_id']}")

        current_time = time.time()
        original_count = len(log_manager.completed_steps)
        log_manager.completed_steps = {
            step_id: completion_time
            for step_id, completion_time in log_manager.completed_steps.items()
            if current_time - completion_time <= 60
        }
        if original_count != len(log_manager.completed_steps):
            logger.debug(f"Cleaned up {original_count - len(log_manager.completed_steps)} expired completed steps")

        previous_running_steps = current_running_steps
        logger.debug("Waiting for next monitoring cycle")
        time.sleep(30)


def main(config_file: str):
    config = load_config(config_file)
    emr = initialize_emr_cluster()
    storage_backends = initialize_storage_backends(config, emr)
    cluster_id = emr._fetch_cluster_id()

    log_manager = YarnLogManager(config['logs_dir'], storage_backends, cluster_id)
    try:
        log_manager.start_monitoring()
        monitor_emr_steps(emr, log_manager)
    except KeyboardInterrupt:
        log_manager.stop_monitoring()
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        log_manager.stop_monitoring()
        raise


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python yarn_log_monitor_with_conf.py config.json")
        sys.exit(1)
    main(sys.argv[1])
