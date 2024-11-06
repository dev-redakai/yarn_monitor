# YARN Log Monitor

A robust Python-based monitoring system for Apache YARN and Spark logs with flexible storage backend support. This tool automatically detects new log files and stores them in your choice of Elasticsearch, Amazon S3, or PostgreSQL (or any combination of these).

## Features

- 🔍 Real-time monitoring of YARN/Spark log directories
- 📦 Multiple storage backend support:
  - Elasticsearch for full-text search capabilities
  - Amazon S3 for durable cloud storage
  - PostgreSQL for structured data storage
- 🔄 Concurrent storage to multiple backends
- 📂 Organized storage structure with metadata preservation
- 🚀 Easy configuration via JSON
- 📝 Comprehensive logging and error handling
- 🔌 Modular design for easy extension

## Prerequisites

- Python 3.7+
- Access to one or more of the following:
  - Elasticsearch cluster
  - AWS S3 bucket
  - PostgreSQL database
- YARN/Spark cluster with accessible log directory

## Installation

1. Clone the repository:
```bash
git clone https://github.com/dev-redakai/yarn_monitor.git
cd yarn-log-monitor
```

2. Create and activate a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Create a `config.json` file with your desired settings. Example configuration:

```json
{
    "logs_dir": "/path/to/yarn/logs/userlogs",
    "log_level": "INFO",
    
    "elasticsearch": {
        "enabled": true,
        "host": "localhost",
        "port": 9200,
        "scheme": "https",
        "user": "elastic",
        "password": "your-password",
        "index": "yarn_logs"
    },
    
    "s3": {
        "enabled": true,
        "aws_access_key_id": "your-access-key",
        "aws_secret_access_key": "your-secret-key",
        "region": "us-west-2",
        "bucket": "your-bucket-name",
        "prefix": "yarn-logs"
    },
    
    "postgres": {
        "enabled": true,
        "host": "localhost",
        "port": 5432,
        "dbname": "yarn_logs",
        "user": "postgres",
        "password": "your-password"
    }
}
```

### Configuration Options

#### General Settings
- `logs_dir`: Directory path containing YARN/Spark logs
- `log_level`: Logging level (INFO, DEBUG, WARNING, ERROR)

#### Elasticsearch Settings
- `enabled`: Enable/disable Elasticsearch storage
- `host`: Elasticsearch host
- `port`: Elasticsearch port
- `scheme`: Connection scheme (http/https)
- `user`: Elasticsearch username
- `password`: Elasticsearch password
- `index`: Index name for storing logs

#### S3 Settings
- `enabled`: Enable/disable S3 storage
- `aws_access_key_id`: AWS access key
- `aws_secret_access_key`: AWS secret key
- `region`: AWS region
- `bucket`: S3 bucket name
- `prefix`: Prefix for log files in bucket (optional)

#### PostgreSQL Settings
- `enabled`: Enable/disable PostgreSQL storage
- `host`: PostgreSQL host
- `port`: PostgreSQL port
- `dbname`: Database name
- `user`: PostgreSQL username
- `password`: PostgreSQL password

## Usage

1. Start the monitor:
```bash
python yarn_log_monitor.py config.json
```

2. The monitor will:
   - Watch for new log files in the specified directory
   - Parse log metadata and content
   - Store logs in enabled backends
   - Provide real-time feedback via logging

## Storage Structure

### Elasticsearch
- Logs are stored as documents in the specified index
- Each document contains:
  - File path
  - Application ID
  - Container ID
  - Timestamp
  - Log content
  - Full metadata

### Amazon S3
- Logs are organized in a hierarchical structure:
  ```
  bucket/
  └── prefix/
      └── YYYY/
          └── MM/
              └── DD/
                  └── application_id/
                      ├── log_file
                      └── log_file.metadata.json
  ```

### PostgreSQL
- Logs are stored in a table with the following schema:
  ```sql
  CREATE TABLE yarn_logs (
      id SERIAL PRIMARY KEY,
      file_path VARCHAR(1024),
      file_name VARCHAR(256),
      application_id VARCHAR(256),
      container_id VARCHAR(256),
      timestamp TIMESTAMP,
      log_content TEXT,
      metadata JSONB
  )
  ```

## Development

### Adding a New Storage Backend

1. Create a new class that inherits from `LogStorageBackend`
2. Implement the required methods:
   - `initialize()`
   - `store_log()`
3. Add configuration handling in the main function

Example:
```python
class NewBackend(LogStorageBackend):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
    def initialize(self) -> bool:
        # Initialize connection/client
        pass
        
    def store_log(self, log_metadata: Dict[str, Any]) -> bool:
        # Store log data
        pass
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [watchdog](https://github.com/gorakhargosh/watchdog) for file system monitoring
- Uses [elasticsearch-py](https://github.com/elastic/elasticsearch-py) for Elasticsearch integration
- Uses [boto3](https://github.com/boto/boto3) for AWS S3 integration
- Uses [psycopg2](https://github.com/psycopg/psycopg2) for PostgreSQL integration

---

## Support

For support, please open an issue in the GitHub repository or contact [manikantgautam3@gmail.com]