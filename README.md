# YARN Log Monitor for EMR Clusters

## Overview

The YARN Log Monitor is a sophisticated Python script designed to automatically monitor, capture, and store log files from Amazon EMR (Elastic MapReduce) cluster steps. It provides a robust solution for log management that helps track and analyze step executions by:

- Monitoring log files in real-time
- Extracting application IDs for EMR steps
- Fetching YARN logs
- Storing logs in multiple backends (currently supporting Amazon S3)
- Tracking running and completed steps

## Features

- Real-time log file monitoring using watchdog
- Automatic log file processing
- Error extraction from log files
- Multiple storage backend support (S3)
- EMR cluster step tracking
- Configurable log storage and monitoring

## Prerequisites

- Python 3.7+
- boto3 library
- watchdog library
- AWS credentials with appropriate EMR and S3 permissions
- Running on an EMR cluster

## Installation and Deployment

### Manual Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/dev-redakai/yarn_monitor.git
   cd yarn-log-monitor
   ```

2. Install required dependencies:
   ```bash
   pip install boto3 watchdog
   ```

### EMR Bootstrap Script Deployment

The project includes a comprehensive bootstrap script that automates the installation and setup of the YARN Log Monitor on Amazon EMR clusters. This script provides a robust, automated way to deploy the monitoring solution.

#### Bootstrap Script Features

- Automatic package installation
- Error handling and retry mechanisms
- Parallel package installation
- Logging of installation process
- Background script execution

#### Using the Bootstrap Script

When creating an EMR cluster, you can specify this bootstrap script to automatically set up the YARN Log Monitor:

1. Save the bootstrap script (provided in the repository) to a location accessible by your EMR cluster (e.g., S3 bucket)

2. When creating your EMR cluster, add the bootstrap script under "Bootstrap Actions":
   - Action type: Custom action
   - Script location: `s3://your-bucket/path/to/bootstrap_script.sh`

3. Ensure the EMR cluster's IAM role has necessary permissions:
   - S3 read access
   - Ability to install packages
   - Network access to required repositories

#### Bootstrap Script Workflow

1. Installs system and Python packages
2. Clones the YARN Log Monitor repository
3. Installs Python dependencies
4. Starts the monitoring script in the background
5. Logs all installation and startup activities

**Note**: The bootstrap script assumes the repository URL and configuration file path. Modify these to match your specific setup.

## Configuration

Create a `config.json` file with the following structure:

```json
{
    "logs_dir": "/path/to/dump/local/logs/directory",
    "s3": {
        "enabled": true,
        "bucket": "your-s3-bucket-name",
        "prefix": "yarn-logs",
        "region": "us-east-1"
    }
}
```

### Configuration Parameters

- `logs_dir`: Local directory to store captured log files
- `s3.enabled`: Enable/disable S3 backend storage
- `s3.bucket`: S3 bucket name for log storage
- `s3.prefix`: Prefix for S3 object keys
- `s3.region`: AWS region for S3 bucket

## Usage

Run the script directly on your EMR cluster:

```bash
python yarn_log_monitor_with_conf.py config.json
```

## How It Works

1. **Initialization**
   - Loads configuration from the specified JSON file
   - Initializes EMR cluster information
   - Sets up storage backends (currently S3)

2. **Log Monitoring**
   - Watches a specified directory for new or modified `.log` files
   - Extracts metadata from log filenames
   - Processes log files to identify errors

3. **Step Tracking**
   - Monitors running EMR cluster steps
   - Extracts application IDs for completed steps
   - Fetches YARN logs for identified steps

4. **Log Storage**
   - Stores log content in configured backends
   - Creates separate metadata files for easy indexing
   - Supports extensible storage backend architecture

## Error Handling

The script includes comprehensive error handling:
- Logs errors and warnings
- Continues operation even if individual step log processing fails
- Provides detailed logging for troubleshooting

## Logging

Utilizes Python's logging module with:
- Timestamp
- Log level
- Detailed error messages
- Console output

## Security Considerations

- Requires AWS credentials with appropriate permissions
- Stores logs securely in S3
- Handles log file reading with error tolerance

## Extending the Project

### Adding New Storage Backends
- Implement a new class inheriting from `LogStorageBackend`
- Override `store_log()` and `initialize()` methods
- Add backend initialization in `initialize_storage_backends()`

### Customizing the Bootstrap Script

The bootstrap script can be extended to support more complex deployment scenarios:

1. **Package Management**
   - Add more package installations
   - Configure custom package sources
   - Add package version pinning

2. **Configuration Management**
   - Support dynamic configuration generation
   - Add configuration validation
   - Implement configuration templating

3. **Error Handling**
   - Enhance logging capabilities
   - Add more detailed error reporting
   - Implement advanced retry mechanisms

4. **Security Enhancements**
   - Add credential management
   - Implement additional security checks
   - Support encryption of sensitive information

Example of extending the bootstrap script:
```bash
# Add custom package installation
python3 -m pip install additional-package1 additional-package2

# Dynamic configuration generation
python3 generate_config.py > /home/hadoop/yarn_monitor/config/dynamic_config.json

# Enhanced logging
exec > >(tee -a "$EXTENDED_LOG_FILE") 2>&1
```

## Limitations

- Currently supports only S3 as a storage backend
- Designed specifically for EMR clusters
- Requires running on the EMR cluster node


## Disclaimer

Ensure you have appropriate AWS permissions and understand the potential costs associated with log storage and processing.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [watchdog](https://github.com/gorakhargosh/watchdog) for file system monitoring
- Uses [boto3](https://github.com/boto/boto3) for AWS S3 integration
---

## Support

For support, please open an issue in the GitHub repository or contact [manikantgautam3@gmail.com]
