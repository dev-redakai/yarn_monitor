from abc import ABC, abstractmethod
from typing import Dict, Any

class LogStorageBackend(abc.ABC):
    """Abstract base class for log storage backends"""
    
    @abstractmethod
    def store_log(self, log_metadata: Dict[str, Any]) -> bool:
        """Store log data in the backend"""
        pass

    @abstractmethod
    def initialize(self) -> bool:
        """Initialize the storage backend"""
        pass
