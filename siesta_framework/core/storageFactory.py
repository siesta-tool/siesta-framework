from typing import Dict, Any, Optional
from siesta_framework.core.interfaces import StorageManager
from siesta_framework.storage.S3.S3Manager import S3Manager
from siesta_framework.model.StorageModel import MetaData
from siesta_framework.core.config import get_config

# Global storage manager instance
_storage_manager: Optional[StorageManager] = None
# Cache of metadata instances per log name
_metadata_cache: Dict[str, MetaData] = {}
# Current active log name (set for CLI mode, None for API mode)
_active_log_name: Optional[str] = None


def get_metadata(log_name: Optional[str] = None) -> Optional[MetaData]:
    """Get the MetaData instance for a specific log/dataset.
    
    In CLI mode (with active log), returns the active log's metadata if no log_name specified.
    In API mode, log_name must be provided or returns None.
    
    Args:
        log_name: Optional log name. If None, uses the active log (CLI mode).
        
    Returns:
        The MetaData instance, or None if not found/initialized.
    """
    target_log = log_name or _active_log_name
    if target_log is None:
        return None
    return _metadata_cache.get(target_log)


def get_or_create_metadata(log_name: str) -> MetaData:
    """Get existing or create new MetaData for a log/dataset.
    
    This is the preferred method for API mode where logs are specified per-request.
    
    Args:
        log_name: The log/dataset name.
        
    Returns:
        MetaData instance for the log.
        
    Raises:
        RuntimeError: If config is not available.
    """
    if log_name in _metadata_cache:
        return _metadata_cache[log_name]
    
    config = get_config()
    if not config:
        raise RuntimeError("Configuration not available. Ensure framework is started.")
    
    metadata = MetaData(
        storage_namespace=config.get("storage_namespace", "siesta"),
        log_name=log_name
    )
    metadata.is_continued = config.get("is_continued", False)
    metadata.is_streaming = config.get("is_streaming", False)
    
    _metadata_cache[log_name] = metadata
    return metadata


def set_active_log(log_name: str) -> MetaData:
    """Set the active log for CLI mode and create its metadata.
    
    Args:
        log_name: The log/dataset name to set as active.
        
    Returns:
        MetaData instance for the active log.
    """
    global _active_log_name
    _active_log_name = log_name
    return get_or_create_metadata(log_name)


def get_active_log_name() -> Optional[str]:
    """Get the current active log name (CLI mode only).
    
    Returns:
        The active log name, or None if in API mode.
    """
    return _active_log_name


def clear_metadata_cache() -> None:
    """Clear all cached metadata instances."""
    global _metadata_cache, _active_log_name
    _metadata_cache = {}
    _active_log_name = None


def get_storage_manager() -> Optional[StorageManager]:
    """Get the global storage manager instance.
    
    Returns:
        The initialized StorageManager instance, or None if not initialized.
    """
    return _storage_manager


def set_storage_manager(manager: StorageManager) -> None:
    """Set the global storage manager instance.
    
    Args:
        manager: The StorageManager instance to set as global.
    """
    global _storage_manager
    _storage_manager = manager


class StorageManagerFactory:
    """
    Factory class for creating storage manager instances based on configuration.
    
    This allows the framework to dynamically select the appropriate storage backend
    (S3, Cassandra, PostgreSQL, etc.) without hardcoding dependencies.
    """
    
    _registry: Dict[str, type] = {
        "s3": S3Manager,
        # Add more storage managers here as they are implemented
        # "cassandra": CassandraManager,
        # "postgres": PostgresManager,
    }
    
    @classmethod
    def register_storage_manager(cls, name: str, manager_class: type) -> None:
        """
        Register a new storage manager type.
        
        Args:
            name: Identifier for the storage manager (e.g., "s3", "cassandra")
            manager_class: Class that implements StorageManager interface
        """
        if not issubclass(manager_class, StorageManager):
            raise TypeError(f"{manager_class} must be a subclass of StorageManager")
        cls._registry[name.lower()] = manager_class
        print(f"Registered storage manager: {name} -> {manager_class.__name__}")
    
    @classmethod
    def create_storage_manager(cls, config: Dict[str, Any], spark_manager) -> StorageManager:
        """
        Create and return a storage manager instance based on configuration.
        
        Args:
            config: Configuration dictionary that must contain a "storage_type" key
            spark_manager: The spark manager instance to pass to the storage manager
            
        Returns:
            An instance of the appropriate StorageManager implementation
            
        Raises:
            ValueError: If storage_type is not specified or not recognized
            RuntimeError: If initialization fails
        """
        try:
            storage_type = config.get("storage_type", "s3").lower()
            
            if not storage_type:
                raise ValueError(
                    "Configuration must specify 'storage_type'. "
                    f"Available options: {', '.join(cls._registry.keys())}"
                )
            
            manager_class = cls._registry.get(storage_type)
            
            if manager_class is None:
                raise ValueError(
                    f"Unknown storage type: '{storage_type}'. "
                    f"Available options: {', '.join(cls._registry.keys())}"
                )
            
            print(f"Creating storage manager: {storage_type} ({manager_class.name} v{manager_class.version})")
            return manager_class(spark_manager, config)
            
        except (ValueError, RuntimeError) as e:
            print(f"Error initializing storage manager: {e}")
            print(f"Available storage types: {', '.join(cls.get_available_storage_types())}")
            raise
    
    @classmethod
    def get_available_storage_types(cls) -> list[str]:
        """
        Get a list of all registered storage manager types.
        
        Returns:
            List of storage type identifiers
        """
        return list(cls._registry.keys())
