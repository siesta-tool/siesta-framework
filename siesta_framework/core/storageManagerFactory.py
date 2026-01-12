from typing import Dict, Any
from siesta_framework.core.interfaces import StorageManager
from siesta_framework.storage.S3.S3Manager import S3Manager


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
            storage_type = config.get("storage_type", "").lower()
            
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
