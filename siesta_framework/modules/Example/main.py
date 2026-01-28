from typing import Any, Dict, Tuple
from siesta_framework.core.interfaces import SiestaModule
from siesta_framework.core.storageFactory import get_or_create_metadata, get_storage_manager

class Example(SiestaModule):
    def __init__(self):
        super().__init__()

    name = "Example Module"
    version = "1.0.0"

    type exampleSimpleType = int | str
    type exampleComplicatedType = Dict[str, Tuple[int, exampleSimpleType]]

    def startup(self):
        print(f"{self.name} v{self.version} initialized.")

    def register_routes(self) -> SiestaModule.ApiRoutes:
        return {
            "example_endpoint": ("GET", self.example_endpoint),
            "log_info": ("GET", self.get_log_info)
        }
    
    def cli_run(*args: Any, **kwargs: Any) -> Any:
        print(f"{Example.name} is running with args: {args} and kwargs: {kwargs}")
        print("Hello from Example Module!")

    def example_endpoint(self, request_data: str = "default") -> exampleComplicatedType|None:
        return None
    
    def get_log_info(self, log_name: str) -> Dict[str, Any]:
        """Example API endpoint that works with a specific log.
        
        In API mode, use get_or_create_metadata(log_name) to get/create
        metadata for the requested log.
        
        Args:
            log_name: The log/dataset name from the request.
            
        Returns:
            Dict with log metadata info.
        """
        metadata = get_or_create_metadata(log_name)
        return {
            "log_name": metadata.log_name,
            "storage_namespace": metadata.storage_namespace,
            "sequence_table_path": metadata.sequence_table_path
        }