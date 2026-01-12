import importlib
import pkgutil
import json
from pathlib import Path
from typing import List, Type, Dict, Any
from siesta_framework.core.interfaces import SiestaModule, StorageManager
import siesta_framework.modules as modules
import siesta_framework.core.sparkManager as sm
from siesta_framework.core.storageFactory import StorageManagerFactory
from siesta_framework.model.SystemModel import DEFAULT_CONFIG

class Siesta:
    def __init__(self, config_path: str = None) -> None:
        self.config = self._load_config(config_path) if config_path else {}
        self.storage_manager = None
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from a JSON file and merge with defaults.
        
        Args:
            config_path: Path to the configuration JSON file
            
        Returns:
            Dictionary containing configuration merged with defaults
        """
        # Start with default config
        config = DEFAULT_CONFIG.copy()
        
        config_file = Path(config_path)
        if not config_file.exists():
            print(f"Warning: Config file {config_path} not found. Using default config.")
            return config
        
        try:
            with open(config_file, 'r') as f:
                user_config = json.load(f)
                # Merge user config with defaults
                config.update(user_config)
                print(f"Configuration loaded from {config_path} and merged with defaults")
                return config
        except Exception as e:
            print(f"Error loading config from {config_path}: {e}. Using default config.")
            return config

    def discover_modules(self) -> List[Type[SiestaModule]]:
        discovered: set[Type[SiestaModule]] = set()

        for module_info in pkgutil.iter_modules(modules.__path__):
            module_name = f"siesta_framework.modules.{module_info.name}.main"

            mod = importlib.import_module(module_name)

            for obj in vars(mod).values():
                if (
                    isinstance(obj, type)
                    and issubclass(obj, SiestaModule)
                    and obj is not SiestaModule
                ):
                    discovered.add(obj)

        return list(discovered)

    def startup(self) -> None:
        print("--- Starting Framework ---")
        
        # Start Spark Manager
        sm.startup(self.config)
        
        # Setup Storage Manager
        self.storage_manager = StorageManagerFactory.create_storage_manager(self.config, sm)
        
        discovered_modules = self.discover_modules()
        print(f"Discovered Modules: {[mod.__name__ for mod in discovered_modules]}")
        for mod_class in discovered_modules:
            mod_instance = mod_class()
            print(f"--- Starting Module: {mod_instance.name} v{mod_instance.version} ---")
            mod_instance.startup()
    
    def get_storage_manager(self) -> StorageManager:
        """Get the StorageManager instance.
        
        Returns:
            The initialized StorageManager instance
        """
        if self.storage_manager is None:
            raise RuntimeError("StorageManager not initialized. Call startup() first.")
        return self.storage_manager

    def shutdown(self) -> None:
        sm.shutdown()