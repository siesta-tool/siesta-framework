import importlib
import pkgutil
import json
from pathlib import Path
from typing import Callable, List, Tuple, Type, Dict, Any
from siesta_framework.core.interfaces import SiestaModule, StorageManager
import siesta_framework.modules as modules
import siesta_framework.core.sparkManager as sparkManager
from siesta_framework.core.storageFactory import StorageManagerFactory, set_storage_manager, set_config, set_active_log
from siesta_framework.model.SystemModel import DEFAULT_CONFIG
import argparse

class Siesta:
    def __init__(self, config_path: str|None = None) -> None:
        self.config = self._load_config(config_path) if config_path else {}
        self.storage_manager = None
        self.registered_routes: Dict[str, SiestaModule.ApiRoutes|None] = {}

    @classmethod
    def with_args(cls, args:List[str]):
        """Alternative constructor to initialize Siesta with command-line arguments.
        
        Args:
            args: List of command-line arguments
            
        Returns:
            An instance of Siesta
        """
        parser = argparse.ArgumentParser(description="Siesta Framework Initialization")
        parser.add_argument('--config', type=str, help='Path to configuration JSON file')
        parser.add_argument('module', type=str, help='Module to run')
        
        parsed_args, unknown_args = parser.parse_known_args(args)
        
        app = cls(config_path=parsed_args.config)
        app.startup(cli_mode=True)

        for module in app.discovered_modules:
            if module.name == parsed_args.module:
                module().run(unknown_args)
                return app
        
        print(f"Module {parsed_args.module} not found")
        return app
    
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

    def startup(self, cli_mode: bool = False) -> None:
        """Start the Siesta framework.
        
        Args:
            cli_mode: If True, sets an active log from config (for CLI module runs).
                      If False (API mode), no active log is set - logs are handled per-request.
        """
        print("--- Starting Framework ---")
        
        # Set global config accessor
        set_config(self.config)
        
        # In CLI mode, set the active log from config
        # In API mode, logs are created on-demand per request
        if cli_mode:
            log_name = self.config.get("log_name", "default")
            self.metadata = set_active_log(log_name)
            print(f"CLI mode: Active log set to '{log_name}'")
        else:
            self.metadata = None
            print("API mode: No active log set (logs created per-request)")
        
        # Start Spark Manager
        sparkManager.startup(self.config)
        
        # Setup Storage Manager and set global accessor
        self.storage_manager = StorageManagerFactory.create_storage_manager(self.config, sparkManager)
        set_storage_manager(self.storage_manager)
        
        # Initialize Modules
        self.discovered_modules = self.discover_modules()

        print(f"Discovered Modules: {[mod.__name__ for mod in self.discovered_modules]}")
        for mod_class in self.discovered_modules:
            mod_instance = mod_class()
            print(f"--- Starting Module: {mod_instance.name} v{mod_instance.version} ---")
            mod_instance.startup()

        print("--- Framework Started ---")
    
    def get_registered_routes(self) -> Dict[str, SiestaModule.ApiRoutes|None]:
        if not self.discovered_modules:
            raise RuntimeError("Modules not discovered. Call startup() first.")

        for mod_class in self.discovered_modules:
            mod_instance = mod_class()
            routes = mod_instance.register_routes()
            if routes:
                self.registered_routes[mod_class.__name__.lower()] = routes
        
        return self.registered_routes

    def get_storage_manager(self) -> StorageManager:
        """Get the StorageManager instance.
        
        Returns:
            The initialized StorageManager instance
        """
        if self.storage_manager is None:
            raise RuntimeError("StorageManager not initialized. Call startup() first.")
        return self.storage_manager

    def shutdown(self) -> None:
        print("--- Shutting Down Framework ---")
        sparkManager.shutdown()