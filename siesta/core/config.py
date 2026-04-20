"""
Global configuration accessor for Siesta Framework.

This module provides a centralized way to access the system configuration
from anywhere in the framework without needing to pass config through
multiple layers.
"""

from typing import Dict, Any, Optional
from pathlib import Path
import json
from siesta.model.SystemModel import DEFAULT_SYSTEM_CONFIG
import logging
logger = logging.getLogger(__name__)

_config: Optional[Dict[str, Any]] = None


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from a JSON file and merge with defaults.
    
    Args:
        config_path: Path to the configuration JSON file. If None, returns DEFAULT_CONFIG.
        
    Returns:
        Dictionary containing configuration merged with defaults
    """
    # Start with default config
    config = DEFAULT_SYSTEM_CONFIG.copy()
    
    if not config_path:
        return config
    
    config_file = Path(config_path)
    if not config_file.exists():
        logger.warning(f"Config file {config_path} not found. Using default config.")
        return config
    
    try:
        with open(config_file, 'r') as f:
            user_config = json.load(f)
            # Merge user config with defaults
            config.update(user_config)
            logger.info(f"Configuration loaded from {config_path} and merged with defaults")
            return config
    except Exception as e:
        logger.error(f"Error loading config from {config_path}: {e}. Using default config.")
        return config


def initialize_config(config: Dict[str, Any]) -> None:
    """Initialize the global configuration.
    
    Should be called once during application startup (in app.py).
    
    Args:
        config: Configuration dictionary to make globally available
    """
    global _config
    _config = config


def get_system_config() -> Dict[str, Any]:
    """Get the current system configuration.
    
    Returns:
        Configuration dictionary. Returns DEFAULT_CONFIG if not initialized.
    """
    if _config is None:
        return DEFAULT_SYSTEM_CONFIG
    return _config


def get_config_value(key: str, default: Any = None) -> Any:
    """Get a specific configuration value.
    
    Args:
        key: Configuration key to retrieve
        default: Default value if key not found
        
    Returns:
        Configuration value or default
    """
    config = get_system_config()
    return config.get(key, default)


def is_initialized() -> bool:
    """Check if configuration has been initialized.
    
    Returns:
        True if config is initialized, False otherwise
    """
    return _config is not None
