import sys;from pathlib import Path;sys.path.insert(0, str(Path(__file__).parent.parent))
from siesta_framework.core.app import Siesta
from siesta_framework.api import router
import argparse

import logging
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger('py4j').setLevel(logging.ERROR)
logging.getLogger('SLF4J').setLevel(logging.FATAL)
logging.getLogger('Spark').setLevel(logging.ERROR)
logging.getLogger('log4j').setLevel(logging.ERROR)

'''
Example usage:
# Run API server (default config)
python3 main.py

# Run API server with specific config
python3 main.py --config config/siesta.config.json

# Run a specific module (CLI mode)
python3 main.py --config config/preprocess.config.json preprocess
'''

import logging
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger('py4j').setLevel(logging.ERROR)
logging.getLogger('SLF4J').setLevel(logging.FATAL)
logging.getLogger('Spark').setLevel(logging.ERROR)
logging.getLogger('log4j').setLevel(logging.ERROR)

'''
Example usage:
python3 main.py --config config/preprocess.config.json preprocess
python3 main.py
'''

args = sys.argv[1:]  # Get command-line arguments excluding the script name

# Check if running in CLI mode (module name provided) or API mode
if not args or (len(args) >= 1 and args[0].startswith('--')):
    # API mode: no module name, or only flags
    parser = argparse.ArgumentParser(description="Siesta Framework - API Mode")
    parser.add_argument('--config', type=str, help='Path to system configuration JSON file (otherwise, using default)', required=False)
    parsed_args = parser.parse_args(args)
    
    # Create Siesta instance with optional config
    config_path = parsed_args.config if parsed_args.config else None
    app = Siesta(config_path=config_path)
    app.startup()
    router.startup(app)
else:
    app = Siesta.with_args(args)