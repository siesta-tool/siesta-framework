#!/usr/bin/env python3
'''
Before first execution: run install_dependencies.py to set up the environment.
    python3 install_dependencies.py


Example usage:
# Run API server (default config)
    python3 main.py

# Run API server with specific config
    python3 main.py --config config/siesta.config.json

# Run a specific module (CLI mode)
    python3 main.py --config config/preprocess.config.json preprocess
'''

#import sys;from pathlib import Path;sys.path.insert(0, str(Path(__file__).parent.parent))
from siesta.core.app import Siesta
from siesta.api import router
import argparse
import logging
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger('py4j').setLevel(logging.ERROR)
logging.getLogger('SLF4J').setLevel(logging.FATAL)
logging.getLogger('Spark').setLevel(logging.ERROR)
logging.getLogger('log4j').setLevel(logging.ERROR)


def main(args):
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
        # CLI mode: module name provided
        app = Siesta.with_args(args)

if __name__ == "__main__":
    main(sys.argv[1:])