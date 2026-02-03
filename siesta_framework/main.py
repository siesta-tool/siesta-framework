import sys;from pathlib import Path;sys.path.insert(0, str(Path(__file__).parent.parent))
from siesta_framework.core.app import Siesta
from siesta_framework.api import router

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

if not args:
    app =  Siesta() #Siesta(config_path=str(Path(__file__).parent / 'config.example.json'))
    app.startup()
    router.startup(app)
else:
    app = Siesta.with_args(args)