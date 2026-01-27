import sys;from pathlib import Path;sys.path.insert(0, str(Path(__file__).parent.parent))
from siesta_framework.core.app import Siesta
from siesta_framework.api import router

args = sys.argv[1:]  # Get command-line arguments excluding the script name

if not args:
    app =  Siesta() #Siesta(config_path=str(Path(__file__).parent / 'config.example.json'))
    app.startup()
    router.startup(app)
    app.shutdown()
else:
    app = Siesta.with_args(args)
    app.shutdown()