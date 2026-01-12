import sys;from pathlib import Path;sys.path.insert(0, str(Path(__file__).parent.parent))
from siesta_framework.core.app import Siesta

app = Siesta(config_path=str(Path(__file__).parent / 'config.example.json'))
app.startup()
app.shutdown()