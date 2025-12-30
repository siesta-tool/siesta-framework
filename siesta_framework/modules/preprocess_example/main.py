from siesta_framework.modules.example import run as example_run
from siesta_framework.core.interfaces import SiestaModule

class AnotherExample(SiestaModule):
    def __init__(self):
        self.name = "Another Example Module"
    
    def run(self, *args, **kwargs):
        return example_run(*args, **kwargs)
