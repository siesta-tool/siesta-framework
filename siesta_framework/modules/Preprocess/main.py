from siesta_framework.modules.Example.main import Example
from siesta_framework.core.interfaces import SiestaModule

class Preprocessor(SiestaModule):
        
    name = "Another Example Module"
    version = "1.2.0"

    def startup(self):
        # print(dir(example_module))
        Example.run()
    