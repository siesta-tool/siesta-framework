import importlib
import pkgutil
from typing import List, Type
from siesta_framework.core.interfaces import SiestaModule
import siesta_framework.modules as modules
import siesta_framework.core.sparkManager as sm

class Siesta:
    def __init__(self) -> None:
        pass

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
        sm.startup()
        discovered_modules = self.discover_modules()
        print(len(discovered_modules))
        print(f"Discovered Modules: {[mod.__name__ for mod in discovered_modules]}")
        for mod_class in discovered_modules:
            mod_instance = mod_class()
            print(f"--- Starting Module: {mod_instance.name} v{mod_instance.version} ---")
            mod_instance.startup()

    def shutdown(self) -> None:
        sm.shutdown()