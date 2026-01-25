from typing import Any
from siesta_framework.modules.Example.main import Example
from siesta_framework.core.interfaces import SiestaModule
from siesta_framework.core.storageFactory import get_storage_manager, get_metadata
from .parse_log import parse_log_file


class Preprocessor(SiestaModule):
        
    name = "preprocess"
    version = "1.2.0"

    def startup(self):
        # print(dir(example_module))
        Example.run()

        
    def run(self, *args: Any, **kwargs: Any) -> Any:
        print(f"{self.name} is running with args: {args} and kwargs: {kwargs}")
        
        module = args[0] if args else None
        match module:
            case ["testparse"]:
                print("Running testparse...")
                import time
                # while True:
                time.sleep(10)
                    # break
                exit(0)
                events_rdd = parse_log_file()
                
                storage = get_storage_manager()
                metadata = get_metadata()
                if storage and metadata:
                    storage.write_sequence_table(events_rdd, metadata)