from typing import Any, Dict, ClassVar
from abc import ABC, abstractmethod

class SiestaModule(ABC):
    """All Siesta modules must implement to be accessed by the framework.
       eg. ::
        class PreprocessorModule(SiestaModule):
            name = "preprocessor"

            def register_routes(self):
                return {
                    "preprocess": self.preprocess_endpoint
                }

            def startup(self):
                # Initialization code here
                pass
            ...
    """
    
    name: ClassVar[str] = "Unnamed Module"
    version: ClassVar[str] = "unversioned"

    # @abstractmethod
    def register_routes(self) -> Dict[str, Any] | None:
        """Return a dict of endpoint_name -> function."""
        pass

    def startup(self) -> None:
        """Lifecycle hook: Called when the framework starts."""
        pass

    def run(*args: Any, **kwargs: Any) -> Any:
        """Main execution method for the module."""
        pass