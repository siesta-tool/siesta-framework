from typing import Any, Protocol, Dict
from abc import abstractmethod, ABCMeta

class SiestaModule(Protocol, metaclass=ABCMeta):
    """All Siesta modules must implement to be accessed by the framework.
       eg. ::
        class PreprocessorModule(IModule):
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
    
    name: str

    @abstractmethod
    def register_routes(self) -> Dict[str, Any] | None:
        """Return a dict of endpoint_name -> function."""
        pass

    def startup(self) -> None:
        """Lifecycle hook: Called when the framework starts."""
        pass

    def run(self, *args: Any, **kwargs: Any) -> Any:
        """Main execution method for the module."""
        pass