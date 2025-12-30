from typing import Any, Protocol, Dict
from siesta_framework.core.interfaces import SiestaModule

class ExampleModule(SiestaModule):
    def __init__(self):
        super().__init__()
        self.name = "Example Module"
        self.version = "1.0.0"

    def startup(self):
        print(f"{self.name} v{self.version} initialized.")

    def register_routes(self) -> Dict[str, Any]:
        return {
            "example_endpoint": self.example_endpoint
        }
    
    def run(self, *args: Any, **kwargs: Any) -> Any:
        print(f"{self.name} is running with args: {args} and kwargs: {kwargs}")

    def example_endpoint(self, request_data: Any) -> str:
        return f"Example endpoint received: {request_data}"