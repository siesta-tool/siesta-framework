from typing import Any, Dict
from siesta_framework.core.interfaces import SiestaModule

class Example(SiestaModule):
    def __init__(self):
        super().__init__()

    name = "Example Module"
    version = "1.0.0"

    def startup(self):
        print(f"{self.name} v{self.version} initialized.")

    def register_routes(self) -> Dict[str, Any]:
        return {
            "example_endpoint": self.example_endpoint
        }
    
    def run(*args: Any, **kwargs: Any) -> Any:
        print(f"{Example.name} is running with args: {args} and kwargs: {kwargs}")
        print("Hello from Example Module!")

    def example_endpoint(self, request_data: Any) -> str:
        return f"Example endpoint received: {request_data}"