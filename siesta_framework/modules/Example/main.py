from typing import Any, Dict, Tuple
from siesta_framework.core.interfaces import SiestaModule

class Example(SiestaModule):
    def __init__(self):
        super().__init__()

    name = "Example Module"
    version = "1.0.0"

    type exampleSimpleType = int | str
    type exampleComplicatedType = Dict[str, Tuple[int, exampleSimpleType]]

    def startup(self):
        print(f"{self.name} v{self.version} initialized.")

    def register_routes(self) -> SiestaModule.ApiRoutes:
        return {
            "example_endpoint": ("GET", self.example_endpoint)
        }
    
    def run(*args: Any, **kwargs: Any) -> Any:
        print(f"{Example.name} is running with args: {args} and kwargs: {kwargs}")
        print("Hello from Example Module!")

    def example_endpoint(self, request_data: str = "default") -> exampleComplicatedType|None:
        return None