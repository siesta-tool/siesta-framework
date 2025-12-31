from typing import Any, Dict
from siesta_framework.core.interfaces import SiestaModule
from pyspark.sql import SparkSession
import os

class SparkManager(SiestaModule):
    def __init__(self):
        super().__init__()
        self.spark_session = None

    name = "Spark Manager Module"
    version = "1.0.0"

    def startup(self):
        print(f"{self.name} v{self.version} initialized.")
        spark_master_url = os.getenv("SPARK_MASTER", "local")
        self.spark_session = SparkSession.builder \
            .appName("SiestaFramework") \
            .master(spark_master_url) \
            .getOrCreate()
        print(f"SparkSession initialized and connected to Spark Master at {spark_master_url}.")

    def register_routes(self) -> Dict[str, Any]:
        return {
            "example_endpoint": self.example_endpoint
        }
    
    def run(self, *args: Any, **kwargs: Any) -> Any:
        print(f"{self.name} is running with args: {args} and kwargs: {kwargs}")
        print("Hello from Spark Manager Module!")

    def example_endpoint(self, request_data: Any) -> str:
        return f"Example endpoint received: {request_data}"

    def get_spark_session(self):
        return self.spark_session

    def shutdown(self):
        if self.spark_session:
            print("Shutting down SparkSession...")
            self.spark_session.stop()
            print("SparkSession stopped successfully.")