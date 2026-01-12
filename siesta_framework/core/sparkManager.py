from typing import Any, Dict
from pyspark.sql import SparkSession
import os

spark_session = None

def startup(config: Dict[str, Any] = None) -> None:
    """Initialize Spark session with configuration.
    
    Args:
        config: Configuration dictionary containing Spark settings.
                Falls back to environment variables or default if config is not provided.
    """

    spark_master_url = config.get("spark_master", os.getenv("SPARK_MASTER", "local"))
    app_name = config.get("spark_app_name", "SiestaFramework")
    
    global spark_session 
    try:
        spark_session = SparkSession.builder \
            .appName(app_name) \
            .master(spark_master_url) \
            .getOrCreate()
        print(f"SparkSession initialized and connected to Spark Master at {spark_master_url}.")
    except Exception as e:
        raise RuntimeError(f"Failed to initialize SparkSession: {e}")

def get_spark_session():
    return spark_session

def shutdown():
    if spark_session:
        print("Shutting down SparkSession...")
        spark_session.stop()
        print("SparkSession stopped successfully.")