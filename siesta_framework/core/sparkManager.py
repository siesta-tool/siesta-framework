from typing import Any, Dict
from pyspark.sql import SparkSession
import os

spark_session = None

def startup() -> None:
    spark_master_url = os.getenv("SPARK_MASTER", "local")
    global spark_session 
    spark_session = SparkSession.builder \
        .appName("SiestaFramework") \
        .master(spark_master_url) \
        .getOrCreate()
    print(f"SparkSession initialized and connected to Spark Master at {spark_master_url}.")

def get_spark_session():
    return spark_session

def shutdown():
    if spark_session:
        print("Shutting down SparkSession...")
        spark_session.stop()
        print("SparkSession stopped successfully.")