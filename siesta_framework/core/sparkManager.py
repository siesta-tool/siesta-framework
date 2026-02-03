from typing import Any, Dict
from pyspark.sql import SparkSession
import os
import subprocess
import zipfile
from pathlib import Path
import logging
_s_logger = logging.getLogger('py4j.java_gateway')
_s_logger.setLevel(logging.ERROR)

# Ensure PySpark uses its bundled Spark
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

spark_session = None

def get_docker_bridge_ip():
    """Get Docker bridge gateway IP dynamically."""
    try:
        result = subprocess.run(
            ["ip", "-4", "addr", "show", "docker0"],
            capture_output=True,
            text=True,
            timeout=2
        )
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if 'inet ' in line:
                    ip = line.strip().split()[1].split('/')[0]
                    return ip
    except:
        pass
    
    # Fallback to default
    return "172.17.0.1"

def startup(config: Dict[str, Any] = {}) -> None:
    """Initialize Spark session with configuration.
    
    Args:
        config: Configuration dictionary containing Spark settings.
                Falls back to environment variables or default if config is not provided.
    """

    spark_master_url = config.get("spark_master", os.getenv("SPARK_MASTER", "local[*]"))
    app_name = config.get("spark_app_name", "SiestaFramework")
    driver_memory = config.get("spark_driver_memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
    executor_memory = config.get("spark_executor_memory", os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))
    
    global spark_session 
    try:
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(spark_master_url)
        
        # Required JVM options for Java 17+ compatibility
        java_options = (
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
            "--add-opens=java.base/java.io=ALL-UNNAMED "
            "--add-opens=java.base/java.net=ALL-UNNAMED "
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED "
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
            "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
        )
        
        # Hadoop AWS packages for S3 support and Delta Lake and Kafka
        packages = "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.540,io.delta:delta-spark_2.13:4.0.0,io.delta:delta-storage:4.0.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
        
        builder = builder \
            .config("spark.driver.extraJavaOptions", java_options) \
            .config("spark.executor.extraJavaOptions", java_options) \
            .config("spark.driver.memory", driver_memory) \
            .config("spark.executor.memory", executor_memory) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.pyspark.python", "/opt/bitnami/python/bin/python3") \
            .config("spark.pyspark.driver.python", "python3") \
            .config("spark.executorEnv.PYSPARK_PYTHON", "/opt/bitnami/python/bin/python3") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
            .config("spark.databricks.delta.autoCompact.enabled", "true") \
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
            .config("spark.jars.packages", packages) 
        
        # Configure S3 credentials if provided
        if config and config.get("s3_access_key") and config.get("s3_secret_key"):
            builder = builder \
                .config("spark.hadoop.fs.s3a.access.key", config["s3_access_key"]) \
                .config("spark.hadoop.fs.s3a.secret.key", config["s3_secret_key"])
            print(f"S3 credentials configured from config file.")
        
        # Configure S3 endpoint if provided
        if config and config.get("s3_endpoint"):
            s3_endpoint = config["s3_endpoint"]
            # Translate localhost to Docker bridge IP
            bridge_ip = get_docker_bridge_ip()
            executor_s3_endpoint = s3_endpoint.replace("localhost", bridge_ip).replace("127.0.0.1", bridge_ip)
            builder = builder.config("spark.hadoop.fs.s3a.endpoint", executor_s3_endpoint)
            print(f"S3 endpoint configured: {executor_s3_endpoint} (Docker bridge: {bridge_ip})")
        
        spark_session = builder.getOrCreate()
        spark_session.sparkContext.setLogLevel("ERROR")
        print(f"SparkSession initialized and connected to Spark Master at {spark_master_url}.")
        
        # Ship code to executors
        try:
            current_file = Path(__file__).resolve()
            package_dir = current_file.parent.parent # siesta_framework directory
            project_root = package_dir.parent # Project root containing siesta_framework
            
            zip_path = "/tmp/siesta_framework.zip"
            
            # Create zip with only .py files to reduce size
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                siesta_dir = project_root / "siesta_framework"
                for py_file in siesta_dir.rglob("*.py"):
                    arcname = py_file.relative_to(project_root)
                    zipf.write(py_file, arcname)
            
            spark_session.sparkContext.addPyFile(zip_path)
            print(f"Shipped code to executors: {zip_path}")
        except Exception as e:
            print(f"Warning: Failed to ship code to executors: {e}")

    except Exception as e:
        raise RuntimeError(f"Failed to initialize SparkSession: {e}")

def get_spark_session() -> SparkSession:
    """Get the active Spark session.
    Returns:
        Active SparkSession instance.
    """
    if spark_session is None:
        raise RuntimeError("SparkSession not initialized. Call startup() first.")
    return spark_session

def shutdown():
    if spark_session:
        print("Shutting down SparkSession...")
        spark_session.stop()
        print("SparkSession stopped successfully.")