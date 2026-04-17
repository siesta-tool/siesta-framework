"""
Shared fixtures for Preprocessor integration tests.

Initializes Siesta with local Spark, runs the Preprocessor on a small
CSV dataset, and exposes the storage manager + metadata so every test
can read tables back from S3 (MinIO).
"""
import pytest
import os
import socket
import logging

logging.basicConfig(level=logging.INFO)

TEST_LOG_NAME = "test_preprocess_output"
TEST_NAMESPACE = "siesta-test"


def _minio_reachable(host="localhost", port=9000, timeout=2):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, ConnectionRefusedError):
        return False


def _force_clean_tables(log_name: str, namespace: str):
    """Physically delete and recreate all Delta tables for a log.

    When ``overwrite_data=True`` deletes S3 objects via boto3, Spark's
    in-process Delta log cache may still hold stale metadata from a prior
    pytest session.  This function clears the Delta cache, physically
    removes the directories via Hadoop FS, and recreates empty tables
    to guarantee a truly clean starting state.
    """
    from siesta_framework.core.sparkManager import get_spark_session
    from siesta_framework.model.StorageModel import MetaData
    from siesta_framework.model.DataModel import (
        Event, EventPair, Last_Checked_table_schema,
        Trace_metadata_table_schema, count_table_schema,
    )

    spark = get_spark_session()
    md = MetaData(storage_namespace=namespace, log_name=log_name, storage_type="s3")

    # Clear Delta's internal transaction-log cache
    try:
        spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    except Exception:
        pass

    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    tables = [
        (Event.get_schema(), md.sequence_table_path, None),
        (Trace_metadata_table_schema, md.trace_metadata_table_path, None),
        (Last_Checked_table_schema, md.last_checked_table_path, None),
        (EventPair.get_schema(), md.pairs_index_path, ("source", "target")),
        (count_table_schema, md.count_table_path, ("source",)),
    ]
    for schema, path, parts in tables:
        # Physically delete via Hadoop FileSystem API
        try:
            fs_path = jvm.org.apache.hadoop.fs.Path(path)
            fs = fs_path.getFileSystem(hadoop_conf)
            if fs.exists(fs_path):
                fs.delete(fs_path, True)
        except Exception:
            pass

        # Recreate empty Delta table
        writer = (spark.createDataFrame([], schema=schema)
                  .write.format("delta").mode("overwrite"))
        if parts:
            writer = writer.partitionBy(*parts)
        writer.option("overwriteSchema", "true").save(path)


@pytest.fixture(scope="session")
def siesta_app():
    """Initialize Siesta framework with local Spark for testing."""
    if not _minio_reachable():
        pytest.skip("MinIO is not running at localhost:9000")

    from siesta_framework.core.app import Siesta

    config_path = os.path.join(
        os.path.dirname(__file__), "..", "config", "siesta.config.json"
    )
    app = Siesta(config_path=config_path)
    app.config["spark_master"] = "local[*]"
    app.config["spark_driver_memory"] = "2g"
    app.startup()

    # Ensure consistent timestamp handling across environments
    from siesta_framework.core.sparkManager import get_spark_session
    get_spark_session().conf.set("spark.sql.session.timeZone", "UTC")

    yield app


@pytest.fixture(scope="session")
def preprocess_config():
    """Preprocess configuration for the test dataset."""
    csv_path = os.path.join(
        os.path.dirname(__file__), "..", "datasets", "test_preprocess.csv"
    )
    return {
        "log_name": TEST_LOG_NAME,
        "log_path": csv_path,
        "storage_namespace": TEST_NAMESPACE,
        "overwrite_data": True,
        "enable_streaming": False,
        "lookback": "7d",
        "field_mappings": {
            "csv": {
                "activity": "activity",
                "trace_id": "trace_id",
                "position": "position",
                "start_timestamp": "timestamp",
                "attributes": ["resource", "cost"],
            }
        },
        "trace_level_fields": ["trace_id"],
        "timestamp_fields": ["start_timestamp"],
    }


@pytest.fixture(scope="session")
def preprocessed(siesta_app, preprocess_config):
    """Run the Preprocessor and return storage + metadata for verification."""
    from siesta_framework.core.storageFactory import get_storage_manager
    from siesta_framework.model.StorageModel import MetaData
    from siesta_framework.modules.Preprocess.main import Preprocessor

    storage = get_storage_manager()

    preprocessor = Preprocessor()
    preprocessor.siesta_config = siesta_app.config
    preprocessor.storage = storage
    preprocessor._load_preprocess_config(preprocess_config)
    preprocessor.storage.initialize_db(preprocessor.preprocess_config)
    _force_clean_tables(TEST_LOG_NAME, TEST_NAMESPACE)
    preprocessor.begin_builders(caller="cli")

    metadata = MetaData(
        storage_namespace=preprocess_config["storage_namespace"],
        log_name=preprocess_config["log_name"],
        storage_type="s3",
    )

    return {"storage": storage, "metadata": metadata, "config": preprocess_config}


INCR_LOG_NAME = "test_preprocess_incr"


@pytest.fixture(scope="session")
def preprocessed_incremental(siesta_app):
    """Run batch 1 then batch 2 on a separate log_name for incremental tests.

    Batch 1 (test_preprocess.csv):
      t1: A -> B -> C,  t2: A -> D,  t3: B -> C -> D -> E

    Batch 2 (test_preprocess_batch2.csv):
      t1: ... -> D -> E  (extend),  t4: C -> A -> B  (new trace)
    """
    from siesta_framework.core.storageFactory import get_storage_manager
    from siesta_framework.model.StorageModel import MetaData
    from siesta_framework.modules.Preprocess.main import Preprocessor

    storage = get_storage_manager()
    datasets_dir = os.path.join(os.path.dirname(__file__), "..", "datasets")

    base_config = {
        "log_name": INCR_LOG_NAME,
        "storage_namespace": TEST_NAMESPACE,
        "enable_streaming": False,
        "lookback": "7d",
        "field_mappings": {
            "csv": {
                "activity": "activity",
                "trace_id": "trace_id",
                "position": "position",
                "start_timestamp": "timestamp",
                "attributes": ["resource", "cost"],
            }
        },
        "trace_level_fields": ["trace_id"],
        "timestamp_fields": ["start_timestamp"],
    }

    # --- Batch 1 (clean slate) ---
    b1_config = {**base_config, "overwrite_data": True,
                 "log_path": os.path.join(datasets_dir, "test_preprocess.csv")}
    p1 = Preprocessor()
    p1.siesta_config = siesta_app.config
    p1.storage = storage
    p1._load_preprocess_config(b1_config)
    p1.storage.initialize_db(p1.preprocess_config)
    _force_clean_tables(INCR_LOG_NAME, TEST_NAMESPACE)
    p1.begin_builders(caller="cli")

    # --- Batch 2 (incremental) ---
    b2_config = {**base_config, "overwrite_data": False,
                 "log_path": os.path.join(datasets_dir, "test_preprocess_batch2.csv")}
    p2 = Preprocessor()
    p2.siesta_config = siesta_app.config
    p2.storage = storage
    p2._load_preprocess_config(b2_config)
    p2.storage.initialize_db(p2.preprocess_config)
    p2.begin_builders(caller="cli")

    metadata = MetaData(
        storage_namespace=TEST_NAMESPACE,
        log_name=INCR_LOG_NAME,
        storage_type="s3",
    )

    return {"storage": storage, "metadata": metadata}


QUERY_LOG_NAME = "test_query_detection"


@pytest.fixture(scope="session")
def query_preprocessed(siesta_app):
    """Preprocess datasets/test.csv for query integration tests.

    Traces
    ------
    trace_1 : A(r1,clerk) -> B(r2,analyst) -> C(r3,system)
    trace_2 : A(r1,clerk) -> C(r3,system)
    trace_3 : B(r2,analyst) -> C(r3,system) -> D(r4,manager)
    trace_4 : A(r1,clerk) -> D(r4,manager)

    All four attribute columns (resource, role, cost, lifecycle) are indexed.
    """
    from siesta_framework.core.storageFactory import get_storage_manager
    from siesta_framework.model.StorageModel import MetaData
    from siesta_framework.modules.Preprocess.main import Preprocessor

    csv_path = os.path.join(
        os.path.dirname(__file__), "..", "datasets", "test.csv"
    )
    config = {
        "log_name": QUERY_LOG_NAME,
        "log_path": csv_path,
        "storage_namespace": TEST_NAMESPACE,
        "overwrite_data": True,
        "enable_streaming": False,
        "lookback": "7d",
        "field_mappings": {
            "csv": {
                "activity": "activity",
                "trace_id": "trace_id",
                "position": "position",
                "start_timestamp": "timestamp",
                "attributes": ["resource", "role", "cost", "lifecycle"],
            }
        },
        "trace_level_fields": ["trace_id"],
        "timestamp_fields": ["start_timestamp"],
    }

    storage = get_storage_manager()
    preprocessor = Preprocessor()
    preprocessor.siesta_config = siesta_app.config
    preprocessor.storage = storage
    preprocessor._load_preprocess_config(config)
    preprocessor.storage.initialize_db(preprocessor.preprocess_config)
    _force_clean_tables(QUERY_LOG_NAME, TEST_NAMESPACE)
    preprocessor.begin_builders(caller="cli")

    metadata = MetaData(
        storage_namespace=TEST_NAMESPACE,
        log_name=QUERY_LOG_NAME,
        storage_type="s3",
    )

    return {"storage": storage, "metadata": metadata, "config": config}
