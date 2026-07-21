import json
import re
from typing import Annotated, Any, Dict

from fastapi import Body, Query
from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import SparkSession

from siesta.core.config import get_system_config
from siesta.core.interfaces import SiestaModule, StorageManager
from siesta.core.sparkManager import get_spark_session
from siesta.core.storageFactory import get_storage_manager
from siesta.model.StorageModel import MetaData

import logging

logger = logging.getLogger(__name__)

# Only read-only statements are allowed through the ad-hoc query endpoint, since it
# runs directly against Spark SQL views backed by the storage tables.
_READ_ONLY_SQL = re.compile(r"^(SELECT|WITH|SHOW|DESCRIBE|EXPLAIN)\b", re.IGNORECASE)


class AdHocQueryConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    log_name: str = Field("example_log", description="Name of the indexed log to query")
    storage_namespace: str = Field("siesta", description="Storage namespace")
    sql: str = Field(..., description="Read-only SQL (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN) to run against the log's tables. See the `tables` endpoint for available table names.")
    row_limit: int = Field(1000, description="Maximum number of result rows to return")


DEFAULT_ADHOC_QUERY_CONFIG: Dict[str, Any] = AdHocQueryConfig(sql="SELECT 1").model_dump()


class Manager(SiestaModule):

    name = "manager"
    version = "1.0.0"
    spark: SparkSession
    storage: StorageManager
    siesta_config: Dict[str, Any]

    def startup(self):
        logger.info("Manager startup complete.")

    def register_routes(self) -> SiestaModule.ApiRoutes | None:
        return {
            "namespaces": ("GET", self.api_namespaces),
            "delete_namespace": ("DELETE", self.api_delete_namespace),
            "log_metadata": ("GET", self.api_log_metadata),
            "delete_log": ("DELETE", self.api_delete_log),
            "tables": ("GET", self.api_tables),
            "query": ("POST", self.api_query),
        }

    # ------------------------------------------------------------------
    # API entry points
    # ------------------------------------------------------------------

    def api_namespaces(self) -> Any:
        """List all storage namespaces along with the log names stored in each.

        Returns a summary of the storage backend: the number of namespaces
        (e.g. S3 buckets), their names, and the log names found under each one.
        """
        logger.info(f"{self.name} listing namespaces via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        namespaces = self.storage.list_namespaces()
        namespace_logs = {
            namespace: self.storage.list_logs(namespace) for namespace in namespaces
        }

        return {
            "namespace_count": len(namespaces),
            "namespaces": [
                {
                    "name": namespace,
                    "log_count": len(logs),
                    "logs": logs,
                }
                for namespace, logs in namespace_logs.items()
            ],
        }

    def api_delete_namespace(
        self,
        storage_namespace: str = Query(..., description="Storage namespace to permanently delete"),
    ) -> Any:
        """Permanently delete a storage namespace and every log stored within it.

        **Query params:**
        - `storage_namespace` *(str)* - namespace to delete. **Required.**
        """
        logger.info(f"{self.name} deleting namespace '{storage_namespace}' via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        if storage_namespace not in self.storage.list_namespaces():
            raise ValueError(f"Namespace '{storage_namespace}' does not exist.")

        deleted = self.storage.delete_namespace(storage_namespace)
        return {"deleted": deleted, "storage_namespace": storage_namespace}

    def api_log_metadata(
        self,
        log_name: str = Query(..., description="Name of the indexed log"),
        storage_namespace: str = Query("siesta", description="Storage namespace"),
        include_alphabet: bool = Query(False, description="Include the activity alphabet (requires a distinct scan of the activity index)"),
        include_indexed_pairs: bool = Query(False, description="Include the set of indexed activity pairs (requires a distinct scan of the pairs index)"),
    ) -> Any:
        """Return the stored metadata for a given log within a storage namespace.

        Always includes total trace and event counts and first/last observed
        timestamps. The activity alphabet and set of indexed activity pairs are
        each opt-in, since computing them requires an extra distinct scan.

        **Query params:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        - `include_alphabet` *(bool, default: `false`)* - include the activity alphabet.
        - `include_indexed_pairs` *(bool, default: `false`)* - include the set of indexed activity pairs.
        """
        logger.info(f"{self.name} fetching metadata for '{log_name}' in '{storage_namespace}' via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        metadata = MetaData(storage_namespace=storage_namespace, log_name=log_name)
        metadata = self.storage.read_metadata_table(metadata)

        result = metadata.to_dict()

        if include_alphabet:
            result["activity_alphabet"] = sorted(
                row["activity"]
                for row in self.storage.read_activity_index(metadata).select("activity").distinct().collect()
            )
        if include_indexed_pairs:
            result["indexed_pairs"] = [
                {"source": row["source"], "target": row["target"]}
                for row in self.storage.read_pairs_index(metadata).select("source", "target").distinct().collect()
            ]

        return result

    def api_delete_log(
        self,
        log_name: str = Query(..., description="Name of the indexed log to permanently delete"),
        storage_namespace: str = Query("siesta", description="Storage namespace"),
    ) -> Any:
        """Permanently delete a log and all of its indexed tables from a storage namespace.

        **Query params:**
        - `log_name` *(str)* - name of the indexed log to delete. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        """
        logger.info(f"{self.name} deleting log '{log_name}' in '{storage_namespace}' via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        if not self.storage.log_exists({"log_name": log_name, "storage_namespace": storage_namespace}):
            raise ValueError(f"Log '{log_name}' does not exist in namespace '{storage_namespace}'.")

        metadata = MetaData(storage_namespace=storage_namespace, log_name=log_name)
        deleted = self.storage.delete_log(metadata)
        return {"deleted": deleted, "log_name": log_name, "storage_namespace": storage_namespace}

    def api_tables(
        self,
        log_name: str = Query(..., description="Name of the indexed log"),
        storage_namespace: str = Query("siesta", description="Storage namespace"),
    ) -> Any:
        """List the canonical table names available for ad-hoc SQL queries against a log.

        **Query params:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        """
        logger.info(f"{self.name} listing tables for '{log_name}' in '{storage_namespace}' via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        metadata = MetaData(storage_namespace=storage_namespace, log_name=log_name)
        return {
            "log_name": log_name,
            "storage_namespace": storage_namespace,
            "tables": self.storage.list_tables(metadata),
        }

    def api_query(
        self,
        query_config: Annotated[AdHocQueryConfig, Body(openapi_examples={
            "default": {
                "summary": "Count events per activity",
                "value": {
                    "log_name": "example_log",
                    "storage_namespace": "siesta",
                    "sql": "SELECT activity, COUNT(*) AS event_count FROM activity_index GROUP BY activity ORDER BY event_count DESC",
                    "row_limit": 1000,
                },
            },
        })],
    ) -> Any:
        """Execute an ad-hoc, read-only SQL query over any of a log's stored tables via Spark.

        Every table returned by the `tables` endpoint is registered as a temporary Spark
        SQL view (named after the table) scoped to this request, then the supplied query
        is executed against them. Only SELECT/WITH/SHOW/DESCRIBE/EXPLAIN statements are
        permitted.

        **Body params:** see `AdHocQueryConfig`.
        """
        logger.info(
            f"{self.name} executing ad-hoc query on '{query_config.log_name}' "
            f"in '{query_config.storage_namespace}' via API."
        )
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()
        spark = get_spark_session()

        sql_text = query_config.sql.strip()
        if not _READ_ONLY_SQL.match(sql_text):
            raise ValueError("Only read-only SELECT/WITH/SHOW/DESCRIBE/EXPLAIN statements are permitted.")

        metadata = MetaData(storage_namespace=query_config.storage_namespace, log_name=query_config.log_name)

        registered_tables = []
        for table_name in self.storage.list_tables(metadata):
            try:
                self.storage.read_table(metadata, table_name).createOrReplaceTempView(table_name)
                registered_tables.append(table_name)
            except Exception as e:
                logger.warning(f"Skipping table '{table_name}' - could not register as view: {e}")

        try:
            result_df = spark.sql(sql_text).limit(query_config.row_limit)
            rows = [json.loads(r) for r in result_df.toJSON().collect()]
        except Exception as e:
            logger.exception(f"Ad-hoc query failed: {e}")
            raise ValueError(f"Query execution failed: {e}")

        return {
            "columns": result_df.columns,
            "row_count": len(rows),
            "rows": rows,
            "available_tables": registered_tables,
        }
