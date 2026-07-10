from typing import Any, ClassVar, Dict

from fastapi import Query
from pyspark.sql import SparkSession

from siesta.core.config import get_system_config
from siesta.core.interfaces import SiestaModule, StorageManager
from siesta.core.storageFactory import get_storage_manager
from siesta.model.StorageModel import MetaData

import logging

logger = logging.getLogger(__name__)


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
            "log_metadata": ("GET", self.api_log_metadata),
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

    def api_log_metadata(
        self,
        log_name: str = Query(..., description="Name of the indexed log"),
        storage_namespace: str = Query("siesta", description="Storage namespace"),
    ) -> Any:
        """Return the stored metadata for a given log within a storage namespace.

        **Query params:**
        - `log_name` *(str)* - name of the indexed log. **Required.**
        - `storage_namespace` *(str, default: `"siesta"`)* - storage namespace.
        """
        logger.info(f"{self.name} fetching metadata for '{log_name}' in '{storage_namespace}' via API.")
        self.siesta_config = get_system_config()
        self.storage = get_storage_manager()

        metadata = MetaData(storage_namespace=storage_namespace, log_name=log_name)
        metadata = self.storage.read_metadata_table(metadata)
        return metadata.to_dict()
