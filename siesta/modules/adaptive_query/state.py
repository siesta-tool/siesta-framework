"""
Process-wide adaptive query state shared by the query and index modules.

The adaptive query module and the adaptive indexer are separate module
instances, but both need to act on the same per-log state: the post-query
promotion workers, the per-perspective group counts and the LRU cache.
Keeping that state here (keyed by namespace and log) lets the indexer reset
it on ``clear_existing`` and lets evaluation code wait for pending
promotions before measuring.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Callable, Optional

from siesta.model.StorageModel import MetaData

logger = logging.getLogger(__name__)

# (namespace, log_name, pid) -> promotion queue; one worker thread per queue
# serialises build_pair_persistent calls so they cannot conflict on the
# perspective's LastChecked table.
_PROMOTION_QUEUES: dict[tuple[str, str, str], queue.Queue] = {}
_PROMOTION_LOCK = threading.Lock()

# (namespace, log_name, pid, event_count) -> number of groups
GROUP_COUNTS: dict[tuple, int] = {}


def _log_key(metadata: MetaData) -> tuple[str, str]:
    return (metadata.storage_namespace, metadata.log_name)


def get_promotion_queue(metadata: MetaData, pid: str) -> queue.Queue:
    """Return the promotion queue for (log, pid), starting its worker on first use."""
    key = (*_log_key(metadata), pid)
    with _PROMOTION_LOCK:
        q = _PROMOTION_QUEUES.get(key)
        if q is not None:
            return q
        q = queue.Queue()
        _PROMOTION_QUEUES[key] = q

        def _worker(q=q):
            while True:
                fn = q.get()
                try:
                    if fn is None:  # sentinel - shutdown
                        return
                    fn()
                except Exception as exc:
                    logger.error(
                        f"Promotion worker for '{key}' failed: {exc}",
                        exc_info=True,
                    )
                finally:
                    q.task_done()

        threading.Thread(
            target=_worker, daemon=True, name=f"promote-{key[1]}-{pid}"
        ).start()
        return q


def submit_promotion(metadata: MetaData, pid: str, fn: Callable[[], None]) -> None:
    get_promotion_queue(metadata, pid).put(fn)


def drain_promotions(
    metadata: Optional[MetaData] = None,
    pid: Optional[str] = None,
    timeout_s: Optional[float] = None,
) -> float:
    """
    Block until every matching promotion queue is empty and idle.

    With no metadata all logs are drained; with no pid all perspectives of
    the log.  Returns the seconds spent waiting.  Raises TimeoutError if the
    queues are still busy after ``timeout_s``.
    """
    t0 = time.time()
    with _PROMOTION_LOCK:
        queues = [
            q for (ns, log, p), q in _PROMOTION_QUEUES.items()
            if (metadata is None or (ns, log) == _log_key(metadata))
            and (pid is None or p == pid)
        ]
    for q in queues:
        while q.unfinished_tasks:
            if timeout_s is not None and time.time() - t0 > timeout_s:
                raise TimeoutError(
                    f"promotion queues still busy after {timeout_s:.0f}s"
                )
            time.sleep(0.05)
    return time.time() - t0


def reset_log_state(metadata: MetaData, drain_timeout_s: float = 3600.0) -> dict:
    """
    Drop all in-memory adaptive state of one log: pending promotions are
    drained first (so no worker writes after the reset), then the LRU cache,
    the group-count cache and the catalog singleton are cleared.
    """
    from siesta.modules.adaptive_index.catalog import evict_catalog
    from siesta.modules.adaptive_query.lru_cache import drop_lru_cache

    waited = drain_promotions(metadata, timeout_s=drain_timeout_s)
    lru_entries = drop_lru_cache(metadata)
    ns, log = _log_key(metadata)
    stale = [k for k in GROUP_COUNTS if k[0] == ns and k[1] == log]
    for k in stale:
        del GROUP_COUNTS[k]
    evict_catalog(metadata)
    invalidate_pair_files(f"s3a://{ns}/{log}/")
    logger.info(
        f"reset_log_state: {ns}/{log} - drained {waited:.2f}s, "
        f"dropped {lru_entries} LRU entries and {len(stale)} group counts."
    )
    return {
        "drained_s": waited,
        "lru_entries_dropped": lru_entries,
        "group_counts_dropped": len(stale),
    }


# ---------------------------------------------------------------------------
# Per-log storage options
# ---------------------------------------------------------------------------

# Options that decide how pair tables are written, set by every index and
# query request that carries them (see pair_attributes.py).  Process-wide so
# that maintenance (indexer) and promotion (query module) agree.
_LOG_OPTIONS: dict[tuple[str, str], dict] = {}


def set_log_option(metadata, key: str, value) -> None:
    _LOG_OPTIONS.setdefault((metadata.storage_namespace, metadata.log_name), {})[key] = value


def get_log_option(metadata, key: str, default=None):
    return _LOG_OPTIONS.get((metadata.storage_namespace, metadata.log_name), {}).get(key, default)


# ---------------------------------------------------------------------------
# Live files of persisted pair tables
# ---------------------------------------------------------------------------

# Resolving a Delta snapshot costs a small Spark job per table, and a long
# pattern reads dozens of pair tables.  Pair tables are written only by this
# process (promotion, maintenance, force-persist), and every such write
# invalidates the entry, so the cached file list is the table's current
# snapshot.
_PAIR_FILES: dict[str, list[str]] = {}
_PAIR_FILES_LOCK = threading.Lock()


def pair_table_files(spark, path: str) -> list[str]:
    with _PAIR_FILES_LOCK:
        files = _PAIR_FILES.get(path)
    if files is None:
        files = list(spark.read.format("delta").load(path).inputFiles())
        with _PAIR_FILES_LOCK:
            _PAIR_FILES[path] = files
    return files


def invalidate_pair_files(path_or_prefix: str) -> None:
    with _PAIR_FILES_LOCK:
        for k in [k for k in _PAIR_FILES if k.startswith(path_or_prefix)]:
            del _PAIR_FILES[k]
