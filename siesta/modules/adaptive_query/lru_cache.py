import threading
from collections import OrderedDict
from typing import Optional, Tuple
from siesta.model.StorageModel import MetaData

_LRU_REGISTRY: dict[tuple[str, str], "PairLRUCache"] = {}
_LRU_LOCK = threading.Lock()

def get_lru_cache(metadata: MetaData, max_entries: int = 128) -> "PairLRUCache":
    key = (metadata.storage_namespace, metadata.log_name)
    with _LRU_LOCK:
        if key not in _LRU_REGISTRY:
            _LRU_REGISTRY[key] = PairLRUCache(max_entries)
        return _LRU_REGISTRY[key]
    

class PairLRUCache:
    """
    Bounded LRU cache for transient pair DataFrames, keyed by
    (pid, act_a, act_b).

    Values are collected pair lists (not DataFrames) so they survive
    Spark context changes.  The cache uses an OrderedDict for O(1) LRU
    eviction.
    """

    def __init__(self, max_entries: int = 128):
        self._max = max_entries
        self._cache: OrderedDict[Tuple[str, str, str], list] = OrderedDict()

    def get(
        self, pid: str, act_a: str, act_b: str
    ) -> Optional[list]:
        key = (pid, act_a, act_b)
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]
        return None

    def put(
        self, pid: str, act_a: str, act_b: str, rows: list
    ) -> None:
        key = (pid, act_a, act_b)
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = rows
        while len(self._cache) > self._max:
            self._cache.popitem(last=False)

    def contains(self, pid: str, act_a: str, act_b: str) -> bool:
        return (pid, act_a, act_b) in self._cache

    def invalidate_perspective(self, pid: str) -> None:
        """Remove all entries for a perspective (e.g. after ingest)."""
        keys = [k for k in self._cache if k[0] == pid]
        for k in keys:
            del self._cache[k]

    def clear(self) -> None:
        self._cache.clear()
