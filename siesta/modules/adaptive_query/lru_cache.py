import threading
from collections import OrderedDict
from typing import Optional, Tuple
from siesta.model.StorageModel import MetaData

DEFAULT_LRU_CAPACITY = 128

_LRU_REGISTRY: dict[tuple[str, str], "PairLRUCache"] = {}
_LRU_LOCK = threading.Lock()


def get_lru_cache(
    metadata: MetaData, max_entries: Optional[int] = None
) -> "PairLRUCache":
    """
    Return the log's LRU cache, creating it on first use.  A given
    ``max_entries`` resizes an existing cache (evicting if it shrinks).
    """
    key = (metadata.storage_namespace, metadata.log_name)
    with _LRU_LOCK:
        cache = _LRU_REGISTRY.get(key)
        if cache is None:
            cache = PairLRUCache(max_entries or DEFAULT_LRU_CAPACITY)
            _LRU_REGISTRY[key] = cache
        elif max_entries is not None:
            cache.resize(max_entries)
        return cache


def drop_lru_cache(metadata: MetaData) -> int:
    """Remove the log's cache from the registry; returns the entries dropped."""
    key = (metadata.storage_namespace, metadata.log_name)
    with _LRU_LOCK:
        cache = _LRU_REGISTRY.pop(key, None)
    return len(cache) if cache is not None else 0


class PairLRUCache:
    """
    Bounded LRU cache for transient pair DataFrames, keyed by
    (pid, act_a, act_b).

    Values are collected pair lists (not DataFrames) so they survive
    Spark context changes.  The cache uses an OrderedDict for O(1) LRU
    eviction.
    """

    def __init__(self, max_entries: int = DEFAULT_LRU_CAPACITY):
        self._max = max_entries
        self._cache: OrderedDict[Tuple[str, str, str], list] = OrderedDict()
        self._lock = threading.RLock()
        self.evictions = 0

    def __len__(self) -> int:
        return len(self._cache)

    @property
    def capacity(self) -> int:
        return self._max

    def resize(self, max_entries: int) -> None:
        with self._lock:
            self._max = max_entries
            self._evict()

    def _evict(self) -> None:
        while len(self._cache) > self._max:
            self._cache.popitem(last=False)
            self.evictions += 1

    def get(
        self, pid: str, act_a: str, act_b: str
    ) -> Optional[list]:
        key = (pid, act_a, act_b)
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
            return None

    def put(
        self, pid: str, act_a: str, act_b: str, rows: list
    ) -> None:
        key = (pid, act_a, act_b)
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = rows
            self._evict()

    def contains(self, pid: str, act_a: str, act_b: str) -> bool:
        with self._lock:
            return (pid, act_a, act_b) in self._cache

    def keys(self) -> list[Tuple[str, str, str]]:
        with self._lock:
            return list(self._cache)

    def invalidate_perspective(self, pid: str) -> None:
        """Remove all entries for a perspective (e.g. after ingest)."""
        with self._lock:
            keys = [k for k in self._cache if k[0] == pid]
            for k in keys:
                del self._cache[k]

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
