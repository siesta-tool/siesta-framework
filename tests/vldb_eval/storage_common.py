"""
tests/vldb_eval/storage_common.py — on-disk size of a SIESTA log's tables.

Sizes are read from each Delta table's current snapshot (the ``add``
actions of its log), so files removed by later commits (overwrites,
compactions) are not counted; this is what a VACUUMed table would occupy.
Every table lives under ``s3://siesta/<log>/``; adaptive tables are under
``<log>/adaptive/<pid>/...`` (one table per persisted pair).

``table_sizes(log)`` -> {table_path: {"bytes", "files", "kind", "pid", "pair"}}
``summarise(sizes)``  -> bytes per kind (sequence_table, activity_index,
                         pairs_index, adaptive_pairs, adaptive_overlays,
                         catalog, last_checked, other)
"""

from __future__ import annotations

import collections

import boto3

from tests.vldb_eval.pattern_common import S3_OPTIONS

BUCKET = "siesta"


def _s3():
    return boto3.client(
        "s3", endpoint_url=S3_OPTIONS["AWS_ENDPOINT_URL"],
        aws_access_key_id=S3_OPTIONS["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=S3_OPTIONS["AWS_SECRET_ACCESS_KEY"],
    )


def delta_tables(log: str) -> list[str]:
    """Key prefixes of all Delta tables under the log."""
    out = set()
    for page in _s3().get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{log}/"):
        for o in page.get("Contents", []):
            k = o["Key"]
            if "/_delta_log/" in k:
                out.add(k.split("/_delta_log/")[0])
    return sorted(out)


def _classify(log: str, path: str) -> tuple[str, str | None, str | None]:
    rel = path[len(log) + 1:]
    parts = rel.split("/")
    if parts[0] != "adaptive":
        return parts[0], None, None
    if parts[1] == "catalog":
        return "catalog", None, None
    pid = parts[1]
    if len(parts) >= 4 and parts[2] == "pairs":
        return "adaptive_pairs", pid, parts[3]
    if parts[2] == "last_checked":
        return "adaptive_last_checked", pid, None
    return "adaptive_overlays", pid, None


def table_sizes(log: str) -> dict[str, dict]:
    from deltalake import DeltaTable

    out = {}
    for path in delta_tables(log):
        try:
            dt = DeltaTable(f"s3://{BUCKET}/{path}", storage_options=S3_OPTIONS)
            adds = dt.get_add_actions(flatten=True)
            sizes = adds.column("size_bytes").to_pylist() if adds.num_rows else []
        except Exception as exc:  # an empty / half-written table
            out[path] = {"bytes": 0, "files": 0, "error": str(exc)[:200]}
            continue
        kind, pid, pair = _classify(log, path)
        out[path] = {"bytes": int(sum(sizes)), "files": len(sizes), "kind": kind, "pid": pid, "pair": pair}
    return out


def summarise(sizes: dict[str, dict]) -> dict[str, int]:
    agg = collections.Counter()
    n_pairs = collections.Counter()
    for v in sizes.values():
        agg[v.get("kind", "other")] += v["bytes"]
        if v.get("kind") == "adaptive_pairs":
            n_pairs[v["pid"]] += 1
    out = dict(agg)
    out["total"] = sum(agg.values())
    out["adaptive_pair_tables"] = dict(n_pairs)
    return out


def pair_row_counts(log: str) -> dict[tuple[str, str], int]:
    """Rows per (source, target) of the eager pairs index of ``log``."""
    from deltalake import DeltaTable

    dt = DeltaTable(f"s3://{BUCKET}/{log}/pairs_index", storage_options=S3_OPTIONS)
    tb = dt.to_pyarrow_table(columns=["source", "target"])
    counts = collections.Counter(zip(tb.column("source").to_pylist(), tb.column("target").to_pylist()))
    return dict(counts)
