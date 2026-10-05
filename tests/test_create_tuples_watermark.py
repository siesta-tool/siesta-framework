"""
createTuples across batches: an incremental run guided by the LastChecked
watermark must produce the pairs a full run produces.  (No Spark needed.)
"""

from siesta.modules.index.computations import createTuples

LOOKBACK = (10**9, "time")


def _events(ts_list):
    return [(ts, pos, {}) for pos, ts in enumerate(ts_list)]


def _incremental(key1, key2, src, tgt, split_ts):
    """Batch 1 = events with ts < split_ts; batch 2 re-reads the whole group."""
    b1_src = [e for e in src if e[0] < split_ts]
    b1_tgt = [e for e in tgt if e[0] < split_ts]
    first = createTuples(key1, key2, b1_src, b1_tgt, LOOKBACK, None, "g")
    watermark = first[-1][4] if first else None
    second = createTuples(key1, key2, src, tgt, LOOKBACK, watermark, "g")
    return first + second


def test_self_pair_chain_survives_batch_boundary():
    a = _events([1, 3, 5, 7, 9])
    full = createTuples("A", "A", a, a, LOOKBACK, None, "g")
    inc = _incremental("A", "A", a, a, split_ts=6)
    assert [(p[3], p[4]) for p in full] == [(1, 3), (3, 5), (5, 7), (7, 9)]
    assert sorted(inc) == sorted(full)


def test_cross_pair_unchanged_across_batch_boundary():
    events = [("A", 1), ("B", 2), ("A", 3), ("A", 5), ("B", 6), ("A", 7), ("B", 8)]
    src = [(ts, pos, {}) for pos, (act, ts) in enumerate(events) if act == "A"]
    tgt = [(ts, pos, {}) for pos, (act, ts) in enumerate(events) if act == "B"]
    full = createTuples("A", "B", src, tgt, LOOKBACK, None, "g")
    inc = _incremental("A", "B", src, tgt, split_ts=4)
    assert sorted(inc) == sorted(full)
