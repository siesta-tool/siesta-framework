"""
tests/vldb_eval/pattern_common.py — shared ground for the pattern-detection
experiments (Section 2, Exp 4: latency vs pattern length; also used by Exp 1).

Every system (SIESTA, ELK, MR, LPG) answers the *same* queries over the
*same* events:

* Canonical events.  After SIESTA ingests a log, its SequenceTable is
  exported once to ``results/data/<ds>/canonical.parquet``: trace_id,
  position (intra-trace), activity, ts (Unix seconds, as SIESTA stores it)
  and one string column per event attribute (SIESTA's attribute strings).
  Competitors load this file, so traces, positions, timestamp ties and
  attribute values are identical across systems.

* Groups and their order.  Under the case perspective a group is a trace,
  ordered by position.  Under an attribute perspective ``k`` a group is the
  set of events with a non-null ``k``, ordered by (ts, trace_id, position);
  this is the order SIESTA assigns to group positions.

* Pattern semantics.  A query is a sequence of events e1..eL, each with an
  activity and optional literal predicates ``attr = 'v'`` (values sampled
  from the log).  (The matcher also supports bindings ``attr = $j`` /
  ``attr != $j``, but the generator no longer emits them.)
  A group matches when events of the group occur in that order (other
  events may lie between them) and satisfy their predicates.  The result
  of a query is the number of matching groups.

* Ground truth.  ``match_count`` evaluates a query directly on the
  canonical events (backtracking search, memoised on the binding state);
  every system's count is checked against it.

Query JSONL record (``results/exp4_patterns/queries/<ds>.<perspective>.jsonl``)::

    {"qid": "bpic2017.case.L08.attr.03", "dataset": "bpic2017",
     "perspective": "case", "grouping_keys": ["trace_id"], "group_attr": null,
     "length": 8, "kind": "structural" | "attribute",
     "events": [{"activity": "A_Create Application", "preds": []},
                {"activity": "W_Complete application",
                 "preds": [{"attr": "org:resource", "op": "=", "value": "User_1"},
                           {"attr": "Action", "op": "!=", "ref": 1}]}, ...],
     "siesta_pattern": "...", "truth": {"matched_groups": 123, "group_count": 31509},
     "witness_group": "Application_..."}

``ref`` is the 1-based index of an earlier pattern event.

Result record (one per system x query x run), written by each adapter::

    {"system": "elk", "variant": "...", "qid": ..., "time_s": float,
     "matched_groups": int | None, "timed_out": bool, "error": str | None,
     "phases": {...}}

Usage:
    python -m tests.vldb_eval.pattern_common export
    python -m tests.vldb_eval.pattern_common generate
"""

from __future__ import annotations

import argparse
import bisect
import collections
import json
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from tests.vldb_eval.eval_common import RESULTS_DIR, quote_label

DATA_DIR = RESULTS_DIR / "data"
QUERY_DIR = RESULTS_DIR / "exp4_patterns" / "queries"
LENGTHS = list(range(8, 16))
PER_LENGTH = 10            # queries per (length, kind)
TIMEOUT_S = 300.0          # per-query timeout, applied uniformly to every dataset/system
ORACLE_LIMIT_S = 120.0     # ground-truth budget per candidate query (generation only)
SEED = 42

# Non-case perspectives of the complementary experiment (BPIC 2017 only).
# EventOrigin is left out: every activity has exactly one origin, so a
# pattern can only ever match one of its 3 groups.
PERSPECTIVES = {
    "bpic2011": [],
    "bpic2012": [],
    "bpic2015": [],
    "bpic2017": ["org:resource", "Action", "lifecycle:transition"],
    "bpic2018": [],
}
DATASETS = ["bpic2011", "bpic2012", "bpic2015", "bpic2017", "bpic2018"]  # case-centric part

S3_OPTIONS = {
    "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "AWS_ENDPOINT_URL": "http://localhost:9000", "AWS_ALLOW_HTTP": "true",
    "AWS_REGION": "us-east-1",
}

# SeQL lexer: attribute keys must be LABEL tokens, values are "..." strings
# without escapes.
_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_:\\]*$")
_UNSAFE_VALUE = re.compile(r'["\\\[\],=$]')


def canonical_path(ds: str) -> Path:
    return DATA_DIR / ds / "canonical.parquet"


def query_path(ds: str, perspective: str) -> Path:
    return QUERY_DIR / f"{ds}.{safe(perspective)}.jsonl"


def safe(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s)


# ---------------------------------------------------------------------------
# Canonical events
# ---------------------------------------------------------------------------

def export_canonical(ds: str, log_name: str | None = None) -> Path:
    """Export SIESTA's SequenceTable of ``log_name`` (default ``ds``) to Parquet."""
    from deltalake import DeltaTable

    dt = DeltaTable(f"s3://siesta/{log_name or ds}/sequence_table", storage_options=S3_OPTIONS)
    tb = dt.to_pyarrow_table(columns=["trace_id", "position", "activity", "start_timestamp", "attributes"])
    base = pd.DataFrame({
        "trace_id": tb.column("trace_id").to_pylist(),
        "position": np.asarray(tb.column("position").to_pylist(), dtype=np.int64),
        "activity": tb.column("activity").to_pylist(),
        "ts": np.asarray(tb.column("start_timestamp").to_pylist(), dtype=np.int64),
    })
    attrs = pd.DataFrame([dict(m) if m else {} for m in tb.column("attributes").to_pylist()])
    df = pd.concat([base, attrs], axis=1)
    df = df.sort_values(["trace_id", "position"], kind="stable").reset_index(drop=True)
    out = canonical_path(ds)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    meta = {"dataset": ds, "log_name": log_name or ds, "events": len(df),
            "traces": int(df.trace_id.nunique()), "activities": int(df.activity.nunique()),
            "attributes": [c for c in df.columns if c not in ("trace_id", "position", "activity", "ts")]}
    out.with_suffix(".json").write_text(json.dumps(meta, indent=1))
    return out


def load_events(ds: str) -> pd.DataFrame:
    """Canonical events, sorted by (trace_id, position); attribute values are str or None."""
    df = pd.read_parquet(canonical_path(ds))
    return df


_BASE_COLUMNS = ("trace_id", "position", "activity", "ts", "group", "gpos")  # group / gpos: added by group_order


def attribute_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in _BASE_COLUMNS]


def grouping_keys(perspective: str) -> list[str]:
    return ["trace_id"] if perspective == "case" else [perspective]


def group_order(df: pd.DataFrame, perspective: str) -> pd.DataFrame:
    """
    Events of the perspective's groups in group order, with columns
    ``group`` and ``gpos`` (0-based position within the group) added.
    """
    if perspective == "case":
        g = df.copy()
        g["group"] = g["trace_id"]
        g = g.sort_values(["trace_id", "position"], kind="stable")
    else:
        g = df[df[perspective].notna()].copy()
        g["group"] = g[perspective]
        g = g.sort_values(["group", "ts", "trace_id", "position"], kind="stable")
    g["gpos"] = g.groupby("group", sort=False).cumcount()
    return g.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

@dataclass
class Pred:
    attr: str
    op: str                 # "=" | "!="
    value: str | None = None
    ref: int | None = None  # 1-based index of an earlier pattern event

    def to_dict(self) -> dict:
        d = {"attr": self.attr, "op": self.op}
        if self.ref is not None:
            d["ref"] = self.ref
        else:
            d["value"] = self.value
        return d


def siesta_pattern(events: list[dict]) -> str:
    """The query in SIESTA's SeQL syntax."""
    parts = []
    for e in events:
        tok = quote_label(e["activity"])
        if e["preds"]:
            cons = []
            for p in e["preds"]:
                rhs = f"${p['ref']}" if "ref" in p else f'"{p["value"]}"'
                cons.append(f"{p['attr']}{p['op']}{rhs}")
            tok += "[" + ",".join(cons) + "]"
        parts.append(tok)
    return " ".join(parts)


def categorical_attrs(g: pd.DataFrame, exclude: set[str]) -> list[str]:
    """Attributes usable in predicates: 2..1000 distinct values, mostly present, not ID-like."""
    out = []
    n = len(g)
    for c in attribute_columns(g):
        if c in exclude or not _KEY_RE.match(c):
            continue
        s = g[c]
        present = s.notna().mean()
        nunique = s.nunique(dropna=True)
        if present < 0.3 or not (2 <= nunique <= 1000) or nunique > 0.5 * n:
            continue
        out.append(c)
    return out


def _usable(v) -> bool:
    return isinstance(v, str) and v != "" and not _UNSAFE_VALUE.search(v)


def _pick_window(rng: random.Random, seq: pd.DataFrame, length: int) -> list[int] | None:
    """
    Row indices (in group order) of a contiguous window of ``length``
    events: real process fragments, but with repeats curbed so validation
    stays tractable.  A window must have at least ceil(length / 4) distinct
    activities, and no single activity may occur more than 4 times in it
    (tuned empirically: tighter caps made length 14-15 windows infeasible
    on BPIC 2012/2017, whose logs have few distinct activities).  Repeated
    activities (e.g. a lab test re-ordered dozens of times per trace) make
    CEP's backtracking search explode combinatorially — every extra
    occurrence of a pattern activity multiplies the candidate positions to
    try in every group, not just the witness.  The group is a witness.
    """
    n = len(seq)
    if n < length:
        return None
    acts = seq["activity"].to_numpy()
    need = -(-length // 4)
    max_repeat = 4
    for _ in range(60):
        st = rng.randrange(n - length + 1)
        window = acts[st:st + length]
        counts = collections.Counter(window)
        if len(counts) >= need and max(counts.values()) <= max_repeat:
            return list(range(st, st + length))
    return None


def _add_predicates(rng: random.Random, rows: pd.DataFrame, cat: list[str]) -> list[list[Pred]]:
    """
    1–3 literal predicates ``attr = value`` taken from the witness events
    (values sampled from the log, never null).  No variable bindings
    (``$j``): their meaning on missing attributes differs between systems
    (SQL: a null never matches; SIESTA: missing values compare as values).
    Satisfied by the witness by construction.
    """
    L = len(rows)
    preds: list[list[Pred]] = [[] for _ in range(L)]
    want = rng.randint(1, 3)
    tries = 0
    while sum(map(len, preds)) < want and tries < 200:
        tries += 1
        i = rng.randrange(L)
        attr = rng.choice(cat)
        vi = rows.iloc[i][attr]
        if not _usable(vi) or any(p.attr == attr for p in preds[i]):
            continue
        preds[i].append(Pred(attr, "=", value=vi))
    return preds


def generate(ds: str, perspective: str, seed: int = SEED,
             lengths: list[int] = LENGTHS, per_length: int = PER_LENGTH) -> list[dict]:
    """
    ``per_length`` structural and ``per_length`` attribute-aware queries per
    length.  Witness groups are drawn uniformly among the groups that can
    supply the length, so queries are spread across groups.  Each witness
    window yields one structural query and one attribute-aware query (the
    same window plus predicates taken from its events).
    """
    rng = random.Random(f"{seed}:{ds}:{perspective}")
    df = load_events(ds)
    g = group_order(df, perspective)
    exclude = {perspective} if perspective != "case" else set()
    cat = categorical_attrs(g, exclude)
    groups = {k: v for k, v in g.groupby("group", sort=False)}
    sizes = {k: len(v) for k, v in groups.items()}
    truth_ctx = TruthIndex(g)
    group_count = len(groups)
    out = []
    for L in lengths:
        cands = sorted(k for k, n in sizes.items() if n >= L)
        if not cands:
            print(f"  [{ds}/{perspective}] no group with {L} events", file=sys.stderr)
            continue
        made = {"structural": 0, "attribute": 0}
        dropped = {"structural": 0, "attribute": 0}
        attempts = 0
        seen = set()
        while min(made.values()) < per_length and attempts < 50 * per_length:
            attempts += 1
            gid = rng.choice(cands)
            seq = groups[gid].reset_index(drop=True)
            win = _pick_window(rng, seq, L)
            if win is None:
                continue
            rows = seq.iloc[win].reset_index(drop=True)
            acts = rows["activity"].tolist()
            for kind in ("structural", "attribute"):
                if made[kind] >= per_length:
                    continue
                if kind == "structural":
                    preds = [[] for _ in acts]
                else:
                    if not cat:
                        continue
                    preds = _add_predicates(rng, rows, cat)
                    if not any(preds):
                        continue
                events = [{"activity": a, "preds": [p.to_dict() for p in ps]} for a, ps in zip(acts, preds)]
                key = json.dumps(events, sort_keys=True)
                if key in seen:
                    continue
                seen.add(key)
                q = {
                    "qid": f"{ds}.{safe(perspective)}.L{L:02d}.{kind[:6]}.{made[kind]:02d}",
                    "dataset": ds, "perspective": perspective,
                    "grouping_keys": grouping_keys(perspective),
                    "group_attr": None if perspective == "case" else perspective,
                    "length": L, "kind": kind, "events": events,
                    "siesta_pattern": siesta_pattern(events),
                    "witness_group": str(gid),
                }
                t0 = time.perf_counter()
                try:
                    n = truth_ctx.match_count(events, deadline=time.monotonic() + ORACLE_LIMIT_S)
                except OracleTimeout:
                    # Too many partial embeddings to decide exactly: the
                    # candidate is dropped (counted) and another one drawn.
                    dropped[kind] += 1
                    continue
                q["truth"] = {"matched_groups": n, "group_count": group_count,
                              "oracle_s": round(time.perf_counter() - t0, 3)}
                assert n >= 1, q  # the witness group always matches
                out.append(q)
                made[kind] += 1
        print(f"  [{ds}/{perspective}] L={L}: {made}" + (f" dropped (oracle > {ORACLE_LIMIT_S:g}s): {dropped}" if any(dropped.values()) else ""))
    return out


# ---------------------------------------------------------------------------
# Ground truth
# ---------------------------------------------------------------------------

class OracleTimeout(Exception):
    """The ground truth of a candidate query could not be decided in time."""


class TruthIndex:
    """
    Per-group event lists by activity over group-ordered events, for
    evaluating a query on every group.
    """

    def __init__(self, g: pd.DataFrame):
        self.attr_cols = attribute_columns(g)
        self.by_act: dict[str, dict[str, tuple[np.ndarray, np.ndarray]]] = {}
        acts = g["activity"].to_numpy()
        grp = g["group"].astype(str).to_numpy()
        gpos = g["gpos"].to_numpy()
        self._rows = g
        self._cols = {c: g[c].to_numpy(dtype=object) for c in self.attr_cols}
        order = np.lexsort((gpos, grp, acts))
        acts_s, grp_s = acts[order], grp[order]
        # boundaries of (activity, group) runs
        cut = np.flatnonzero((acts_s[1:] != acts_s[:-1]) | (grp_s[1:] != grp_s[:-1])) + 1
        starts = np.concatenate([[0], cut])
        ends = np.concatenate([cut, [len(order)]])
        for s, e in zip(starts, ends):
            a, k = acts_s[s], grp_s[s]
            rows = order[s:e]
            self.by_act.setdefault(a, {})[k] = (gpos[rows], rows)

    def match_count(self, events: list[dict], deadline: float | None = None) -> int:
        """Matching groups; raises OracleTimeout past ``deadline`` (time.monotonic())."""
        self._deadline = deadline
        self._steps = 0
        acts = [e["activity"] for e in events]
        per_act = [self.by_act.get(a, {}) for a in acts]
        # candidate groups contain every activity of the pattern
        groups = set(per_act[0])
        for d in per_act[1:]:
            groups &= set(d)
        return sum(1 for k in groups if self._match_group(k, events, per_act))

    def _match_group(self, k: str, events: list[dict], per_act) -> bool:
        L = len(events)
        cand = []
        for i, e in enumerate(events):
            pos, rows = per_act[i][k]
            keep = np.ones(len(pos), dtype=bool)
            for p in e["preds"]:
                if "ref" not in p:
                    col = self._cols[p["attr"]][rows] if p["attr"] in self._cols else np.full(len(rows), None)
                    keep &= np.array([v == p["value"] for v in col], dtype=bool)
            if not keep.any():
                return False
            cand.append((pos[keep], rows[keep]))
        # attributes referenced later, per pattern index (binding state)
        needed = [set() for _ in range(L)]
        bind_preds = [[p for p in e["preds"] if "ref" in p] for e in events]
        for i, bp in enumerate(bind_preds):
            for p in bp:
                needed[p["ref"] - 1].add(p["attr"])
        live = [set() for _ in range(L + 1)]  # (ref, attr) still needed after step i
        for i in range(L - 1, -1, -1):
            live[i] = set(live[i + 1])
            for p in bind_preds[i]:
                live[i].add((p["ref"] - 1, p["attr"]))
        cols = self._cols
        failed = set()

        def val(row, attr):
            v = cols[attr][row] if attr in cols else None
            return v if isinstance(v, str) else None

        def dfs(i: int, after: int, bound: dict) -> bool:
            if i == L:
                return True
            self._steps += 1
            if self._deadline is not None and self._steps % 4096 == 0 and time.monotonic() > self._deadline:
                raise OracleTimeout()
            state = (i, after, tuple(sorted((key, bound.get(key)) for key in live[i] if key[0] < i)))
            if state in failed:
                return False
            pos, rows = cand[i]
            start = bisect.bisect_right(pos, after)
            for t in range(start, len(pos)):
                r = rows[t]
                ok = True
                for p in bind_preds[i]:
                    ref_v = bound.get((p["ref"] - 1, p["attr"]))
                    v = val(r, p["attr"])
                    if ref_v is None or v is None:
                        ok = False  # a null never satisfies a binding
                        break
                    if (p["op"] == "=") != (v == ref_v):
                        ok = False
                        break
                if not ok:
                    continue
                nb = bound
                if needed[i]:
                    nb = dict(bound)
                    for attr in needed[i]:
                        nb[(i, attr)] = val(r, attr)
                if dfs(i + 1, int(pos[t]), nb):
                    return True
                if not needed[i]:
                    break  # no later event refers to this one: the earliest candidate is optimal
            failed.add(state)
            return False

        return dfs(0, -1, {})


# ---------------------------------------------------------------------------
# Timeout cut-off
# ---------------------------------------------------------------------------

SKIP_AFTER = 3  # consecutive timeouts of one kind before the rest of that kind is skipped


class SkipAfter:
    """
    Per (dataset, perspective, kind): once ``n`` consecutive queries of a kind
    time out (queries run by increasing length), the remaining queries of that
    kind are recorded as timed out without being run (``skipped: true``).
    The same rule for every system; ``n = 0`` disables it.
    """

    def __init__(self, n: int = SKIP_AFTER):
        self.n = n
        self.streak: dict[str, int] = {}

    def skip(self, q: dict) -> bool:
        return self.n > 0 and self.streak.get(q["kind"], 0) >= self.n

    def record(self, q: dict, timed_out: bool) -> None:
        self.streak[q["kind"]] = self.streak.get(q["kind"], 0) + 1 if timed_out else 0


def skipped_record(system: str, q: dict, **extra) -> dict:
    return {"system": system, "qid": q["qid"], "dataset": q["dataset"], "perspective": q["perspective"],
            "length": q["length"], "kind": q["kind"], "rep": 0, "time_s": None, "matched_groups": None,
            "truth": q["truth"], "parity": None, "timed_out": True, "skipped": True, "error": None, **extra}


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def save_queries(qs: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for q in qs:
            f.write(json.dumps(q) + "\n")


def load_queries(ds: str, perspective: str = "case") -> list[dict]:
    with query_path(ds, perspective).open() as f:
        return [json.loads(l) for l in f if l.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["export", "generate"])
    ap.add_argument("--datasets", nargs="+", default=DATASETS)
    ap.add_argument("--perspectives", nargs="*", default=None,
                    help="default: case plus PERSPECTIVES[ds]")
    args = ap.parse_args()
    for ds in args.datasets:
        if args.cmd == "export":
            p = export_canonical(ds)
            print(ds, "->", p, json.loads(p.with_suffix(".json").read_text())["events"], "events")
        else:
            persps = args.perspectives if args.perspectives is not None else ["case"] + PERSPECTIVES.get(ds, [])
            for persp in persps:
                qs = generate(ds, persp)
                save_queries(qs, query_path(ds, persp))
                print(f"{ds}/{persp}: {len(qs)} queries -> {query_path(ds, persp)}")


if __name__ == "__main__":
    main()
