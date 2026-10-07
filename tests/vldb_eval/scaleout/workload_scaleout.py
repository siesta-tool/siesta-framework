"""
tests/vldb_eval/scaleout/workload_scaleout.py

Fixed query workload for Exp 2, computed once from the r1 log and reused for
every (size, cores) cell, so all cells answer exactly the same queries.

Per perspective (case + the requested attribute perspectives) it picks N
activity pairs (distinct source activities, highest group coverage first) and,
for each pair, one attribute predicate on the source event:

    structural:  A B
    attribute:   A[attr="v"] B

so the two query kinds differ only by the predicate.  A predicate is kept only
if, under that perspective's grouping, it matches at least one group AND
strictly fewer groups than the bare pair: it is non-empty and actually filters
(no always-false or always-true predicates).  Among valid predicates the one
closest to matching half of the pair's groups is chosen.

Predicate attributes are the low-cardinality string attributes of the log
(2..20 distinct values, e.g. Action, lifecycle:transition, EventOrigin), minus
the perspective's own grouping attribute.

A group matches when some (qualifying) A event is earlier than the last B event
of that group -- the same rule as workload.pair_coverage.  Replicas copy traces
with their timestamps, so a query non-empty on r1 stays non-empty on r2 / r3.

Output: results/exp_scaleout/workload.json (expected r1 group counts included,
to cross-check against the server's matched_groups).

Usage:
    python -m tests.vldb_eval.scaleout.workload_scaleout \
        --perspectives Action,org:resource --n-pairs 4
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from tests.vldb_eval.eval_common import RESULTS_DIR, quote_label

BASE_CSV = Path("/srv/datasets/scaleout/bpic2017_r1.csv")
WORKLOAD_JSON = RESULTS_DIR / "exp_scaleout" / "workload.json"
CASE = "case"

MAX_PRED_CARD = 20      # predicate attributes: 2..20 distinct values
TOP_PAIRS = 120         # candidate pairs examined per perspective
_CORE = {"trace_id", "activity", "timestamp", "ts"}
# SeQL lexer constraints (see pattern_common): keys are LABEL tokens, values
# are "..." strings without escapes.
_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_:]*$")
_UNSAFE_VALUE = re.compile(r'["\\\[\],=$]')


def group_column(perspective: str) -> str:
    return "trace_id" if perspective == CASE else perspective


def load_events(csv: Path) -> pd.DataFrame:
    df = pd.read_csv(csv, dtype=str, keep_default_na=False)
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601").astype("int64")
    return df


def predicate_attributes(df: pd.DataFrame) -> list[str]:
    out = []
    for c in df.columns:
        if c in _CORE or not _KEY_RE.match(c):
            continue
        n = df.loc[df[c] != "", c].nunique()
        if 2 <= n <= MAX_PRED_CARD:
            out.append(c)
    return out


def _pair_coverage(first: pd.DataFrame, last: pd.DataFrame) -> pd.Series:
    """(a, b) -> number of groups where first(a) < last(b), descending."""
    m = first.merge(last, on="g")
    m = m[(m["a"] != m["b"]) & (m["first"] < m["last"])]
    return m.groupby(["a", "b"]).size().sort_values(ascending=False, kind="stable")


def build_perspective(df: pd.DataFrame, perspective: str, pred_attrs: list[str],
                      n_pairs: int) -> dict:
    gcol = group_column(perspective)
    ev = df[df[gcol] != ""]
    ev = ev.assign(g=ev[gcol])

    agg = ev.groupby(["g", "activity"])["ts"]
    first = agg.min().rename("first").reset_index().rename(columns={"activity": "a"})
    last = agg.max().rename("last").reset_index().rename(columns={"activity": "b"})
    cov = _pair_coverage(first, last)

    by_act = {a: sub for a, sub in ev.groupby("activity")}
    attrs = [a for a in pred_attrs if a != gcol]
    chosen: list[dict] = []
    used_src: set[str] = set()

    for (a, b), n_struct in cov.head(TOP_PAIRS).items():
        if len(chosen) >= n_pairs:
            break
        if a in used_src or n_struct < 2:
            continue
        last_b = last[last["b"] == b][["g", "last"]]
        ev_a = by_act[a]
        best = None
        for attr in attrs:
            sub = ev_a[ev_a[attr] != ""]
            if sub.empty:
                continue
            fa = (sub.groupby(["g", attr])["ts"].min().rename("first")
                  .reset_index().merge(last_b, on="g"))
            hits = fa[fa["first"] < fa["last"]].groupby(attr)["g"].nunique()
            for v, n_pred in hits.items():
                if _UNSAFE_VALUE.search(v) or not 0 < n_pred < n_struct:
                    continue
                score = abs(n_pred / n_struct - 0.5)
                if best is None or score < best[0]:
                    best = (score, attr, v, int(n_pred))
        if best is None:
            continue
        _, attr, v, n_pred = best
        chosen.append({
            "source": a, "target": b,
            "structural": f"{quote_label(a)} {quote_label(b)}",
            "attribute": f'{quote_label(a)}[{attr}="{v}"] {quote_label(b)}',
            "pred_attr": attr, "pred_value": v,
            "groups_struct": int(n_struct), "groups_attr": n_pred,
        })
        used_src.add(a)

    return {"group_column": gcol, "group_count": int(ev["g"].nunique()),
            "queries": chosen}


def build(csv: Path, perspectives: list[str], n_pairs: int) -> dict:
    t0 = time.time()
    df = load_events(csv)
    pred_attrs = predicate_attributes(df)
    out = {"source": str(csv), "n_pairs": n_pairs, "predicate_attributes": pred_attrs,
           "perspectives": {}}
    for p in [CASE] + [p for p in perspectives if p != CASE]:
        out["perspectives"][p] = build_perspective(df, p, pred_attrs, n_pairs)
        got = len(out["perspectives"][p]["queries"])
        if got < n_pairs:
            print(f"  warning: {p}: only {got}/{n_pairs} pairs have a valid predicate")
    print(f"workload built in {time.time() - t0:.1f}s")
    return out


def save(workload: dict, path: Path = WORKLOAD_JSON) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(workload, indent=2))
    return path


def load(path: Path = WORKLOAD_JSON) -> dict:
    return json.loads(path.read_text())


def ensure(perspectives: list[str], n_pairs: int, *, csv: Path = BASE_CSV,
           path: Path = WORKLOAD_JSON) -> dict:
    """Load the saved workload if it matches; otherwise (re)build and save it."""
    want = [CASE] + [p for p in perspectives if p != CASE]
    if path.exists():
        w = load(path)
        if list(w["perspectives"]) == want and w.get("n_pairs") == n_pairs:
            return w
    w = build(csv, perspectives, n_pairs)
    save(w, path)
    return w


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", type=Path, default=BASE_CSV)
    ap.add_argument("--perspectives", default="Action,org:resource")
    ap.add_argument("--n-pairs", type=int, default=4)
    ap.add_argument("--out", type=Path, default=WORKLOAD_JSON)
    args = ap.parse_args()

    persps = [p.strip() for p in args.perspectives.split(",") if p.strip()]
    w = build(args.csv, persps, args.n_pairs)
    print("wrote", save(w, args.out))
    for p, spec in w["perspectives"].items():
        print(f"\n[{p}] groups={spec['group_count']}")
        for q in spec["queries"]:
            print(f"  {q['groups_struct']:>6} -> {q['groups_attr']:>6}  {q['attribute']}")


if __name__ == "__main__":
    main()
