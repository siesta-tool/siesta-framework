"""
tests/vldb_eval/suite_data.py

Dataset preparation for the indexing experiments (Exp 1-5).

For every dataset this module produces, once, under
``tests/vldb_eval/results/data/<name>/``:

    full.csv                  the whole log as a flat CSV (trace_id, activity,
                              timestamp, <event attributes>)
    perspectives.json         the perspective set P = case + top-3 attributes
                              and per-attribute statistics
    batches/batch_<k>.csv     B0 (bootstrap) + B1..B5 (measured): a stratified
                              trace sample, every trace wholly in one batch
    batches/<persp>/batch_<k>.csv
                              the same batches restricted to events that carry
                              a value for <persp>, for the eager baseline, which
                              indexes a perspective by using it as trace_id

Perspective selection (the paper's filter)
------------------------------------------
Attribute keys that are non-numeric, have cardinality in [3, 500] and more
than 1.2 distinct values per trace on average, ranked by cardinality
(ascending, ties by name).  The top 3 plus the case perspective form P.

One filter is added to the paper's: the perspective must induce groups
that mix activities (median distinct activities per group >= 2).  An
attribute that is a relabelling of the activity (BPIC 2015's
activityNameEN / activityNameNL / action_code) yields single-activity
groups, which hold self-pairs only and no cross-activity pair to index.

Usage
-----
    python -m tests.vldb_eval.suite_data --datasets bpic2011 bpic2017 synthetic
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from tests.vldb_eval.batch_splitter import _iter_log
from tests.vldb_eval.eval_common import REPO_ROOT, discover_schema

RAW_DIR = Path("/mnt/datasets")
DATA_DIR = Path(__file__).resolve().parent / "results" / "data"

BPIC = ["bpic2011", "bpic2012", "bpic2015", "bpic2017", "bpic2018"]
ALL_DATASETS = BPIC + ["synthetic"]

N_BATCHES = 6          # B0 bootstrap + B1..B5 measured
N_ATTR_PERSPECTIVES = 3
MIN_MEDIAN_ACTIVITIES_PER_GROUP = 2
SPLIT_SEED = 42
SYNTHETIC_TRACES = 2000
SYNTHETIC_SEED = 42

CASE = "case"          # label of the case perspective
CASE_KEYS = ["trace_id"]


@dataclass
class Prepared:
    name: str
    root: Path
    full_csv: Path
    batches: list[Path]
    perspectives: dict          # contents of perspectives.json

    @property
    def log_name(self) -> str:
        return self.name

    def perspective_list(self) -> list[dict]:
        """[{label, grouping_keys, attribute}] with case first."""
        return self.perspectives["perspectives"]

    def eager_full(self, label: str) -> Path:
        """
        The whole log for the eager index of one perspective: the events that
        carry a value for it (created on first use).
        """
        attr = next(p["attribute"] for p in self.perspective_list() if p["label"] == label)
        if attr is None:
            return self.full_csv
        out = self.root / f"full__{safe_name(label)}.csv"
        if not out.exists():
            tmp = out.with_suffix(".tmp")
            with self.full_csv.open(newline="", encoding="utf-8") as f, \
                 tmp.open("w", newline="", encoding="utf-8") as g:
                r = csv.DictReader(f)
                w = csv.DictWriter(g, fieldnames=r.fieldnames)
                w.writeheader()
                w.writerows(row for row in r if row.get(attr))
            tmp.rename(out)
        return out

    def eager_batches(self, label: str) -> list[Path]:
        if label == CASE:
            return self.batches
        d = self.root / "batches" / safe_name(label)
        return [d / p.name for p in self.batches]


def safe_name(s: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in s)


# ---------------------------------------------------------------------------
# Full CSV
# ---------------------------------------------------------------------------

def _write_full_csv(name: str, out: Path) -> None:
    if name == "synthetic":
        from tests.vldb_eval.generate_eval_dataset import generate_events, write_csv
        write_csv(generate_events(SYNTHETIC_TRACES, SYNTHETIC_SEED), out)
        return

    src = RAW_DIR / f"{name}.xes"
    if not src.exists():
        raise FileNotFoundError(src)
    print(f"  converting {src} -> {out}")
    # Two passes: the column set first (XES events are heterogeneous), then
    # the rows.  Streaming both keeps memory flat for BPIC 2018.
    keys: set[str] = set()
    n = 0
    for ev in _iter_log(src):
        keys.update(ev)
        n += 1
    cols = ["trace_id", "activity", "timestamp"] + sorted(keys - {"trace_id", "activity", "timestamp"})
    tmp = out.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for ev in _iter_log(src):
            w.writerow(ev)
    tmp.rename(out)
    print(f"  {n} events, {len(cols)} columns")


# ---------------------------------------------------------------------------
# Perspectives
# ---------------------------------------------------------------------------

def _discover_perspectives(full_csv: Path) -> dict:
    schema = discover_schema(full_csv, max_events=0)
    # Per-key cardinality and coverage for the report.
    card: dict[str, set] = defaultdict(set)
    present: dict[str, int] = defaultdict(int)
    acts_per_value: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    n = 0
    with full_csv.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            n += 1
            for k in schema.perspective_keys:
                v = row.get(k)
                if v:
                    card[k].add(v)
                    present[k] += 1
                    acts_per_value[k][v].add(row["activity"])

    def median_acts(k: str) -> float:
        sizes = sorted(len(a) for a in acts_per_value[k].values())
        return float(sizes[len(sizes) // 2]) if sizes else 0.0

    ranked = sorted(schema.perspective_keys, key=lambda k: (len(card[k]), k))
    eligible = [k for k in ranked if median_acts(k) >= MIN_MEDIAN_ACTIVITIES_PER_GROUP]
    chosen = eligible[:N_ATTR_PERSPECTIVES]
    perspectives = [{"label": CASE, "grouping_keys": CASE_KEYS, "attribute": None}]
    perspectives += [{"label": k, "grouping_keys": [k], "attribute": k} for k in chosen]
    return {
        "n_events": n,
        "n_activities": len(schema.activities),
        "activities": schema.activities,
        "candidates": [
            {
                "key": k,
                "cardinality": len(card[k]),
                "event_coverage": present[k] / n if n else 0.0,
                "median_activities_per_group": median_acts(k),
                "eligible": k in eligible,
                "chosen": k in chosen,
            }
            for k in ranked
        ],
        "perspectives": perspectives,
    }


# ---------------------------------------------------------------------------
# Batches
# ---------------------------------------------------------------------------

def _split(full_csv: Path, out_dir: Path, strat_key: str | None) -> list[Path]:
    """
    Stratified trace sample into N_BATCHES: traces are grouped by the
    stratification value of their first event, shuffled within each
    stratum and dealt round-robin (continuing across strata), so every
    batch holds ~1/N of every stratum and of the log.  Two streaming
    passes (assignment, then rows), so memory stays flat.
    """
    strata: dict[str, list[str]] = defaultdict(list)
    seen: set[str] = set()
    with full_csv.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames
        for row in reader:
            tid = row["trace_id"]
            if tid not in seen:
                seen.add(tid)
                strata[(row.get(strat_key) or "") if strat_key else ""].append(tid)

    rng = random.Random(SPLIT_SEED)
    assign: dict[str, int] = {}
    k = 0
    for s in sorted(strata):
        tids = sorted(strata[s])
        rng.shuffle(tids)
        for tid in tids:
            assign[tid] = k % N_BATCHES
            k += 1

    out_dir.mkdir(parents=True, exist_ok=True)
    paths = [out_dir / f"batch_{i}.csv" for i in range(N_BATCHES)]
    files = [p.open("w", newline="", encoding="utf-8") for p in paths]
    writers = [csv.DictWriter(fh, fieldnames=cols) for fh in files]
    for w in writers:
        w.writeheader()
    with full_csv.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            writers[assign[row["trace_id"]]].writerow(row)
    for fh in files:
        fh.close()
    return paths


def _filter_batches(batches: list[Path], key: str, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for p in batches:
        with p.open(newline="", encoding="utf-8") as f, \
             (out_dir / p.name).open("w", newline="", encoding="utf-8") as g:
            r = csv.DictReader(f)
            w = csv.DictWriter(g, fieldnames=r.fieldnames)
            w.writeheader()
            w.writerows(row for row in r if row.get(key))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def prepare(name: str, force: bool = False) -> Prepared:
    root = DATA_DIR / name
    root.mkdir(parents=True, exist_ok=True)
    full_csv = root / "full.csv"
    if force or not full_csv.exists():
        print(f"[{name}] writing full.csv")
        _write_full_csv(name, full_csv)

    persp_path = root / "perspectives.json"
    if force or not persp_path.exists():
        print(f"[{name}] discovering perspectives")
        persp_path.write_text(json.dumps(_discover_perspectives(full_csv), indent=2))
    persp = json.loads(persp_path.read_text())

    batch_dir = root / "batches"
    batches = [batch_dir / f"batch_{i}.csv" for i in range(N_BATCHES)]
    if force or not all(p.exists() for p in batches):
        print(f"[{name}] splitting into {N_BATCHES} batches")
        attrs = [p["attribute"] for p in persp["perspectives"] if p["attribute"]]
        # Stratify on the highest-cardinality chosen perspective.
        _split(full_csv, batch_dir, attrs[-1] if attrs else None)
    for p in persp["perspectives"]:
        if p["attribute"]:
            d = batch_dir / safe_name(p["label"])
            if force or not all((d / b.name).exists() for b in batches):
                _filter_batches(batches, p["attribute"], d)
    return Prepared(name=name, root=root, full_csv=full_csv, batches=batches, perspectives=persp)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", nargs="+", default=ALL_DATASETS)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    for name in args.datasets:
        prep = prepare(name, force=args.force)
        print(f"[{name}] P = {[p['label'] for p in prep.perspective_list()]}")
        for c in prep.perspectives["candidates"][:6]:
            print(f"    {c['key']:<28} card={c['cardinality']:<5} coverage={c['event_coverage']:.2f} "
                  f"median_acts/group={c['median_activities_per_group']:.0f} "
                  f"{'CHOSEN' if c['chosen'] else ('eligible' if c['eligible'] else 'excluded')}")


if __name__ == "__main__":
    main()
