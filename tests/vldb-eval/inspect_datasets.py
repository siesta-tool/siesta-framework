"""
tests/inspect_datasets.py

Standalone diagnostic — parses each dataset in a directory (no Spark,
no API, no MinIO) and prints per-attribute statistics so we can pick
sensible perspectives for the warm-up experiment.

What it reports per dataset
---------------------------
- Event count, trace count, distinct activities
- Per attribute key:
    * distinct values seen
    * sample values (up to 5)
    * type guess (numeric / boolean / categorical / id-like / text)
    * event-level vs case-level designation
    * "perspective verdict" — a short tag suggesting whether the
      attribute is plausibly useful as a grouping perspective

Event-level vs case-level
-------------------------
An attribute is EVENT_LEVEL if its value varies within at least some
traces — that's what makes it a real perspective: it partitions events
of a single process instance into multiple groups.  Otherwise it's
CASE_LEVEL: every event of a trace carries the same value, so grouping
by this attribute is barely distinguishable from grouping by trace ID.

Detection metric: the average number of distinct values per trace for
that attribute, computed across traces where the attribute appears.
Constant-per-trace attributes (e.g. loan amount, patient age) average
exactly 1.0; per-event attributes (e.g. resource, lifecycle state)
average notably higher.  The threshold is 1.2 by default — conservative
enough to absorb noise but tight enough to catch lifted case-level
metadata.

Type guesses
------------
- NUMERIC     — every observed value parses as a number
- BOOLEAN     — distinct values are a subset of {true/false/0/1/yes/no}
- CATEGORICAL — 2 <= distinct <= high_card_cut string values
- HIGH_CARD   — > high_card_cut distinct string values
- TEXT        — > 1000 distinct values OR average length > 40 (free text)
- SINGLETON   — exactly 1 distinct value (no information)

Perspective verdicts
--------------------
- GOOD            — event-level, categorical or moderately high-card;
                    likely produces meaningful pair co-occurrences.
- CASE_LEVEL      — would otherwise be GOOD, but the value is
                    effectively constant within each trace (lifted
                    trace-level metadata).  Almost grouping by trace.
- DEGENERATE_BOOL — boolean flag; partitions into 2 huge groups, almost
                    always splits activity pairs across groups.
- TOO_LOW         — <= 2 distinct values; not boolean but still trivial.
- TOO_HIGH        — > N * 10 distinct values (per-event identifier).
- TEXT            — free-text field, not a category.
- NUMERIC         — numeric attribute, almost never useful as a partition.
- EMPTY           — no values seen.

Usage
-----
    python tests/inspect_datasets.py datasets/
    python tests/inspect_datasets.py datasets/ --max-events 0    # full pass
    python tests/inspect_datasets.py datasets/ --json out.json    # machine-readable
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path


_BLOCKED_KEYS = {
    "concept:name", "time:timestamp",
    "activity", "trace_id", "timestamp", "position",
    "case:concept:name", "case_id",
}

_XES_ATTR_TAGS  = {"string", "date", "int", "float", "boolean", "id"}
_XES_NS_RE      = re.compile(r"^\{[^}]+\}")
_BOOLEAN_TOKENS = {"true", "false", "0", "1", "yes", "no", "y", "n", "t", "f"}

# Threshold for EVENT_LEVEL vs CASE_LEVEL.  An attribute is EVENT_LEVEL
# if it averages strictly more than this many distinct values per trace
# (across traces where it appears at all).  Case-level attributes
# average exactly 1.0; the 1.2 buffer absorbs occasional XES noise.
_EVENT_LEVEL_THRESHOLD = 1.2


def _strip_ns(tag: str) -> str:
    return _XES_NS_RE.sub("", tag)


def _looks_numeric(s: str) -> bool:
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Reporting types
# ---------------------------------------------------------------------------

@dataclass
class AttrStats:
    key: str
    distinct: int
    samples: list[str]
    numeric_count: int
    boolean_like: bool
    avg_len: float
    avg_distinct_per_trace: float = 0.0   # 1.0 = constant per trace
    traces_seen: int = 0                  # traces where attr appears at all
    type_guess: str   = ""
    event_level: bool = False
    verdict: str      = ""


@dataclass
class DatasetReport:
    path: str
    format: str
    events_scanned: int
    truncated: bool
    activities: int
    activity_sample: list[str] = field(default_factory=list)
    traces: int = 0
    attrs: list[AttrStats] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Per-trace tracking
# ---------------------------------------------------------------------------
#
# To distinguish event-level from case-level attributes we need to know,
# for each attribute, how the value varies within a single trace.  We
# track:
#
#   per_trace_distinct_sum[k] — running sum of |distinct values of k in
#                                trace t|, summed over all traces t
#   traces_with_attr[k]       — count of traces where k appears at least
#                                once
#
# At end-of-trace we fold the trace's local state into these counters.
# The "average distinct per trace" is then
#     per_trace_distinct_sum[k] / traces_with_attr[k]
# which is 1.0 for case-level attrs and > 1 for event-level ones.

def _commit_trace(
    trace_local: dict[str, set[str]],
    per_trace_distinct_sum: dict[str, int],
    traces_with_attr: dict[str, int],
) -> None:
    """Fold one trace's local distinct-value sets into the global
    counters and clear the local state for the next trace."""
    for k, vset in trace_local.items():
        per_trace_distinct_sum[k] += len(vset)
        traces_with_attr[k] += 1
    trace_local.clear()


def _accumulate_event(
    cur_attrs: dict[str, str],
    trace_local: dict[str, set[str]],
    values: dict[str, list[str]],
    distinct_counts: dict[str, set[str]],
    numeric_counts: dict[str, int],
    total_counts: dict[str, int],
    length_sums: dict[str, int],
    max_samples: int,
) -> None:
    """Fold one event's attributes into both the global and per-trace
    stats."""
    for k, v in cur_attrs.items():
        if k in _BLOCKED_KEYS:
            continue
        total_counts[k] += 1
        length_sums[k] += len(v)
        if _looks_numeric(v):
            numeric_counts[k] += 1
        if v not in distinct_counts[k]:
            distinct_counts[k].add(v)
            if len(values[k]) < max_samples:
                values[k].append(v)
        # Per-trace distinct tracking
        trace_local.setdefault(k, set()).add(v)


def _build_attr_stats(
    keys: list[str],
    distinct_counts: dict[str, set[str]],
    values: dict[str, list[str]],
    numeric_counts: dict[str, int],
    total_counts: dict[str, int],
    length_sums: dict[str, int],
    per_trace_distinct_sum: dict[str, int],
    traces_with_attr: dict[str, int],
) -> list[AttrStats]:
    stats: list[AttrStats] = []
    for k in sorted(keys):
        n_dist  = len(distinct_counts[k])
        total   = total_counts[k]
        n_num   = numeric_counts[k]
        avg_len = (length_sums[k] / total) if total else 0.0

        lc_distinct = {v.lower() for v in distinct_counts[k] if isinstance(v, str)}
        boolean_like = (
            1 <= n_dist <= 2
            and lc_distinct.issubset(_BOOLEAN_TOKENS)
        )

        traces_seen = traces_with_attr[k]
        avg_dpt = (
            per_trace_distinct_sum[k] / traces_seen
            if traces_seen else 0.0
        )

        stats.append(AttrStats(
            key=k,
            distinct=n_dist,
            samples=values[k][:5],
            numeric_count=n_num,
            boolean_like=boolean_like,
            avg_len=round(avg_len, 1),
            avg_distinct_per_trace=round(avg_dpt, 2),
            traces_seen=traces_seen,
        ))
    return stats


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_csv(
    path: Path, max_events: int, max_samples: int,
) -> DatasetReport:
    values: dict[str, list[str]]                = defaultdict(list)
    distinct_counts: dict[str, set[str]]        = defaultdict(set)
    numeric_counts: dict[str, int]              = defaultdict(int)
    total_counts: dict[str, int]                = defaultdict(int)
    length_sums: dict[str, int]                 = defaultdict(int)
    per_trace_distinct_sum: dict[str, int]      = defaultdict(int)
    traces_with_attr: dict[str, int]            = defaultdict(int)
    activities: set[str]                        = set()
    activity_order: list[str]                   = []
    trace_ids: set[str]                         = set()

    cur_trace_id: str | None = None
    trace_local: dict[str, set[str]] = {}

    n = 0
    truncated = False
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if max_events and n >= max_events:
                truncated = True
                break
            n += 1
            tid = (
                row.get("trace_id")
                or row.get("case:concept:name")
                or row.get("case_id")
            )
            # Flush per-trace state when the trace_id changes.  This
            # relies on the CSV being ordered by trace_id — most
            # process-mining CSVs are.  If it isn't, the estimate is
            # noisier but still directionally correct.
            if tid != cur_trace_id and cur_trace_id is not None:
                _commit_trace(trace_local, per_trace_distinct_sum,
                              traces_with_attr)
            cur_trace_id = tid
            if tid:
                trace_ids.add(tid)

            act = row.get("activity") or row.get("concept:name")
            if act and act not in activities:
                activities.add(act)
                activity_order.append(act)

            cur = {k: v for k, v in row.items() if v not in (None, "")}
            _accumulate_event(
                cur, trace_local, values, distinct_counts,
                numeric_counts, total_counts, length_sums, max_samples,
            )

    if trace_local:
        _commit_trace(trace_local, per_trace_distinct_sum, traces_with_attr)

    attrs = _build_attr_stats(
        list(values.keys()), distinct_counts, values,
        numeric_counts, total_counts, length_sums,
        per_trace_distinct_sum, traces_with_attr,
    )
    return DatasetReport(
        path=str(path),
        format="csv",
        events_scanned=n,
        truncated=truncated,
        activities=len(activity_order),
        activity_sample=activity_order[:10],
        traces=len(trace_ids),
        attrs=attrs,
    )


def parse_xes(
    path: Path, max_events: int, max_samples: int,
) -> DatasetReport:
    values: dict[str, list[str]]                = defaultdict(list)
    distinct_counts: dict[str, set[str]]        = defaultdict(set)
    numeric_counts: dict[str, int]              = defaultdict(int)
    total_counts: dict[str, int]                = defaultdict(int)
    length_sums: dict[str, int]                 = defaultdict(int)
    per_trace_distinct_sum: dict[str, int]      = defaultdict(int)
    traces_with_attr: dict[str, int]            = defaultdict(int)
    activities: set[str]                        = set()
    activity_order: list[str]                   = []

    n_events = 0
    n_traces = 0
    truncated = False
    in_event = False
    in_trace = False
    cur_attrs: dict[str, str] = {}
    trace_local: dict[str, set[str]] = {}

    context = ET.iterparse(str(path), events=("start", "end"))
    for ev, elem in context:
        tag = _strip_ns(elem.tag)

        if ev == "start" and tag == "trace":
            in_trace = True
            n_traces += 1
        elif ev == "end" and tag == "trace":
            _commit_trace(trace_local, per_trace_distinct_sum, traces_with_attr)
            in_trace = False
            elem.clear()

        elif ev == "start" and tag == "event":
            in_event = True
            cur_attrs = {}
        elif ev == "end" and tag == "event":
            act = cur_attrs.get("concept:name")
            if act and act not in activities:
                activities.add(act)
                activity_order.append(act)
            _accumulate_event(
                cur_attrs, trace_local, values, distinct_counts,
                numeric_counts, total_counts, length_sums, max_samples,
            )
            in_event = False
            elem.clear()
            n_events += 1
            if max_events and n_events >= max_events:
                truncated = True
                # Flush the in-progress trace so its state isn't lost.
                _commit_trace(trace_local, per_trace_distinct_sum,
                              traces_with_attr)
                break

        elif ev == "end" and in_event and tag in _XES_ATTR_TAGS:
            k = elem.attrib.get("key")
            v = elem.attrib.get("value")
            if k and v is not None:
                cur_attrs[k] = v

    attrs = _build_attr_stats(
        list(values.keys()), distinct_counts, values,
        numeric_counts, total_counts, length_sums,
        per_trace_distinct_sum, traces_with_attr,
    )
    return DatasetReport(
        path=str(path),
        format="xes",
        events_scanned=n_events,
        truncated=truncated,
        activities=len(activity_order),
        activity_sample=activity_order[:10],
        traces=n_traces,
        attrs=attrs,
    )


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify(stat: AttrStats, total_events: int, high_card_cut: int) -> AttrStats:
    """Tag each attribute with a type_guess, event-level flag, and verdict."""
    n  = stat.distinct
    nn = stat.numeric_count

    # Type guess
    if n == 0:
        stat.type_guess = "EMPTY"
    elif n == 1:
        stat.type_guess = "SINGLETON"
    elif stat.boolean_like:
        stat.type_guess = "BOOLEAN"
    elif total_events and nn / total_events > 0.95:
        stat.type_guess = "NUMERIC"
    elif n > 1000 and stat.avg_len > 40:
        stat.type_guess = "TEXT"
    elif n > high_card_cut:
        stat.type_guess = "HIGH_CARD"
    else:
        stat.type_guess = "CATEGORICAL"

    # Event-level vs case-level.  Only meaningful for non-degenerate
    # attributes; for SINGLETON/BOOLEAN/EMPTY the distinction is moot.
    stat.event_level = stat.avg_distinct_per_trace > _EVENT_LEVEL_THRESHOLD

    # Perspective verdict
    if stat.type_guess == "BOOLEAN":
        stat.verdict = "DEGENERATE_BOOL"
    elif stat.type_guess == "SINGLETON":
        stat.verdict = "TOO_LOW"
    elif stat.type_guess == "EMPTY":
        stat.verdict = "EMPTY"
    elif stat.type_guess == "NUMERIC":
        stat.verdict = "NUMERIC"
    elif stat.type_guess == "TEXT":
        stat.verdict = "TEXT"
    elif n <= 2:
        stat.verdict = "TOO_LOW"
    elif n > high_card_cut * 10:
        stat.verdict = "TOO_HIGH"
    elif not stat.event_level:
        # Otherwise-good categorical / high-card attribute, but the
        # value is effectively constant within each trace.
        stat.verdict = "CASE_LEVEL"
    else:
        stat.verdict = "GOOD"

    return stat


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

_VERDICT_ORDER = {
    "GOOD":            0,
    "CASE_LEVEL":      1,
    "DEGENERATE_BOOL": 2,
    "TOO_LOW":         3,
    "TOO_HIGH":        4,
    "TEXT":            5,
    "NUMERIC":         6,
    "EMPTY":           7,
}


def print_report(rep: DatasetReport) -> None:
    print()
    print("=" * 96)
    print(f"{rep.path}")
    print(
        f"  format={rep.format}  events={rep.events_scanned}"
        f"{'  (truncated)' if rep.truncated else ''}"
        f"  traces={rep.traces}  activities={rep.activities}"
    )
    if rep.activity_sample:
        sample = ", ".join(rep.activity_sample[:5])
        more   = "..." if rep.activities > 5 else ""
        print(f"  activities: {sample}{more}")

    sorted_attrs = sorted(
        rep.attrs,
        key=lambda a: (_VERDICT_ORDER.get(a.verdict, 99), -a.distinct, a.key),
    )

    print()
    print(f"  {'KEY':<35} {'TYPE':<12} {'DIST':>6} {'DPT':>5}  "
          f"{'VERDICT':<16}  SAMPLES")
    print(f"  {'-'*35} {'-'*12} {'-'*6} {'-'*5}  {'-'*16}  {'-'*7}")
    for a in sorted_attrs:
        samples = ", ".join(repr(s)[:22] for s in a.samples[:3])
        # DPT = "distinct values per trace" — 1.00 means case-level,
        # higher means event-level.
        print(f"  {a.key:<35} {a.type_guess:<12} {a.distinct:>6} "
              f"{a.avg_distinct_per_trace:>5.2f}  "
              f"{a.verdict:<16}  {samples}")

    goods       = [a for a in rep.attrs if a.verdict == "GOOD"]
    case_levels = [a for a in rep.attrs if a.verdict == "CASE_LEVEL"]
    if goods:
        print()
        print(
            f"  ► {len(goods)} GOOD perspective(s): "
            f"{', '.join(a.key for a in sorted(goods, key=lambda x: x.distinct))}"
        )
    else:
        print()
        print("  ► No GOOD perspective candidates found.")
    if case_levels:
        print(
            f"  ► {len(case_levels)} CASE_LEVEL (skipped): "
            f"{', '.join(a.key for a in sorted(case_levels, key=lambda x: x.distinct)[:6])}"
            f"{' ...' if len(case_levels) > 6 else ''}"
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Inspect XES/CSV datasets and identify usable perspectives.",
    )
    ap.add_argument("target", type=Path,
                    help="A dataset file or a directory of datasets.")
    ap.add_argument("--max-events", type=int, default=50000,
                    help="Stop after N events per dataset.  0 means read "
                         "the entire file (slow on large XES).")
    ap.add_argument("--max-samples", type=int, default=10,
                    help="Number of sample values retained per attribute.")
    ap.add_argument("--high-card-cut", type=int, default=200,
                    help="Distinct-value count above which an attribute is "
                         "labelled HIGH_CARD.  Distinct > 10x this cut is "
                         "labelled TOO_HIGH (per-event identifier).")
    ap.add_argument("--json", type=Path, default=None,
                    help="Also write a JSON dump of all reports here.")
    args = ap.parse_args()

    if not args.target.exists():
        print(f"ERROR: {args.target} does not exist", file=sys.stderr)
        return 2

    if args.target.is_file():
        candidates = [args.target]
    else:
        candidates = sorted(
            p for p in args.target.iterdir()
            if p.suffix.lower() in {".xes", ".csv"} and p.is_file()
        )

    if not candidates:
        print(f"ERROR: no .xes or .csv files in {args.target}", file=sys.stderr)
        return 2

    reports: list[DatasetReport] = []
    for path in candidates:
        try:
            fmt = path.suffix.lower().lstrip(".")
            if fmt == "csv":
                rep = parse_csv(path, args.max_events, args.max_samples)
            else:
                rep = parse_xes(path, args.max_events, args.max_samples)
        except Exception as exc:
            print(f"\nERROR parsing {path}: {exc}", file=sys.stderr)
            continue

        rep.attrs = [
            classify(a, rep.events_scanned, args.high_card_cut)
            for a in rep.attrs
        ]
        reports.append(rep)
        print_report(rep)

    if args.json:
        args.json.write_text(
            json.dumps([asdict(r) for r in reports], indent=2, default=str),
        )
        print(f"\nJSON written to {args.json}")

    return 0


if __name__ == "__main__":
    sys.exit(main())