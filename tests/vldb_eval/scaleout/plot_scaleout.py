"""
tests/vldb_eval/scaleout/plot_scaleout.py — figures for Exp 2 (SCALE-OUT).

Reads results/exp_scaleout/*.jsonl and writes results/figures/*.pdf plus
matching CSV tables under results/tables/.  Style mirrors plot_indexing.py.

Figures (x-axis = number of Spark executors == cores_max):
  fig_exp2_scaleout_x<N>.pdf   (a) indexing time, eager vs adaptive
                               (b) query latency on the case perspective,
                                   eager/adaptive x structural/attribute
  fig_exp2_multiperspective_x<N>.pdf adaptive query latency per perspective
                               (case + 3), attribute (solid) & structural (dashed)
  fig_exp2_shuffle_x<N>.pdf    index-build shuffle time per executor, eager vs adaptive
  fig_exp2_size_scaling.pdf    indexing time vs data size at max executors

Usage:
    python -m tests.vldb_eval.scaleout.plot_scaleout [--sizes N ...] [--results DIR]
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402

from tests.vldb_eval.eval_common import RESULTS_DIR  # noqa: E402

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
MARKERS = ["o", "s", "^", "D", "v", "P", "X", "*"]
TEXT, TEXT2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#ffffff"

plt.rcParams.update({
    "font.family": "serif",
    "font.serif": ["Liberation Serif", "Nimbus Roman", "DejaVu Serif"],
    "mathtext.fontset": "stix",
    "font.size": 8, "axes.titlesize": 8, "axes.labelsize": 8, "legend.fontsize": 7,
    "xtick.labelsize": 7, "ytick.labelsize": 7,
    "axes.edgecolor": TEXT2, "axes.labelcolor": TEXT, "xtick.color": TEXT2, "ytick.color": TEXT2,
    "text.color": TEXT, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
    "lines.linewidth": 1.6, "lines.markersize": 4.5, "legend.frameon": False,
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.bbox": "tight",
    "pdf.fonttype": 42,
})

CASE = "case"
SYS_COLOR = {"eager": SERIES[1], "adaptive": SERIES[0]}
SYS_LABEL = {"eager": "Eager (case)", "adaptive": "Adaptive"}

# Figure 2 shows only these perspectives (data is collected for all 4).
MULTIPERSP_SHOW = ["Action", "org:resource"]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load(results_dir: Path, query_limit: int | None = 2) -> tuple[list[dict], list[dict]]:
    """Index and query rows of every cell.  Only the first `query_limit` pairs
    per perspective are kept (qi < query_limit): early cells ran 4 pairs, the
    rest 2, and the workload is ordered, so this yields the same queries in
    every cell."""
    index_rows, query_rows = [], []
    for fp in sorted(results_dir.glob("*.jsonl")):
        for line in fp.read_text().splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            if r.get("type") == "index":
                index_rows.append(r)
            elif r.get("type") == "query":
                if query_limit is not None and r.get("qi", 0) >= query_limit:
                    continue
                # cores/size live on the index/meta rows; carry from filename.
                stem = fp.stem  # e.g. adaptive_r3_c12, adaptivemp_r3_c12
                parts = stem.split("_")
                r.setdefault("size", int(parts[1][1:]))
                r.setdefault("cores", int(parts[2][1:]))
                # Part "mp" (Fig 2) cells are re-indexed and hold only the
                # attribute perspectives; older cells predate the field.
                r.setdefault("part", "mp" if stem.startswith("adaptivemp") else "case")
                query_rows.append(r)
    return index_rows, query_rows


def _median(xs: list[float]) -> float | None:
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def latency(query_rows, *, system, size, perspective, kind, warm=True, part=None,
            exclude=frozenset(), variant=None):
    """Median latency over queries; warm uses the last round only.

    Queries that matched no group are left out (the workload is built to be
    non-empty; an empty answer skips the per-group work and would bias the
    median low).  Censored timeouts carry no count and are kept.  `part`
    restricts to one part of the sweep ("case" = Fig 1, "mp" = Fig 2);
    `exclude` drops the given patterns (see always_censored).  `variant`
    selects a counterfactual run (e.g. "persist100", exp_scaleout_persist);
    None means the main sweep.
    """
    rows = [r for r in query_rows
            if r["system"] == system and r["size"] == size
            and r["perspective"] == perspective and r["kind"] == kind
            and r.get("matched_groups") != 0
            and (part is None or r.get("part") == part)
            and r["pattern"] not in exclude
            and r.get("variant") == variant]
    if not rows:
        return {}
    if warm:
        last = max(r.get("round", 0) for r in rows)
        rows = [r for r in rows if r.get("round", 0) == last]
    by_cores = defaultdict(list)
    for r in rows:
        by_cores[r["cores"]].append(r.get("latency_s"))
    return {c: _median(v) for c, v in sorted(by_cores.items())}


def index_series(index_rows, *, system, size, field="index_time_s"):
    """cores -> value of one index metric.

    "shuffle_seconds" is shuffle write + fetch-wait time summed over all tasks
    of the index build; "shuffle_per_executor" divides it by the executors the
    driver held (one core each; cells predating that field ran exactly
    `cores` executors), so it no longer grows just because there are more
    tasks and more cross-node fetches.
    """
    rows = [r for r in index_rows if r["system"] == system and r["size"] == size]
    out = {}
    for r in sorted(rows, key=lambda x: x["cores"]):
        if field in ("shuffle_seconds", "shuffle_per_executor"):
            val = (r.get("shuffle") or {}).get("shuffle_seconds")
            if val is not None and field == "shuffle_per_executor":
                val /= r.get("executors") or r["cores"]
        else:
            val = r.get(field)
        if val is not None:
            out[r["cores"]] = val
    return out


def _exact_xticks(ax, fmt: str = "{:g}") -> None:
    """Put x ticks exactly on the plotted x values (the swept executor counts
    or replication factors) instead of wherever the auto-locator lands."""
    xs = sorted({float(x) for line in ax.get_lines() for x in line.get_xdata()})
    if not xs:
        return
    ax.set_xticks(xs)
    ax.set_xticklabels([fmt.format(x) for x in xs])
    ax.xaxis.set_minor_locator(matplotlib.ticker.NullLocator())


def _plot_line(ax, series: dict, label, color, marker, ls="-"):
    if not series:
        return
    xs = sorted(series)
    ys = [series[x] for x in xs]
    ax.plot(xs, ys, label=label, color=color, marker=marker, linestyle=ls)


# ---------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------

def fig_scaleout(index_rows, query_rows, size, figs: Path, tabs: Path):
    fig, (axa, axb) = plt.subplots(1, 2, figsize=(7.0, 2.7))

    # (a) indexing time vs #executors
    for sysname in ("eager", "adaptive"):
        _plot_line(axa, index_series(index_rows, system=sysname, size=size),
                   SYS_LABEL[sysname], SYS_COLOR[sysname],
                   MARKERS[0 if sysname == "adaptive" else 1])
    axa.set_xlabel("# executors (cores)")
    axa.set_ylabel("indexing time (s)")
    axa.set_title(f"(a) Indexing time — BPIC2017 ×{size}")
    axa.legend()
    _exact_xticks(axa)

    # (b) query latency on case: eager/adaptive x structural/attribute
    combos = [("adaptive", "structural"), ("adaptive", "attribute"),
              ("eager", "structural"), ("eager", "attribute")]
    for i, (sysname, kind) in enumerate(combos):
        ser = latency(query_rows, system=sysname, size=size,
                      perspective=CASE, kind=kind)
        _plot_line(axb, ser, f"{SYS_LABEL[sysname]} / {kind}",
                   SYS_COLOR[sysname], MARKERS[i % len(MARKERS)],
                   ls="-" if kind == "structural" else "--")
    # Counterfactual runs with adaptive pairs forced PERSISTENT, if present for
    # this size (exp_scaleout_persist): same queries, own colour.
    variants = sorted({r["variant"] for r in query_rows
                       if r.get("variant") and r["size"] == size})
    for j, v in enumerate(variants):
        pct = v[len("persist"):] if v.startswith("persist") else v
        for kind, mk in (("structural", "P"), ("attribute", "X")):
            ser = latency(query_rows, system="adaptive", size=size,
                          perspective=CASE, kind=kind, variant=v)
            _plot_line(axb, ser, f"Adaptive, {pct}% persistent / {kind}",
                       SERIES[2 + j], mk, ls="-" if kind == "structural" else "--")
    axb.set_xlabel("# executors (cores)")
    axb.set_ylabel("query latency (s)")
    axb.set_title("(b) Query latency (case)")
    axb.legend(fontsize=6 if variants else None)
    _exact_xticks(axb)

    fig.tight_layout()
    out = figs / f"fig_exp2_scaleout_x{size}.pdf"
    fig.savefig(out)
    plt.close(fig)
    print("wrote", out)

    # table
    with (tabs / f"tab_exp2_scaleout_x{size}.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["metric", "system", "kind", "cores", "value", "size"])
        for sysname in ("eager", "adaptive"):
            for c, v in index_series(index_rows, system=sysname, size=size).items():
                w.writerow(["index_time_s", sysname, "", c, v, size])
        for sysname, kind in combos:
            for c, v in latency(query_rows, system=sysname, size=size,
                                perspective=CASE, kind=kind).items():
                w.writerow(["latency_s", sysname, kind, c, v, size])
        for variant in variants:
            for kind in ("structural", "attribute"):
                for c, v in latency(query_rows, system="adaptive", size=size,
                                    perspective=CASE, kind=kind, variant=variant).items():
                    w.writerow(["latency_s", f"adaptive-{variant}", kind, c, v, size])


def always_censored(query_rows, *, part, perspective, kind) -> set[str]:
    """Patterns that hit the query cap in every run of every cell of a part.

    With two queries per point, such a query would put the cap itself into
    every median, so it is plotted separately as a note rather than folded in.
    """
    by_pat = defaultdict(list)
    for r in query_rows:
        if (r.get("part") == part and r["perspective"] == perspective
                and r["kind"] == kind):
            by_pat[r["pattern"]].append(bool(r.get("timeout")))
    return {p for p, t in by_pat.items() if t and all(t)}


def fig_multiperspective(query_rows, size, figs: Path, tabs: Path, cap_s: float = 120.0):
    present = {r["perspective"] for r in query_rows
               if r["system"] == "adaptive" and r["size"] == size and r.get("part") == "mp"}
    # Only the requested perspectives (Action, org:resource), in that order.
    persps = [p for p in MULTIPERSP_SHOW if p in present]
    excluded = {(p, k): always_censored(query_rows, part="mp", perspective=p, kind=k)
                for p in persps for k in ("structural", "attribute")}

    fig, ax = plt.subplots(figsize=(4.2, 3.0))
    for i, p in enumerate(persps):
        color = SERIES[i % len(SERIES)]
        mk = MARKERS[i % len(MARKERS)]
        _plot_line(ax, latency(query_rows, system="adaptive", size=size,
                               perspective=p, kind="attribute", part="mp",
                               exclude=excluded[(p, "attribute")]),
                   f"{p} (attr)", color, mk, ls="-")
        _plot_line(ax, latency(query_rows, system="adaptive", size=size,
                               perspective=p, kind="structural", part="mp",
                               exclude=excluded[(p, "structural")]),
                   f"{p} (struct)", color, mk, ls="--")
    ax.set_xlabel("# executors (cores)")
    ax.set_ylabel("query latency (s, log scale)")
    ax.set_title(f"Adaptive multiperspective latency — ×{size}")
    # Log scale: org:resource attribute (~40-50 s) would otherwise flatten the
    # ~1-2 s series onto the x-axis.  Plain-number labels at 1-2-3-5 steps.
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(matplotlib.ticker.LogLocator(base=10, subs=(1.0, 2.0, 3.0, 5.0)))
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
    ax.yaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    ax.legend(ncol=1, fontsize=6)
    _exact_xticks(ax)
    notes = [f"{p} {k[:4]}.: {len(pats)} of its queries >{cap_s:.0f} s in every "
             f"configuration, omitted" for (p, k), pats in excluded.items() if pats]
    if notes:
        fig.text(0.01, -0.02, "\n".join(notes), fontsize=6, color=TEXT2,
                 ha="left", va="top")
    fig.tight_layout()
    out = figs / f"fig_exp2_multiperspective_x{size}.pdf"
    fig.savefig(out)
    plt.close(fig)
    print("wrote", out)

    with (tabs / f"tab_exp2_multiperspective_x{size}.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["perspective", "kind", "cores", "latency_s", "size", "omitted_patterns"])
        for p in persps:
            for kind in ("structural", "attribute"):
                omitted = " | ".join(sorted(excluded[(p, kind)]))
                for c, v in latency(query_rows, system="adaptive", size=size,
                                    perspective=p, kind=kind, part="mp",
                                    exclude=excluded[(p, kind)]).items():
                    w.writerow([p, kind, c, v, size, omitted])


def fig_shuffle(index_rows, size, figs: Path, tabs: Path):
    fig, ax = plt.subplots(figsize=(4.0, 2.8))
    for sysname in ("eager", "adaptive"):
        _plot_line(ax, index_series(index_rows, system=sysname, size=size,
                                    field="shuffle_per_executor"),
                   SYS_LABEL[sysname], SYS_COLOR[sysname],
                   MARKERS[0 if sysname == "adaptive" else 1])
    ax.set_xlabel("# executors (cores)")
    ax.set_ylabel("shuffle time per executor (s)")
    ax.set_title(f"Index-build shuffle per executor — ×{size}")
    ax.legend()
    _exact_xticks(ax)
    fig.tight_layout()
    out = figs / f"fig_exp2_shuffle_x{size}.pdf"
    fig.savefig(out)
    plt.close(fig)
    print("wrote", out)

    with (tabs / f"tab_exp2_shuffle_x{size}.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["system", "cores", "shuffle_total_s", "shuffle_per_executor_s", "size"])
        for sysname in ("eager", "adaptive"):
            total = index_series(index_rows, system=sysname, size=size, field="shuffle_seconds")
            per = index_series(index_rows, system=sysname, size=size, field="shuffle_per_executor")
            for c in sorted(total):
                w.writerow([sysname, c, total[c], per.get(c), size])


def fig_size_scaling(index_rows, figs: Path):
    sizes = sorted({r["size"] for r in index_rows})
    cores_all = sorted({r["cores"] for r in index_rows})
    if not cores_all:
        return
    max_c = cores_all[-1]
    fig, ax = plt.subplots(figsize=(4.0, 2.8))
    for sysname in ("eager", "adaptive"):
        xs, ys = [], []
        for s in sizes:
            v = [r["index_time_s"] for r in index_rows
                 if r["system"] == sysname and r["size"] == s and r["cores"] == max_c]
            if v:
                xs.append(s)
                ys.append(statistics.median(v))
        if xs:
            ax.plot(xs, ys, label=SYS_LABEL[sysname], color=SYS_COLOR[sysname],
                    marker=MARKERS[0 if sysname == "adaptive" else 1])
    ax.set_xlabel("replication factor")
    ax.set_ylabel("indexing time (s)")
    ax.set_title(f"Indexing vs data size @ {max_c} executors")
    ax.legend()
    _exact_xticks(ax, fmt="{:g}×")
    fig.tight_layout()
    out = figs / "fig_exp2_size_scaling.pdf"
    fig.savefig(out)
    plt.close(fig)
    print("wrote", out)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", type=Path, default=RESULTS_DIR / "exp_scaleout")
    ap.add_argument("--sizes", nargs="+", type=int, default=None,
                    help="data sizes for the per-size figures (default: all present); "
                         "files are suffixed _x<size>")
    ap.add_argument("--query-limit", type=int, default=2,
                    help="use only the first N pairs per perspective (0 = all)")
    args = ap.parse_args()

    index_rows, query_rows = load(args.results, args.query_limit or None)
    if not index_rows and not query_rows:
        raise SystemExit(f"no results in {args.results}")

    figs = RESULTS_DIR / "figures"
    tabs = RESULTS_DIR / "tables"
    figs.mkdir(parents=True, exist_ok=True)
    tabs.mkdir(parents=True, exist_ok=True)

    sizes = sorted({r["size"] for r in index_rows} | {r["size"] for r in query_rows})
    for size in args.sizes or sizes:
        print(f"plotting size ×{size} (available: {sizes})")
        fig_scaleout(index_rows, query_rows, size, figs, tabs)
        fig_multiperspective(query_rows, size, figs, tabs)
        fig_shuffle(index_rows, size, figs, tabs)
    fig_size_scaling(index_rows, figs)


if __name__ == "__main__":
    main()
