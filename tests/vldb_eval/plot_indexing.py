"""
tests/vldb_eval/plot_indexing.py — figures and tables for Exp 1-5.

Reads results/<experiment>/<dataset>.jsonl and writes
results/figures/*.pdf and results/tables/*.{csv,tex}.  Every figure has a
table with the same numbers.

    exp1+2 fig3_adaptation.pdf      (a) latency per repetition with tier brackets and eager
                                    case baselines, (b) latency vs query position, skewed
                                    (solid) vs uniform (dashed), all datasets
          tab_exp1_warmup           rep1..rep5, speedup rep1/rep5, baseline
          tab_exp2_skew_uniform     warm-from position, warm share, mean latency q10-30
    exp3  fig4_hotset_<ds>.pdf      (a) latency vs position per hot-set size, materialisation,
                                    (b) fraction PERSISTENT per round (hot / cold / overall, uniform)
          tab_exp3_hotset_<ds>      materialisation position per hot-set size
    exp4  tab_exp4_batches          absolute per-batch seconds, all datasets and configs
          tab_exp4_savings          R, saving, F_persp, F_pair (cumulative over B1-B5)
          fig_exp4_pair_sweep.pdf   per-batch maintenance cost vs maintained pairs
    exp5  fig_exp5_bpic2017.pdf     eager per perspective (bars) vs adaptive (line), R per batch

Usage:
    python -m tests.vldb_eval.plot_indexing [--results DIR]
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
import matplotlib.lines  # noqa: E402
import matplotlib.ticker  # noqa: E402
import matplotlib.transforms  # noqa: E402
import numpy as np  # noqa: E402

from tests.vldb_eval import eval_common  # noqa: E402

# Reference categorical palette (validated, light mode), fixed order.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
MARKERS = ["o", "s", "^", "D", "v", "P", "X", "*"]
TEXT, TEXT2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#ffffff"

DATASET_ORDER = ["bpic2011", "bpic2012", "bpic2015", "bpic2017", "bpic2018", "synthetic"]
LABEL = {"bpic2011": "BPIC 2011", "bpic2012": "BPIC 2012", "bpic2015": "BPIC 2015",
         "bpic2017": "BPIC 2017", "bpic2018": "BPIC 2018", "synthetic": "Synthetic"}
# Colour follows the dataset, never its rank among the plotted ones.
DS_COLOR = {d: SERIES[i] for i, d in enumerate(DATASET_ORDER)}
DS_MARKER = {d: MARKERS[i] for i, d in enumerate(DATASET_ORDER)}

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


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def _datasets(records):
    names = {r["dataset"] for r in records}
    return [d for d in DATASET_ORDER if d in names] + sorted(names - set(DATASET_ORDER))


def write_table(name: str, header: list[str], rows: list[list], out: Path, caption: str = "") -> None:
    out.mkdir(parents=True, exist_ok=True)
    with (out / f"{name}.csv").open("w", newline="") as f:
        csv.writer(f).writerows([header] + rows)

    def fmt(v):
        if v is None:
            return "--"
        if isinstance(v, float):
            return f"{v:.3g}" if abs(v) < 100 else f"{v:.0f}"
        return str(v).replace("_", r"\_").replace("%", r"\%").replace("&", r"\&")

    lines = [r"\begin{tabular}{" + "l" * 2 + "r" * (len(header) - 2) + "}", r"\toprule",
             " & ".join(fmt(h) for h in header) + r" \\", r"\midrule"]
    lines += [" & ".join(fmt(v) for v in row) + r" \\" for row in rows]
    lines += [r"\bottomrule", r"\end{tabular}"]
    if caption:
        lines.insert(0, f"% {caption}")
    (out / f"{name}.tex").write_text("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# Exp 1 + Exp 2  ->  Fig. 3
# ---------------------------------------------------------------------------

TIER = {"SCAN": "ABSENT", "LRU": "TRANSIENT", "DELTA": "PERSISTENT"}
TIER_TICK = {"SCAN": "scan", "LRU": "LRU", "DELTA": "Delta"}


def _modal_source(qs) -> str:
    srcs = [s for r in qs for s in r["pair_sources"].values()]
    return statistics.mode(srcs) if srcs else "?"


def exp1(tabs: Path) -> dict:
    """Per dataset: repetitions, median and individual latencies, tiers, baseline."""
    recs = eval_common.read_results("exp1_warmup")
    data, rows = {}, []
    for ds in _datasets(recs):
        rs = [r for r in recs if r["dataset"] == ds]
        if not any(r["event"] == "done" for r in rs):
            continue
        setup = next(r for r in rs if r["event"] == "setup")
        q = [r for r in rs if r["event"] == "query" and r.get("system") == "adaptive"]
        e = [r for r in rs if r["event"] == "query" and r.get("system") == "eager"]
        reps = sorted({r["rep"] for r in q})
        by_rep = [med([r["time"] for r in q if r["rep"] == k]) for k in reps]
        eager_rep = [med([r["time"] for r in e if r.get("rep") == k]) for k in reps] if e else None
        srcs = [_modal_source([r for r in q if r["rep"] == k]) for k in reps]
        parity = next((r for r in rs if r["event"] == "parity"), None)
        data[ds] = {"reps": reps, "median": by_rep, "sources": srcs, "eager": eager_rep,
                    "points": [(r["rep"], r["time"]) for r in q]}
        e_last = eager_rep[-1] if eager_rep else None
        rows.append([LABEL.get(ds, ds), setup["perspective"], setup["group_count"],
                     *by_rep, "/".join(srcs), by_rep[0] / by_rep[-1] if by_rep[-1] else None,
                     eager_rep[0] if eager_rep else None, e_last,
                     e_last / by_rep[-1] if e_last and by_rep[-1] else None,
                     (f"{parity['pairs'] - len(parity['mismatched'])}/{parity['pairs']}"
                      if parity else None)])
    if rows:
        reps = next(iter(data.values()))["reps"]
        write_table("tab_exp1_warmup",
                    ["dataset", "perspective", "groups", *[f"rep{k} (s)" for k in reps], "sources",
                     "speedup rep1/last", "eager rep1 (s)", "eager last (s)",
                     "eager/adaptive (last)", "support parity"], rows, tabs,
                    "Exp 1: median latency per repetition, case perspective; eager = original SIESTA "
                    "with its case index (all pairs), same queries and repetitions")
    return data


def exp2(tabs: Path) -> dict:
    """Per dataset and workload: latency by query position; convergence table."""
    recs = eval_common.read_results("exp2_skew_uniform")
    data, rows = {}, []
    for ds in _datasets(recs):
        rs = [r for r in recs if r["dataset"] == ds]
        if not any(r["event"] == "done" for r in rs):
            continue
        setup = next(r for r in rs if r["event"] == "setup")
        q = [r for r in rs if r["event"] == "query" and r.get("system", "adaptive") == "adaptive"]
        eq = [r for r in rs if r["event"] == "query" and r.get("system") == "eager"]
        ing = {r.get("system"): r for r in rs if r["event"] == "ingest"}
        a_ing = {r.get("workload"): r["time"] for r in rs
                 if r["event"] == "ingest" and r.get("system") == "adaptive"}
        out, warm_from, warm_frac = {}, {}, {}
        cum, eager = {}, {}
        for wl in ("skewed", "uniform"):
            ordered = sorted((r for r in q if r["workload"] == wl), key=lambda r: r["seq"])
            out[wl] = [(r["seq"], r["time"]) for r in ordered]
            e_ord = sorted((r for r in eq if r["workload"] == wl), key=lambda r: r["seq"])
            eager[wl] = [(r["seq"], r["time"]) for r in e_ord]
            # Cumulative cost: ingest, then every query's latency; adaptive
            # also pays each promotion (pair builds) it triggers.
            if a_ing.get(wl) is not None:
                cum.setdefault("adaptive", {})[wl] = list(np.cumsum(
                    [a_ing[wl]] + [r["time"] + (r.get("promotion_s") or 0.0) for r in ordered]))
            if "eager" in ing and e_ord:
                cum.setdefault("eager", {})[wl] = list(np.cumsum(
                    [ing["eager"]["time"]] + [r["time"] for r in e_ord]))
            warm = [set(r["pair_sources"].values()) <= {"LRU", "DELTA"} for r in ordered]
            warm_frac[wl] = sum(warm) / len(warm) if warm else None
            # First position after which every hot-set query is served warm.
            hot_idx = [k for k, r in enumerate(ordered) if r["hot"]]
            warm_from[wl] = next((ordered[k]["seq"] for k in hot_idx
                                  if all(warm[i] for i in hot_idx if i >= k)), None)
        data[ds] = {**out, "eager": eager, "cum": cum}
        tail = lambda xs: statistics.mean(t for _, t in xs[10:]) if len(xs) > 10 else None  # noqa: E731
        sk, un = tail(out["skewed"]), tail(out["uniform"])
        e_sk = tail(eager["skewed"]) if eager.get("skewed") else None

        def crossover(wl):
            a, e = cum.get("adaptive", {}).get(wl), cum.get("eager", {}).get(wl)
            if not a or not e:
                return None, None
            # Position (0-based query) from which adaptive has spent more than eager.
            pos = next((i - 1 for i in range(1, min(len(a), len(e))) if a[i] > e[i]), None)
            return pos, e[-1] / a[-1]

        x_sk, r_sk = crossover("skewed")
        x_un, r_un = crossover("uniform")
        rows.append([LABEL.get(ds, ds), setup["perspective"],
                     f"{setup['realised_hot_share']['skewed']:.2f}",
                     warm_from["skewed"], warm_frac["skewed"], warm_frac["uniform"],
                     sk, un, e_sk, e_sk / sk if e_sk and sk else None,
                     x_sk, r_sk, x_un, r_un])
    if rows:
        write_table("tab_exp2_skew_uniform",
                    ["dataset", "perspective", "hot share", "hot warm from", "warm share skewed",
                     "warm share uniform", "mean q10+ skewed (s)", "mean q10+ uniform (s)",
                     "eager mean q10+ skewed (s)", "eager/adaptive q10+ skewed",
                     "cost crossover skewed", "cum. eager/adaptive skewed",
                     "cost crossover uniform", "cum. eager/adaptive uniform"], rows, tabs,
                    "Exp 2: hot warm from = first position after which every hot-set query is served "
                    "from the LRU or Delta; warm share = fraction of queries served warm; cost "
                    "crossover = first query after which adaptive's cumulative time (ingest + "
                    "queries + promotions) exceeds eager's (ingest + queries), empty = never; "
                    "cum. eager/adaptive = ratio of cumulative times after the last query")
    return data


ROLLING = 5


def _rolling_median(xs: list, k: int) -> list:
    return [statistics.median(xs[max(0, i - k + 1): i + 1]) for i in range(len(xs))]


def _tier_brackets(ax, reps, sources):
    """ABSENT / TRANSIENT / PERSISTENT brackets over runs of equal tier."""
    runs = []
    for k, src in zip(reps, sources):
        tier = TIER.get(src, src)
        if runs and runs[-1][0] == tier:
            runs[-1][2] = k
        else:
            runs.append([tier, k, k])
    trans = matplotlib.transforms.blended_transform_factory(ax.transData, ax.transAxes)
    for tier, lo, hi in runs:
        x0, x1 = lo - 0.35, hi + 0.35
        ax.plot([x0, x0, x1, x1], [0.95, 0.975, 0.975, 0.95], transform=trans,
                color=TEXT2, linewidth=0.7, clip_on=False)
        ax.text((x0 + x1) / 2, 0.93, tier, transform=trans, ha="center", va="top",
                fontsize=6, style="italic", color=TEXT2)


def fig3(figs: Path, d1: dict, d2: dict) -> None:
    dss = [d for d in DATASET_ORDER if d in d1 or d in d2]
    if not dss:
        return
    fig, (a, b) = plt.subplots(1, 2, figsize=(7.0, 2.6))

    # (a) latency per repetition
    reps_ref, srcs_ref = None, None
    for ds in [d for d in dss if d in d1]:
        d = d1[ds]
        c, m = DS_COLOR[ds], DS_MARKER[ds]
        xs, ys = zip(*d["points"])
        jitter = np.linspace(-0.08, 0.08, len(xs)) if len(xs) > 1 else [0]
        a.scatter(np.array(xs) + jitter, ys, s=5, color=c, alpha=0.18, linewidths=0, zorder=1)
        a.plot(d["reps"], d["median"], color=c, marker=m, markerfacecolor=SURFACE,
               markeredgewidth=1.2, zorder=3)
        if d["eager"]:
            a.plot(d["reps"], d["eager"], color=c, linestyle=":", linewidth=1.0,
                   marker=m, markersize=2.5, alpha=0.9, zorder=2)
        if reps_ref is None or len(d["reps"]) > len(reps_ref):
            reps_ref, srcs_ref = d["reps"], d["sources"]
    if reps_ref:
        a.set_yscale("log")
        a.set_xticks(reps_ref)
        a.set_xticklabels([f"{k}\n({TIER_TICK.get(s, s)})" for k, s in zip(reps_ref, srcs_ref)])
        a.set_xlim(reps_ref[0] - 0.5, reps_ref[-1] + 0.5)
        lo, hi = a.get_ylim()
        a.set_ylim(lo, hi * 3)  # headroom for the tier brackets
        _tier_brackets(a, reps_ref, srcs_ref)
        a.set_xlabel("Query repetition (exposure)")
        a.set_ylabel("Query latency (s)")
        a.text(-0.13, 1.0, "(a)", transform=a.transAxes, fontweight="bold", va="bottom")

    # (b) latency vs query position, skewed solid / uniform dashed
    for ds in [d for d in dss if d in d2]:
        c, m = DS_COLOR[ds], DS_MARKER[ds]
        for wl, ls, face in (("skewed", "-", c), ("uniform", "--", SURFACE)):
            pts = d2[ds].get(wl) or []
            if not pts:
                continue
            xs, ys = zip(*pts)
            # A 20 % share of cold queries makes the raw series jump between
            # cold and warm; the rolling median shows where each workload
            # settles.  Raw latencies are in the results and tables.
            b.plot(xs, _rolling_median(list(ys), ROLLING), color=c, linestyle=ls, marker=m,
                   markevery=5, markerfacecolor=face, markeredgewidth=1.1, linewidth=1.3)
        e_all = [t for wl in ("skewed", "uniform") for _, t in d2[ds]["eager"].get(wl, [])]
        if e_all:
            b.axhline(statistics.median(e_all), color=c, linestyle=":", linewidth=0.9, alpha=0.9)
    b.set_yscale("log")
    b.set_xlabel("Query position")
    b.set_ylabel(f"Latency (s), rolling median of {ROLLING}")
    b.text(-0.13, 1.0, "(b)", transform=b.transAxes, fontweight="bold", va="bottom")

    handles = [matplotlib.lines.Line2D([], [], color=DS_COLOR[d], marker=DS_MARKER[d],
                                       markerfacecolor=SURFACE, label=LABEL[d]) for d in dss]
    handles += [
        matplotlib.lines.Line2D([], [], color=TEXT, linestyle="-", label="skewed"),
        matplotlib.lines.Line2D([], [], color=TEXT, linestyle="--", label="uniform"),
        matplotlib.lines.Line2D([], [], color=TEXT2, linestyle=":", label="eager SIESTA, case index (all pairs)"),
    ]
    fig.legend(handles=handles, loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=5,
               columnspacing=1.2, handlelength=2.2)
    fig.tight_layout()
    fig.savefig(figs / "fig3_adaptation.pdf")
    plt.close(fig)


def fig_breakeven(figs: Path, d2: dict) -> None:
    """Cumulative time (ingest + queries [+ promotions]) vs query position."""
    dss = [d for d in DATASET_ORDER if d in d2 and d2[d]["cum"]]
    if not dss:
        return
    cols = 3
    nrows = (len(dss) + cols - 1) // cols
    fig, axes = plt.subplots(nrows, cols, figsize=(7.0, 1.9 * nrows), squeeze=False)
    for i, ds in enumerate(dss):
        ax = axes[i // cols][i % cols]
        for sysname, c in (("adaptive", SERIES[0]), ("eager", SERIES[1])):
            for wl, ls in (("skewed", "-"), ("uniform", "--")):
                ys = d2[ds]["cum"].get(sysname, {}).get(wl)
                if ys:
                    ax.plot(range(len(ys)), ys, color=c, linestyle=ls, linewidth=1.3)
        ax.set_title(LABEL.get(ds, ds), loc="left")
        if i % cols == 0:
            ax.set_ylabel("Cumulative time (s)")
        if i // cols == nrows - 1:
            ax.set_xlabel("Queries answered (0 = after ingest)")
    for k in range(len(dss), nrows * cols):
        axes[k // cols][k % cols].axis("off")
    handles = [
        matplotlib.lines.Line2D([], [], color=SERIES[0], label="adaptive: ingest + queries + background (promotions, catalog writes)"),
        matplotlib.lines.Line2D([], [], color=SERIES[1], label="eager: ingest (all pairs) + queries"),
        matplotlib.lines.Line2D([], [], color=TEXT, linestyle="-", label="skewed"),
        matplotlib.lines.Line2D([], [], color=TEXT, linestyle="--", label="uniform"),
    ]
    fig.legend(handles=handles, loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=4)
    fig.tight_layout()
    fig.savefig(figs / "fig3c_breakeven.pdf")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Exp 3  ->  Fig. 4
# ---------------------------------------------------------------------------

def exp3(figs: Path, tabs: Path, panel_b_hot: int | None = None) -> None:
    """
    (a) latency vs position per hot-set size and uniform, with the
    materialisation position of each; (b) fraction PERSISTENT per round for
    one hot-set size (default: the middle one) and uniform.
    """
    recs = eval_common.read_results("exp3_hotset")
    for ds in _datasets(recs):
        rs = [r for r in recs if r["dataset"] == ds]
        if not any(r["event"] == "done" for r in rs):
            continue
        setup = next(r for r in rs if r["event"] == "setup")
        space = setup["pair_space"]
        q = [r for r in rs if r["event"] == "query"]
        rounds = [r for r in rs if r["event"] == "round"]
        hots = sorted(setup["n_hot"])
        b_hot = panel_b_hot if panel_b_hot in hots else hots[len(hots) // 2]

        fig, (a, b) = plt.subplots(1, 2, figsize=(7.0, 2.6))
        rows = []
        series = [(f"hot_{h}", SERIES[i], "-", "o",
                   f"$n_{{hot}}$ = {h} ({100 * h / space:.1f}%)") for i, h in enumerate(hots)]
        series.append(("uniform", "#8a8984", "--", "^", f"Uniform ({space} pairs)"))
        mats = []
        for wl, c, ls, m, lab in series:
            xs = sorted((r for r in q if r["workload"] == wl), key=lambda r: r["seq"])
            if not xs:
                continue
            # Skewed: the hot-set queries only (cold queries cost the cold path
            # whatever the hot-set size); uniform has no hot set, so all queries.
            shown = xs if wl == "uniform" else [r for r in xs if r["hot"]]
            a.plot([r["seq"] for r in shown], _rolling_median([r["time"] for r in shown], ROLLING),
                   color=c, linestyle=ls, marker=m, markevery=max(1, len(shown) // 12),
                   markersize=3.5, linewidth=1.4, label=lab)
            mat, n_persisted = None, None
            if wl != "uniform":
                # Materialisation: the query after which every hot pair is PERSISTENT.
                hot_pairs = {r["pair"] for r in xs if r["hot"]}
                n_hot = int(wl.split("_")[1])
                persisted: set = set()
                for r in xs:
                    for pair, st in (r.get("status_after") or {}).items():
                        if st == "PERSISTENT":
                            persisted.add(pair)
                    if mat is None and len(persisted & hot_pairs) >= n_hot:
                        mat = r["seq"]
                n_persisted = f"{len(persisted & hot_pairs)}/{n_hot}"
                if mat is not None:
                    mats.append((mat, c))
            last = max((r for r in rounds if r["workload"] == wl), key=lambda r: r["round"], default={})
            rows.append([wl, mat, n_persisted, last.get("frac_hot"), last.get("frac_cold_touched"),
                         last.get("frac_touched"), last.get("n_persistent"), last.get("n_touched")])
        top = a.get_ylim()[1]
        for mat, c in mats:
            a.axvline(mat, color=c, linestyle=":", linewidth=0.9)
        if mats:
            mat, _ = max(mats)
            a.text(mat, top, " materialisation", color=TEXT2, fontsize=6, style="italic",
                   va="top", ha="left")
        a.set_ylim(0, top)
        a.set_xlabel("Query position")
        a.set_ylabel(f"Hot-set query latency (s)\nrolling median of {ROLLING}")
        a.text(-0.12, 1.0, "(a)", transform=a.transAxes, fontweight="bold", va="bottom")
        a.legend(loc="upper center", bbox_to_anchor=(0.5, 1.22), ncol=2, fontsize=6.5)

        rb = sorted((r for r in rounds if r["workload"] == f"hot_{b_hot}"), key=lambda r: r["round"])
        ru = sorted((r for r in rounds if r["workload"] == "uniform"), key=lambda r: r["round"])
        pct = lambda v: None if v is None else 100 * v  # noqa: E731
        if rb:
            xr = [r["round"] for r in rb]
            b.plot(xr, [pct(r["frac_touched"]) for r in rb], color=SERIES[0], marker="o",
                   label=f"Skewed (overall), $n_{{hot}}$={b_hot}")
            b.plot(xr, [pct(r["frac_hot"]) for r in rb], color=SERIES[7], marker="D",
                   label="Skewed — hot")
            b.plot(xr, [pct(r["frac_cold_touched"]) for r in rb], color=SERIES[1], marker="s",
                   label="Skewed — cold")
        if ru:
            b.plot([r["round"] for r in ru], [pct(r["frac_touched"]) for r in ru], color="#8a8984",
                   linestyle="--", marker="^", label="Uniform")
        b.axhline(100, color=TEXT2, linestyle=":", linewidth=0.8)
        b.set_ylim(-3, 108)
        b.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(decimals=0))
        b.xaxis.set_major_locator(matplotlib.ticker.MaxNLocator(integer=True))
        b.set_xlabel("Query round")
        b.set_ylabel("Fraction PERSISTENT")
        b.text(-0.12, 1.0, "(b)", transform=b.transAxes, fontweight="bold", va="bottom")
        b.legend(loc="upper center", bbox_to_anchor=(0.5, 1.22), ncol=2, fontsize=6.5)
        fig.tight_layout()
        fig.savefig(figs / f"fig4_hotset_{ds}.pdf")
        plt.close(fig)
        write_table(f"tab_exp3_hotset_{ds}",
                    ["workload", "materialised at", "hot persisted", "frac hot",
                     "frac cold touched", "frac touched", "persistent", "touched"], rows, tabs,
                    f"Exp 3 ({ds}): state after the last round; panel (b) uses n_hot = {b_hot}")


# ---------------------------------------------------------------------------
# Exp 4 / 5
# ---------------------------------------------------------------------------

def _batch_times(rs, config):
    return {r["batch"]: r["time"] for r in rs if r["event"] == "batch" and r["config"] == config}


def _allpairs(rs, setup, measured):
    """
    T_allpairs per batch and how it was obtained.

    "measured": the all-pairs run exists.  Otherwise "lower_bound": the
    largest pinned run of that batch.  Maintaining a superset of pairs cannot
    be cheaper, so T_allpairs >= T_pinned_max, hence F_pair >= and
    F_persp <= the ratios computed from it.  (A line through the pinned
    sweep cannot be extrapolated: at a few hundred pairs the per-pair cost is
    below run-to-run noise, and "all" is up to three orders of magnitude more
    pairs on the large-alphabet logs.)
    """
    ap = _batch_times(rs, "allpairs")
    if ap:
        return ap, "measured"
    best = {}
    for r in rs:
        if r["event"] == "batch" and r["config"].startswith("pinned_") and r["batch"] in measured:
            n, t = r["n_pinned"], r["time"]
            if r["batch"] not in best or n > best[r["batch"]][0]:
                best[r["batch"]] = (n, t)
    return {b: t for b, (n, t) in best.items()}, "lower_bound"


def _times(r: float) -> str:
    return f"{r:.0f}x" if round(r, 1) >= 10 else f"{r:.1f}x"


def exp4(figs: Path, tabs: Path) -> None:
    recs = eval_common.read_results("exp4_maintenance")
    if not recs:
        return
    batch_rows, save_rows, persp_rows, query_rows = [], [], [], []
    for ds in _datasets(recs):
        rs = [r for r in recs if r["dataset"] == ds]
        setup = next(r for r in rs if r["event"] == "setup")
        measured = [b for b in range(1, 6)]
        eager, adaptive = _batch_times(rs, "eager"), _batch_times(rs, "adaptive")
        allp, allp_kind = _allpairs(rs, setup, measured)
        bound = allp_kind == "lower_bound"
        if not eager or not adaptive:
            continue
        for b in measured:
            both = b in eager and b in adaptive
            batch_rows.append([LABEL.get(ds, ds), f"B{b}", setup["batch_events"][b],
                               eager.get(b), adaptive.get(b),
                               (f">={allp[b]:.1f}" if bound else allp[b]) if b in allp else None,
                               eager[b] / adaptive[b] if both else None,
                               eager[b] / setup["P"] / adaptive[b] if both else None])
        for p in setup["perspectives"]:
            ts = {r["batch"]: r["time"] for r in rs if r["event"] == "ingest"
                  and r["config"] == "eager" and r.get("perspective") == p}
            persp_rows.append([LABEL.get(ds, ds), p, *[ts.get(b) for b in measured]])
        te = sum(eager.get(b, 0) for b in measured)
        ta = sum(adaptive.get(b, 0) for b in measured)
        tp = sum(allp.get(b, 0) for b in measured) if all(b in allp for b in measured) else None
        R = te / ta if ta else None
        promo = next((r for r in rs if r["event"] == "promotion"), {})
        last_cat = [r for r in rs if r["event"] == "catalog" and r.get("config") == "adaptive"]
        n_persist = sum(v["n_persistent"] for v in last_cat[-1]["summary"].values()) if last_cat else None
        save_rows.append([
            LABEL.get(ds, ds), setup["P"], setup["n_activities"],
            setup["n_hot"], setup["hot_set_total"], n_persist, promo.get("queries"),
            te, ta, (f">={tp:.0f}" if bound else tp) if tp else None, R, 100 * (1 - 1 / R) if R else None,
            (f"<={te / tp:.3g}" if bound else te / tp) if tp else None,
            (f">={tp / ta:.3g}" if bound else tp / ta) if tp and ta else None,
            "lower bound (largest pinned run)" if bound else "measured",
        ])
        for cfg in ("eager", "adaptive"):
            qs = [r["time"] for r in rs if r["event"] == "query" and r.get("config") == cfg
                  and r.get("phase", "batch") == "batch"]
            query_rows.append([LABEL.get(ds, ds), cfg, len(qs), med(qs),
                               statistics.mean(qs) if qs else None])

        # Maintenance cost vs maintained pairs (sweep + adaptive + all pairs).
        pts = defaultdict(list)
        for r in rs:
            if r["event"] == "batch" and r["batch"] in measured and (
                    r["config"].startswith("pinned_") or r["config"] == "allpairs"):
                pts[r["n_pinned"]].append(r["time"])
        if pts:
            fig, ax = plt.subplots(figsize=(3.4, 2.3))
            ns = sorted(pts)
            ax.plot(ns, [statistics.mean(pts[n]) for n in ns], color=SERIES[0], marker="o",
                    label="adaptive, n pinned pairs")
            ax.axhline(statistics.mean(eager[b] for b in measured), color=SERIES[1], linestyle="--",
                       label=f"eager ({setup['P']} perspectives)")
            ax.axhline(statistics.mean(adaptive[b] for b in measured), color=SERIES[2], linestyle=":",
                       label="adaptive, workload-driven")
            ax.set_xscale("log")
            ax.set_xlabel("Maintained pairs (all perspectives)")
            ax.set_ylabel("Mean per-batch ingest (s)")
            ax.set_title(LABEL.get(ds, ds), loc="left")
            ax.legend(loc="upper left")
            fig.savefig(figs / f"fig_exp4_pair_sweep_{ds}.pdf")
            plt.close(fig)

    write_table("tab_exp4_batches", ["dataset", "batch", "events", "eager (s)", "adaptive (s)",
                                     "all pairs (s)", "R total", "R per persp."], batch_rows, tabs,
                "Exp 4/5: absolute per-batch ingest time (server-side); "
                "R total = eager (all P perspectives) / adaptive; "
                "R per persp. = mean eager per-perspective ingest / adaptive = R total / P")
    write_table("tab_exp4_eager_perspectives", ["dataset", "perspective", "B1", "B2", "B3", "B4", "B5"],
                persp_rows, tabs, "Exp 4/5: eager per-perspective ingest seconds per batch")
    write_table("tab_exp4_savings",
                ["dataset", "P", "|A|", "hot/persp", "hot total", "persistent", "promo queries",
                 "T_eager", "T_adaptive", "T_allpairs", "R", "saving %", "F_persp", "F_pair", "T_allpairs source"],
                save_rows, tabs,
                "R = T_eager/T_adaptive = F_persp*F_pair; F_persp = T_eager/T_allpairs; "
                "F_pair = T_allpairs/T_adaptive; sums over B1-B5.  Where the all-pairs run is "
                "infeasible, T_allpairs >= the largest pinned run, giving F_persp <= and F_pair >=")
    write_table("tab_exp4_queries", ["dataset", "config", "queries", "median (s)", "mean (s)"],
                query_rows, tabs, "Exp 4: latency of the per-batch query slices")

    # Exp 5 figure: BPIC 2017.
    rs = [r for r in recs if r["dataset"] == "bpic2017"]
    if rs:
        setup = next(r for r in rs if r["event"] == "setup")
        persps = setup["perspectives"]
        measured = list(range(1, 6))
        eager = {(r["batch"], r["perspective"]): r["time"] for r in rs
                 if r["event"] == "ingest" and r["config"] == "eager"}
        e_tot, adaptive = _batch_times(rs, "eager"), _batch_times(rs, "adaptive")
        if adaptive and e_tot:
            fig, ax = plt.subplots(figsize=(3.6, 2.3))
            width = 0.8 / len(persps)
            for j, p in enumerate(persps):
                xs = [b + (j - (len(persps) - 1) / 2) * width for b in measured]
                ax.bar(xs, [eager.get((b, p), 0) for b in measured], width=width * 0.9,
                       color=SERIES[j], label=f"eager: {p}")
            ax.plot(measured, [adaptive[b] for b in measured], color=SERIES[6], linestyle="--",
                    marker="o", linewidth=1.8, label="Adaptive indexing")
            top = max(eager.values())
            P = len(persps)
            for b in measured:
                r_tot = e_tot[b] / adaptive[b]
                ax.text(b, top * 1.13, _times(r_tot), ha="center", color=TEXT, fontsize=6.5)
                ax.text(b, top * 1.04, _times(r_tot / P), ha="center", color=TEXT2, fontsize=6.5)
            ax.set_xticks(measured)
            ax.set_xticklabels([f"B{b}" for b in measured])
            ax.set_ylabel("Incremental ingest time (s)")
            ax.set_ylim(0, top * 1.25)
            ax.legend(ncol=3, fontsize=6, loc="lower center", bbox_to_anchor=(0.5, 1.02),
                      columnspacing=1.0, handlelength=1.6)
            ax.text(0.5, -0.2, f"above bars — upper: eager total ({P} perspectives) / adaptive;  "
                               "lower: eager per perspective / adaptive",
                    transform=ax.transAxes, ha="center", color=TEXT2, fontsize=5.8)
            fig.savefig(figs / "fig_exp5_bpic2017.pdf")
            plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", type=Path, default=eval_common.RESULTS_DIR)
    args = ap.parse_args()
    eval_common.RESULTS_DIR = args.results
    figs, tabs = args.results / "figures", args.results / "tables"
    figs.mkdir(parents=True, exist_ok=True)
    tabs.mkdir(parents=True, exist_ok=True)
    d2 = exp2(tabs)
    fig3(figs, exp1(tabs), d2)
    fig_breakeven(figs, d2)
    exp3(figs, tabs)
    exp4(figs, tabs)
    print(f"figures -> {figs}\ntables  -> {tabs}")


if __name__ == "__main__":
    main()
