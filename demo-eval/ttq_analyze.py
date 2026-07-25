#!/usr/bin/env python3
"""
ttq_analyze.py
==============
Aggregate per-batch Time-to-Queryable (TTQ) rows into a per-lambda summary and
plot avg / p95-tail TTQ vs injection rate lambda, marking the "knee" -- the
smallest lambda at which the per-run backlog (tail TTQ vs batch index) starts
trending significantly upward, i.e. where consumption falls behind production.

Input : ttq_results.csv  (lambda,batch_id,n_events,avg_ttq,tail_ttq,commit_time)
Output: ttq_summary.csv  (lambda,n_batches,avg_ttq,p95_tail_ttq,backlog_slope,is_knee)
        ttq_plot.png
"""

import argparse
import csv
import os
from collections import defaultdict

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Okabe-Ito colourblind-safe hues (magnitude of two related latency series).
C_AVG = "#0072B2"   # blue   -> average TTQ
C_TAIL = "#D55E00"  # vermilion -> p95 tail TTQ
C_KNEE = "#444444"
INK = "#222222"
GRID = "#DDDDDD"


def load_rows(path):
    by_lambda = defaultdict(list)
    with open(path) as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            try:
                lam = float(r["lambda"])
                by_lambda[lam].append({
                    "batch_id": int(float(r["batch_id"])),
                    "avg_ttq": float(r["avg_ttq"]),
                    "tail_ttq": float(r["tail_ttq"]),
                    "commit_time": float(r["commit_time"]),
                })
            except (KeyError, ValueError):
                continue
    return by_lambda


def summarize(by_lambda):
    rows = []
    for lam in sorted(by_lambda):
        recs = sorted(by_lambda[lam], key=lambda x: x["commit_time"])
        avg = np.array([r["avg_ttq"] for r in recs], dtype=float)
        tail = np.array([r["tail_ttq"] for r in recs], dtype=float)
        # Backlog trend: slope of tail TTQ over batch order within this lambda.
        # A sustainable rate holds tail ~flat (slope ~ 0); once the consumer
        # falls behind, each successive batch is staler -> positive slope.
        if len(tail) >= 3:
            x = np.arange(len(tail), dtype=float)
            slope = float(np.polyfit(x, tail, 1)[0])
        else:
            slope = float("nan")
        rows.append({
            "lambda": lam,
            "n_batches": len(recs),
            "avg_ttq": float(avg.mean()) if len(avg) else float("nan"),
            "p95_tail_ttq": float(np.percentile(tail, 95)) if len(tail) else float("nan"),
            "backlog_slope": slope,
        })
    return rows


def find_knee(rows, knee_slope):
    """First lambda (ascending) whose backlog slope exceeds the threshold."""
    for r in rows:
        s = r["backlog_slope"]
        if not np.isnan(s) and s > knee_slope:
            return r["lambda"]
    return None


def write_summary(rows, knee, out_csv):
    with open(out_csv, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["lambda", "n_batches", "avg_ttq", "p95_tail_ttq", "backlog_slope", "is_knee"])
        for r in rows:
            w.writerow([
                f"{r['lambda']:g}", r["n_batches"],
                f"{r['avg_ttq']:.4f}", f"{r['p95_tail_ttq']:.4f}",
                "" if np.isnan(r["backlog_slope"]) else f"{r['backlog_slope']:.5f}",
                "yes" if (knee is not None and r["lambda"] == knee) else "",
            ])


def plot(rows, knee, out_png):
    lam = np.array([r["lambda"] for r in rows], dtype=float)
    avg = np.array([r["avg_ttq"] for r in rows], dtype=float)
    p95 = np.array([r["p95_tail_ttq"] for r in rows], dtype=float)

    fig, ax = plt.subplots(figsize=(7.2, 4.6), dpi=140)
    ax.plot(lam, avg, "-o", color=C_AVG, lw=2, ms=7, label="avg TTQ", zorder=3)
    ax.plot(lam, p95, "-s", color=C_TAIL, lw=2, ms=7, label="p95 tail TTQ", zorder=3)

    # Direct-label the two series at their right-most point.
    if len(lam):
        ax.annotate("avg", (lam[-1], avg[-1]), color=C_AVG, fontsize=9,
                    xytext=(6, 0), textcoords="offset points", va="center")
        ax.annotate("p95 tail", (lam[-1], p95[-1]), color=C_TAIL, fontsize=9,
                    xytext=(6, 0), textcoords="offset points", va="center")

    if knee is not None:
        ax.axvline(knee, color=C_KNEE, ls="--", lw=1.4, zorder=2)
        ymax = np.nanmax([np.nanmax(avg) if len(avg) else 0,
                          np.nanmax(p95) if len(p95) else 0])
        ax.annotate(f"knee  λ≈{knee:g} ev/s",
                    (knee, ymax), color=C_KNEE, fontsize=9, fontweight="bold",
                    xytext=(6, -4), textcoords="offset points", va="top")

    ax.set_xlabel("injection rate λ (events/s)", color=INK)
    ax.set_ylabel("time-to-queryable (s)", color=INK)
    ax.set_title("Streaming ingestion TTQ vs. injection rate (BPIC 2017)", color=INK)
    ax.grid(True, color=GRID, lw=0.8, zorder=0)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.tick_params(colors=INK)
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False, loc="upper left")
    fig.tight_layout()
    fig.savefig(out_png, bbox_inches="tight")
    print(f"wrote {out_png}")


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="Aggregate + plot TTQ sweep results")
    ap.add_argument("--results", default=os.path.join("demo-eval", "ttq_results.csv"))
    ap.add_argument("--summary", default=os.path.join("demo-eval", "ttq_summary.csv"))
    ap.add_argument("--plot", default=os.path.join("demo-eval", "ttq_plot.png"))
    ap.add_argument("--knee-slope", type=float, default=0.05,
                    help="tail-TTQ slope (s per batch) above which a lambda is the knee")
    args = ap.parse_args()

    repo_root = os.path.dirname(here)
    results = args.results if os.path.isabs(args.results) else os.path.join(repo_root, args.results)
    summary = args.summary if os.path.isabs(args.summary) else os.path.join(repo_root, args.summary)
    plot_path = args.plot if os.path.isabs(args.plot) else os.path.join(repo_root, args.plot)

    if not os.path.exists(results):
        raise SystemExit(f"results file not found: {results}")

    by_lambda = load_rows(results)
    if not by_lambda:
        raise SystemExit(f"no usable rows in {results}")
    rows = summarize(by_lambda)
    knee = find_knee(rows, args.knee_slope)

    write_summary(rows, knee, summary)
    print(f"wrote {summary}")
    print(f"{'lambda':>8} {'n':>5} {'avg_ttq':>9} {'p95_tail':>9} {'slope':>9}  knee")
    for r in rows:
        mark = "  <-- knee" if (knee is not None and r["lambda"] == knee) else ""
        s = "nan" if np.isnan(r["backlog_slope"]) else f"{r['backlog_slope']:.4f}"
        print(f"{r['lambda']:>8g} {r['n_batches']:>5d} {r['avg_ttq']:>9.3f} "
              f"{r['p95_tail_ttq']:>9.3f} {s:>9}{mark}")
    if knee is None:
        print("no knee detected (no lambda exceeded the backlog-slope threshold)")

    plot(rows, knee, plot_path)


if __name__ == "__main__":
    main()
