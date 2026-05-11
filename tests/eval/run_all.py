"""
tests/eval/run_all.py

Master evaluation runner.

Runs all experiments in order, checks API health before each one, and
writes a summary manifest to tests/eval/results/manifest.json.

Dataset configuration
---------------------
The dataset directory and specific file to use can be set in three ways,
from highest to lowest precedence:

  1. CLI arguments (--dataset, --log-name)
  2. Environment variables (EVAL_DATASET, EVAL_LOG_NAME)
  3. Defaults hard-coded in eval_common.py
     (DEFAULT_DATASET = datasets/test.xes, DEFAULT_LOG_NAME = test)

The --dataset-dir flag provides a shorthand for pointing at a different
base directory — it sets EVAL_DATASET to <dir>/test.xes and EVAL_LOG_NAME
to the directory's basename, unless those are already set explicitly.

For real-dataset runs the recommended invocation is::

    python tests/eval/run_all.py \\
        --dataset datasets/bpic_2017.xes \\
        --log-name bpic_2017

Or equivalently via environment variables::

    EVAL_DATASET=datasets/bpic_2017.xes \\
    EVAL_LOG_NAME=bpic_2017 \\
    python tests/eval/run_all.py

Usage
-----
    # Run all experiments against the default test dataset:
    python tests/eval/run_all.py

    # Run with a real-world dataset:
    python tests/eval/run_all.py \\
        --dataset datasets/bpic_2017.xes --log-name bpic_2017

    # Run only specific experiments:
    python tests/eval/run_all.py --experiments 6.3.1 6.3.2

    # Dry-run: print the execution plan without running:
    python tests/eval/run_all.py --dry-run

    # Run the correctness equivalence check before experiments:
    python tests/eval/run_all.py --check-equivalence

Execution order
---------------
Experiments are run in the order listed in EXPERIMENTS.  6.3.1 (warm-up)
is placed first because 6.4 (external) reuses the index state left by
6.3.1 when measuring steady-state adaptive latency.  All other experiments
reset state via overwrite_data=True so their order is flexible.

Output
------
Each experiment writes its own JSONL file to tests/eval/results/.
The master manifest (manifest.json) records status, elapsed time, and
output path for each experiment run.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
import time
from pathlib import Path

# Allow running from any working directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.eval.eval_common import (
    RESULTS_DIR, API_BASE,
    health_check, resolve_dataset,
)


# ---------------------------------------------------------------------------
# Experiment registry
# ---------------------------------------------------------------------------

EXPERIMENTS: list[tuple[str, str, str]] = [
    # (label, module, description)
    ("6.3.1", "tests.eval.exp_warmup",
     "Query performance under adaptive indexing (warm-up curves)"),
    ("6.3.2", "tests.eval.exp_maintenance",
     "Index maintenance cost vs workload coverage"),
    ("6.3.3", "tests.eval.exp_retention",
     "Retention policy sensitivity (half-life + hysteresis ablations)"),
    ("6.3.5", "tests.eval.exp_l2_cost",
     "Positional annotation (L2) cost"),
    ("6.4",   "tests.eval.exp_external",
     "External comparison (adaptive vs SIESTA / ELK)"),
]

EQUIVALENCE_MODULE = "tests.eval.equivalence"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _inject_dataset_env(dataset: str | None, log_name: str | None) -> None:
    """
    Set EVAL_DATASET and EVAL_LOG_NAME so that every experiment script
    (which calls resolve_dataset() internally) picks up the configured
    dataset without needing separate per-experiment plumbing.
    """
    if dataset:
        os.environ["EVAL_DATASET"] = dataset
    if log_name:
        os.environ["EVAL_LOG_NAME"] = log_name


def _run_experiment(
    label: str,
    module_name: str,
    description: str,
    dry_run: bool,
    extra_args: list[str],
) -> dict:
    """Import and run one experiment.  Returns a manifest record."""
    print(f"\n{'='*64}")
    print(f"  {label}  —  {description}")
    print(f"{'='*64}")

    if dry_run:
        print(f"  [DRY RUN] would import {module_name} and call main().")
        return {"experiment": label, "status": "dry_run", "description": description}

    try:
        health_check()
    except Exception as exc:
        print(f"  [SKIP] API not reachable: {exc}")
        return {
            "experiment":  label,
            "status":      "skipped",
            "description": description,
            "reason":      str(exc),
        }

    # Inject extra CLI args so each experiment's argparse sees them.
    saved_argv = sys.argv[:]
    sys.argv = [module_name] + extra_args

    t0 = time.time()
    try:
        mod = importlib.import_module(module_name)
        mod.main()
        elapsed = time.time() - t0
        output  = str(RESULTS_DIR / f"{label.replace('.', '_')}.jsonl")
        print(f"\n  ✓ {label} completed in {elapsed:.1f}s  →  {output}")
        return {
            "experiment":  label,
            "status":      "ok",
            "description": description,
            "elapsed_s":   round(elapsed, 1),
            "output":      output,
        }
    except SystemExit as exc:
        elapsed = time.time() - t0
        code    = exc.code
        # SystemExit(0) from argparse --help is not a real failure.
        if code == 0:
            return {"experiment": label, "status": "ok",
                    "description": description, "elapsed_s": round(elapsed, 1)}
        print(f"\n  ✗ {label} exited with code {code} after {elapsed:.1f}s")
        return {
            "experiment":  label,
            "status":      "failed",
            "description": description,
            "elapsed_s":   round(elapsed, 1),
            "error":       f"SystemExit({code})",
        }
    except Exception as exc:
        elapsed = time.time() - t0
        import traceback
        traceback.print_exc()
        print(f"\n  ✗ {label} FAILED after {elapsed:.1f}s: {exc}")
        return {
            "experiment":  label,
            "status":      "failed",
            "description": description,
            "elapsed_s":   round(elapsed, 1),
            "error":       str(exc),
        }
    finally:
        sys.argv = saved_argv
        # Unload the module so re-imports pick up the injected env vars.
        sys.modules.pop(module_name, None)


def _run_equivalence(dry_run: bool, extra_args: list[str]) -> dict:
    """Run the correctness equivalence check."""
    print(f"\n{'='*64}")
    print("  Equivalence check  —  adaptive vs eager result sets")
    print(f"{'='*64}")

    if dry_run:
        print("  [DRY RUN] would run equivalence.main().")
        return {"experiment": "equivalence", "status": "dry_run"}

    try:
        health_check()
    except Exception as exc:
        return {"experiment": "equivalence", "status": "skipped", "reason": str(exc)}

    saved_argv = sys.argv[:]
    sys.argv = [EQUIVALENCE_MODULE] + extra_args
    t0 = time.time()
    try:
        mod  = importlib.import_module(EQUIVALENCE_MODULE)
        rc   = mod.main()
        elapsed = time.time() - t0
        status = "ok" if rc == 0 else "failed"
        print(f"\n  {'✓' if rc == 0 else '✗'} equivalence check "
              f"{'PASS' if rc == 0 else 'FAIL'} in {elapsed:.1f}s")
        return {"experiment": "equivalence", "status": status,
                "elapsed_s": round(elapsed, 1)}
    except Exception as exc:
        elapsed = time.time() - t0
        return {"experiment": "equivalence", "status": "failed",
                "elapsed_s": round(elapsed, 1), "error": str(exc)}
    finally:
        sys.argv = saved_argv
        sys.modules.pop(EQUIVALENCE_MODULE, None)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__.split("\n")[2],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # ── Dataset configuration (highest-level knobs) ──────────────────────
    ds_group = ap.add_argument_group("Dataset configuration")
    ds_group.add_argument(
        "--dataset", default=None, metavar="PATH",
        help="Path to dataset file (CSV or XES). Overrides EVAL_DATASET env var. "
             "Relative paths are resolved from the repository root.",
    )
    ds_group.add_argument(
        "--log-name", default=None, metavar="NAME",
        help="Log name used in API calls and storage. Overrides EVAL_LOG_NAME.",
    )
    ds_group.add_argument(
        "--dataset-dir", default=None, metavar="DIR",
        help="Shorthand: set the base dataset directory. Sets EVAL_DATASET to "
             "<dir>/test.xes and EVAL_LOG_NAME to the dir basename, unless "
             "--dataset / --log-name are already provided.",
    )

    # ── Experiment selection ──────────────────────────────────────────────
    sel_group = ap.add_argument_group("Experiment selection")
    sel_group.add_argument(
        "--experiments", nargs="*", metavar="LABEL",
        help="Subset of experiment labels to run "
             "(e.g. 6.3.1 6.3.2).  Omit to run all.",
    )
    sel_group.add_argument(
        "--check-equivalence", action="store_true",
        help="Run the correctness equivalence check before experiments.",
    )
    sel_group.add_argument(
        "--dry-run", action="store_true",
        help="Print the execution plan without running anything.",
    )

    # ── Per-experiment pass-throughs ─────────────────────────────────────
    pt_group = ap.add_argument_group("Per-experiment options (passed through)")
    pt_group.add_argument(
        "--skip-elk", action="store_true",
        help="Skip the ELK baseline in exp_external (6.4).",
    )
    pt_group.add_argument(
        "--reps", type=int, default=None,
        help="Override REPS_PER_CATEGORY in exp_warmup (6.3.1).",
    )

    args = ap.parse_args()

    # ── Resolve dataset configuration ────────────────────────────────────
    dataset  = args.dataset
    log_name = args.log_name

    if args.dataset_dir and not dataset:
        d = Path(args.dataset_dir)
        # Look for .xes first, then .csv.
        for ext in (".xes", ".csv"):
            candidate = d / f"test{ext}"
            if candidate.exists():
                dataset = str(candidate)
                break
        if log_name is None:
            log_name = d.name

    _inject_dataset_env(dataset, log_name)

    # Validate dataset resolution early so we fail before running anything.
    try:
        spec = resolve_dataset(dataset, log_name)
    except FileNotFoundError as exc:
        ap.error(str(exc))
        return

    print(f"\nSiesta Adaptive Indexing — Evaluation Suite")
    print(f"  API:      {API_BASE}")
    print(f"  Dataset:  {spec.path}")
    print(f"  Log name: {spec.log_name}")
    print(f"  Results:  {RESULTS_DIR}")

    # ── Build per-experiment extra args ───────────────────────────────────
    shared_dataset_args: list[str] = []
    if dataset:
        shared_dataset_args += ["--dataset", dataset]
    if log_name:
        shared_dataset_args += ["--log-name", log_name]

    extra_for: dict[str, list[str]] = {
        label: list(shared_dataset_args) for label, _, _ in EXPERIMENTS
    }
    if args.skip_elk:
        extra_for.setdefault("6.4", []).append("--skip-elk")
    if args.reps is not None:
        extra_for.setdefault("6.3.1", []).extend(["--reps", str(args.reps)])

    # Equivalence check also accepts --dataset / --log-name.
    equiv_args = list(shared_dataset_args)

    # ── Print execution plan ─────────────────────────────────────────────
    selected = set(args.experiments) if args.experiments else None
    planned  = [
        (label, mod, desc)
        for label, mod, desc in EXPERIMENTS
        if selected is None or label in selected
    ]

    print(f"\nPlanned experiments ({len(planned)}):")
    if args.check_equivalence:
        print("  [pre]  equivalence check")
    for label, _, desc in planned:
        marker = "[DRY]" if args.dry_run else "     "
        print(f"  {marker} {label}  {desc}")

    if args.dry_run:
        print("\n(dry-run: nothing will execute)")

    # ── Execute ───────────────────────────────────────────────────────────
    manifest: dict = {
        "api_base":   API_BASE,
        "dataset":    str(spec.path),
        "log_name":   spec.log_name,
        "experiments": [],
    }

    if args.check_equivalence:
        result = _run_equivalence(args.dry_run, equiv_args)
        manifest["experiments"].append(result)
        if result.get("status") == "failed" and not args.dry_run:
            print("\n⚠  Equivalence check failed — proceeding with experiments anyway.")

    for label, module_name, description in planned:
        result = _run_experiment(
            label, module_name, description,
            dry_run=args.dry_run,
            extra_args=extra_for.get(label, shared_dataset_args),
        )
        manifest["experiments"].append(result)

    # ── Write manifest ────────────────────────────────────────────────────
    manifest_path = RESULTS_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\nManifest written to {manifest_path}")

    # ── Summary ───────────────────────────────────────────────────────────
    failed  = [e for e in manifest["experiments"] if e.get("status") == "failed"]
    skipped = [e for e in manifest["experiments"] if e.get("status") == "skipped"]

    print(f"\n{'─'*64}")
    print(f"  Total:    {len(manifest['experiments'])}")
    print(f"  OK:       {sum(1 for e in manifest['experiments'] if e.get('status') == 'ok')}")
    print(f"  Failed:   {len(failed)}")
    print(f"  Skipped:  {len(skipped)}")
    print(f"{'─'*64}")

    if failed:
        print("\nFailed experiments:")
        for e in failed:
            print(f"  {e['experiment']}: {e.get('error', '?')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
