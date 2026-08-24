#!/usr/bin/env python3
"""
ttq_sanity.py
=============
One-time commit-visibility sanity check (spec step 3, NOT part of the sweep).

After a micro-batch has committed, fire a single detection query for a pattern
known to exist in the replayed BPIC 2017 events and confirm the Query Executor
returns it within milliseconds of commit -- i.e. Delta's commit-visibility
guarantee holds for the read path that TTQ claims queryability against.

Run this once against a live indexed ttq_bpic2017 log (e.g. after a smoke run,
before tearing the indexer down), not per lambda.

    python demo-eval/ttq_sanity.py --pattern "A_Create Application A_Submitted"
"""

import argparse
import time

import requests

# Activity names contain spaces, so they must be double-quoted DSL string tokens
# (bare tokens are split on whitespace). This pair opens virtually every trace.
DEFAULT_PATTERN = '"A_Create Application" "A_Submitted"'


def main():
    ap = argparse.ArgumentParser(description="TTQ commit-visibility sanity check")
    ap.add_argument("--endpoint", default="http://localhost:8000")
    ap.add_argument("--log-name", default="ttq_bpic2017")
    ap.add_argument("--namespace", default="siesta")
    ap.add_argument("--pattern", default=DEFAULT_PATTERN)
    args = ap.parse_args()

    payload = {
        "log_name": args.log_name,
        "storage_namespace": args.namespace,
        "query": {"pattern": args.pattern},
    }
    t0 = time.perf_counter()
    r = requests.post(f"{args.endpoint}/querying/detection",
                      headers={"accept": "application/json",
                               "Content-Type": "application/json"},
                      json=payload, timeout=120)
    net = time.perf_counter() - t0
    r.raise_for_status()
    resp = r.json()
    total = resp.get("total", 0)
    backend = resp.get("time", 0.0)
    print(f"pattern   : {args.pattern!r}")
    print(f"matches   : {total}")
    print(f"backend   : {backend*1000:.1f} ms   (query executor read+detect)")
    print(f"round-trip: {net*1000:.1f} ms")
    if total > 0:
        print("OK: committed batch is visible to the detection query path.")
    else:
        print("NOTE: 0 matches -- pick a pattern present in the replayed events, "
              "or wait for a batch containing it to commit.")


if __name__ == "__main__":
    main()
