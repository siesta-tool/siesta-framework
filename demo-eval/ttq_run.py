#!/usr/bin/env python3
"""
ttq_run.py
==========
Time-to-Queryable (TTQ) sweep orchestrator.

For each injection rate ``lambda`` it starts a *fresh* streaming indexer (cleared
tables + checkpoints, ``startingOffsets: latest`` so prior topic backlog is
ignored), lets it warm up, replays BPIC2017 into Kafka at rate lambda for
``--duration`` via ttq_producer.py, drains the last micro-batches, and collects
the per-lambda ``ttq_batch`` rows the indexer appended to its ``ttq_log_path``.
Finally it merges everything into ttq_results.csv and runs ttq_analyze.py.

Two driver backends (the *indexer* runs on whichever; the *producer* always runs
on the host and sends to --bootstrap-servers):

  --driver api   (default)  Drive the containerised SIESTA API (the deployment's
                            known-good path: driver Python matches the Spark
                            cluster). Per lambda it restarts the API container so
                            the indexer starts clean and picks up code changes,
                            then POSTs the streaming index config to
                            /indexing/run. ttq_log_path is written under
                            /workspace (bind-mounted back to the host).

  --driver host             Launch `python main.py indexer` on the host as a
                            subprocess. Only valid when the host Python/Spark
                            match the cluster's executors; otherwise PySpark UDFs
                            fail to deserialise.

Start with a smoke run:
    python demo-eval/ttq_run.py --smoke
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
DEFAULT_SWEEP = [250, 500, 1000, 1500, 2000, 2500, 3000]


def _default_python():
    venv_py = os.path.join(REPO_ROOT, "venv", "bin", "python")
    return venv_py if os.path.exists(venv_py) else sys.executable


def log(msg):
    print(f"[ttq_run {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def build_lambda_config(base_cfg, lam, ttq_log_path):
    cfg = json.loads(json.dumps(base_cfg))  # deep copy
    cfg["clear_existing"] = True
    cfg["enable_streaming"] = True
    cfg["ttq_logging"] = True
    cfg["ttq_lambda"] = lam
    cfg["ttq_log_path"] = ttq_log_path
    return cfg


# ─────────────────────────── host driver ────────────────────────────
def host_start_indexer(python, sys_config, cfg, lam, log_path):
    cfg_path = os.path.join(HERE, f".ttq_index_lambda{int(lam)}.json")
    with open(cfg_path, "w") as fh:
        json.dump(cfg, fh, indent=2)
    cmd = [python, "main.py", "--config", sys_config, "indexer", "--index_config", cfg_path]
    log(f"[host] start indexer: {' '.join(cmd)}")
    logf = open(log_path, "w")
    proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=logf, stderr=subprocess.STDOUT,
                            start_new_session=True)
    proc._logf = logf
    return proc


def host_stop_indexer(proc, grace=25):
    if proc.poll() is not None:
        return
    log("[host] stopping indexer (SIGINT -> group)")
    for sig, wait in ((signal.SIGINT, grace), (signal.SIGTERM, 10), (signal.SIGKILL, 5)):
        try:
            os.killpg(os.getpgid(proc.pid), sig)
        except ProcessLookupError:
            break
        t0 = time.time()
        while proc.poll() is None and time.time() - t0 < wait:
            time.sleep(1)
        if proc.poll() is not None:
            break
    try:
        proc._logf.close()
    except Exception:
        pass


# ─────────────────────────── api driver ─────────────────────────────
def wait_for_api(endpoint, timeout=120):
    import requests
    t0 = time.time()
    while time.time() - t0 < timeout:
        for path in ("/docs", "/"):
            try:
                r = requests.get(endpoint + path, timeout=5)
                if r.status_code < 500:
                    return True
            except requests.exceptions.RequestException:
                pass
        time.sleep(2)
    return False


def api_restart_container(container):
    log(f"[api] docker restart {container}")
    rc = subprocess.call(["docker", "restart", container],
                         stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    if rc != 0:
        raise RuntimeError(f"docker restart {container} failed (rc={rc})")


def api_start_indexer(endpoint, cfg):
    """POST the streaming index config to /indexing/run (multipart form field)."""
    import requests
    log("[api] POST /indexing/run (enable_streaming)")
    r = requests.post(endpoint + "/indexing/run",
                      data={"index_config": json.dumps(cfg)}, timeout=180)
    r.raise_for_status()
    body = r.json()
    log(f"[api] indexer response: {body}")
    code = body.get("code", 200)
    if code != 200:
        raise RuntimeError(f"indexer start returned code {code}: {body.get('message')}")


# ─────────────────────────── producer ───────────────────────────────
def run_producer(python, bootstrap, topic, lam, duration, dataset, log_path):
    cmd = [python, os.path.join("demo-eval", "ttq_producer.py"),
           "--bootstrap-servers", bootstrap, "--topic", topic,
           "--rate", str(lam), "--duration", str(duration)]
    if dataset:
        cmd += ["--dataset", dataset]
    log(f"replay lambda={lam:g} for {duration}s")
    with open(log_path, "w") as logf:
        rc = subprocess.call(cmd, cwd=REPO_ROOT, stdout=logf, stderr=subprocess.STDOUT)
    if rc != 0:
        log(f"WARNING: producer exited rc={rc} (see {os.path.relpath(log_path, REPO_ROOT)})")


# ─────────────────────────── merge / drive ──────────────────────────
def merge_results(per_lambda_csvs, out_csv, warmup_batches):
    header = "lambda,batch_id,n_events,avg_ttq,tail_ttq,commit_time"
    rows = []
    for lam, csv_path in per_lambda_csvs:
        if not os.path.exists(csv_path):
            log(f"NOTE: no ttq rows for lambda={lam} (missing {os.path.basename(csv_path)})")
            continue
        with open(csv_path) as fh:
            lines = [ln.strip() for ln in fh if ln.strip()]
        data = lines[1:] if lines and lines[0].startswith("lambda,") else lines
        kept = data[warmup_batches:]
        if not kept:
            log(f"NOTE: lambda={lam} produced {len(data)} batch(es) <= warmup "
                f"({warmup_batches}); none kept")
        rows.extend(kept)
    with open(out_csv, "w") as fh:
        fh.write(header + "\n")
        for r in rows:
            fh.write(r + "\n")
    log(f"merged {len(rows)} batch rows -> {os.path.relpath(out_csv, REPO_ROOT)}")
    return len(rows)


def main():
    ap = argparse.ArgumentParser(description="TTQ sweep orchestrator")
    ap.add_argument("--driver", choices=["api", "host"], default="api")
    # api driver
    ap.add_argument("--endpoint", default="http://localhost:8000")
    ap.add_argument("--container", default="siesta-api")
    ap.add_argument("--no-restart", action="store_true",
                    help="api: don't restart the container between lambdas "
                         "(faster, but tables accumulate across lambdas)")
    ap.add_argument("--workspace-prefix", default="/workspace",
                    help="api: container path that maps to the repo root")
    # host driver
    ap.add_argument("--config", default=os.path.join("config", "siesta.config.json"))
    ap.add_argument("--python", default=_default_python())
    # common
    ap.add_argument("--index-config", default=os.path.join("demo-eval", "ttq_index.config.json"))
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--dataset", default=None)
    ap.add_argument("--sweep", default=None,
                    help="comma-separated lambdas (default 250,500,1000,1500,2000,2500,3000)")
    ap.add_argument("--duration", type=float, default=300.0)
    ap.add_argument("--warmup", type=float, default=45.0)
    ap.add_argument("--drain", type=float, default=30.0)
    ap.add_argument("--warmup-batches", type=int, default=2)
    ap.add_argument("--out", default=os.path.join("demo-eval", "ttq_results.csv"))
    ap.add_argument("--smoke", action="store_true",
                    help="single short run (lambda=500, 60s, warmup 30s, drain 20s)")
    ap.add_argument("--no-analyze", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.smoke:
        sweep = [500]
        args.duration, args.warmup, args.drain, args.warmup_batches = 60.0, 30.0, 20.0, 0
    elif args.sweep:
        sweep = [float(x) for x in args.sweep.split(",") if x.strip()]
    else:
        sweep = list(DEFAULT_SWEEP)

    with open(os.path.join(REPO_ROOT, args.index_config)) as fh:
        base_cfg = json.load(fh)
    topic = base_cfg.get("kafka_topic", "ttq_bpic2017")
    logdir = os.path.join(HERE, "ttq_logs")
    os.makedirs(logdir, exist_ok=True)

    est = sum((args.warmup + args.duration + args.drain + 25) for _ in sweep) / 60.0
    log(f"driver={args.driver} sweep={sweep} topic={topic} "
        f"duration={args.duration}s warmup={args.warmup}s drain={args.drain}s (~{est:.0f} min)")
    if args.dry_run:
        log("dry-run: exiting before launching anything")
        return

    # The API driver runs as the container user (uid 1001), which can write
    # /workspace/output but not the host-owned demo-eval dir; the host driver
    # writes as the invoking user, so demo-eval is fine there.
    coll_dir = os.path.join(REPO_ROOT, "output") if args.driver == "api" else HERE

    per_lambda_csvs = []
    for lam in sweep:
        fname = f"ttq_batches_lambda{int(lam)}.csv"
        host_csv = os.path.join(coll_dir, fname)
        per_lambda_csvs.append((lam, host_csv))

        # ttq_log_path as seen by the indexer driver, and stale-file cleanup.
        if args.driver == "api":
            ttq_log_path = os.path.join(args.workspace_prefix, "output", fname)
            # host user can't delete in the container-owned output dir; use exec.
            subprocess.call(["docker", "exec", args.container, "rm", "-f", ttq_log_path],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            ttq_log_path = host_csv
            if os.path.exists(host_csv):
                os.remove(host_csv)
        cfg = build_lambda_config(base_cfg, lam, ttq_log_path)

        idx_log = os.path.join(logdir, f"indexer_lambda{int(lam)}.log")
        prod_log = os.path.join(logdir, f"producer_lambda{int(lam)}.log")
        proc = None
        try:
            if args.driver == "api":
                if not args.no_restart:
                    api_restart_container(args.container)
                    if not wait_for_api(args.endpoint):
                        log(f"ERROR: API not healthy after restart; skipping lambda={lam}")
                        continue
                api_start_indexer(args.endpoint, cfg)
            else:
                proc = host_start_indexer(args.python, os.path.join(REPO_ROOT, args.config),
                                          cfg, lam, idx_log)

            log(f"warm-up {args.warmup}s ...")
            time.sleep(args.warmup)
            if args.driver == "host" and proc.poll() is not None:
                log(f"ERROR: host indexer exited during warm-up (rc={proc.returncode}); "
                    f"see {os.path.relpath(idx_log, REPO_ROOT)}. Skipping lambda={lam}.")
                continue

            run_producer(args.python, args.bootstrap_servers, topic, lam,
                         args.duration, args.dataset, prod_log)
            log(f"drain {args.drain}s ...")
            time.sleep(args.drain)
        finally:
            if args.driver == "host" and proc is not None:
                host_stop_indexer(proc)

        n = (len(open(host_csv).readlines()) - 1) if os.path.exists(host_csv) else 0
        log(f"lambda={lam}: collected {max(n, 0)} ttq_batch rows")

    n_rows = merge_results(per_lambda_csvs, os.path.join(REPO_ROOT, args.out), args.warmup_batches)
    if n_rows and not args.no_analyze:
        log("running analysis ...")
        subprocess.call([args.python, os.path.join("demo-eval", "ttq_analyze.py"),
                         "--results", args.out], cwd=REPO_ROOT)
    log("done.")


if __name__ == "__main__":
    main()
