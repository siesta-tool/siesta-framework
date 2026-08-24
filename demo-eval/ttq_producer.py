#!/usr/bin/env python3
"""
ttq_producer.py
===============
Time-to-Queryable (TTQ) replay producer.

Replays the BPIC 2017 event log into a Kafka topic at a controlled injection
rate ``lambda`` (events/second) for a fixed duration, attaching to every record
a wall-clock send marker ``_produced_at`` (``time.time()`` at the instant the
record is handed to the Kafka producer) and a ``_lambda`` tag carrying the
target rate.

Why a separate marker: the BPIC timestamps are *historical replay* values, not
real send time, so they cannot be used to measure ingestion latency. The
consumer side (SIESTA indexer) reads ``_produced_at`` back out of the event's
``attributes`` map after the CountTable commit and logs the per-batch TTQ.

For ``_produced_at`` / ``_lambda`` to survive the streaming Kafka->JSON parse
they must be listed as explicit ``attributes`` in the indexer's ``json`` field
mapping -- see demo-eval/ttq_index.config.json.

Usage
-----
    # replay at 1000 ev/s for 5 minutes into topic ttq_bpic2017
    python demo-eval/ttq_producer.py --rate 1000 --duration 300 --topic ttq_bpic2017

    # just show what would be sent (no Kafka), first few records
    python demo-eval/ttq_producer.py --rate 1000 --dry-run --limit 3
"""

import argparse
import csv
import json
import os
import sys
import time

# Only the fields the indexer's json mapping consumes are emitted, to keep the
# payload small (matters at high lambda). Keys must match ttq_index.config.json.
SOURCE_ACTIVITY = "activity"
SOURCE_TRACE_ID = "trace_id"
SOURCE_POSITION = "position"
SOURCE_TIMESTAMP = "timestamp"

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DATASET = os.path.join(REPO_ROOT, "datasets", "bpic2017.csv")


def iter_events(dataset_path):
    """Yield event dicts from the BPIC2017 CSV, streaming (row-by-row)."""
    with open(dataset_path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield {
                SOURCE_TRACE_ID: row.get("trace_id"),
                SOURCE_POSITION: row.get("position"),
                SOURCE_ACTIVITY: row.get("activity"),
                SOURCE_TIMESTAMP: row.get("timestamp"),
            }


def run_dry(args):
    print(f"[dry-run] dataset={args.dataset} rate={args.rate} duration={args.duration}s")
    for i, ev in enumerate(iter_events(args.dataset)):
        if i >= args.limit:
            break
        ev["_produced_at"] = time.time()
        ev["_lambda"] = args.lambda_tag if args.lambda_tag is not None else args.rate
        print(json.dumps(ev))
    print(f"[dry-run] shown {min(args.limit, i + 1)} records")


def run(args):
    from confluent_kafka import Producer

    lam_tag = args.lambda_tag if args.lambda_tag is not None else args.rate
    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "acks": args.acks,
        "linger.ms": args.linger_ms,
        "batch.num.messages": 100000,
        "queue.buffering.max.messages": 2000000,
    })

    topic = args.topic
    rate = float(args.rate)
    # Emit in fixed wall-clock windows so the average rate tracks lambda even at
    # sub-millisecond per-record intervals (batched sends, no per-record sleep).
    window_s = args.window_ms / 1000.0
    per_window = max(1, int(round(rate * window_s)))

    print(f"Connecting to Kafka at {args.bootstrap_servers}; topic '{topic}'")
    print(f"Target lambda={rate:g} ev/s (~{per_window} ev / {args.window_ms:g}ms window), "
          f"duration={args.duration}s, tag _lambda={lam_tag:g}")

    sent = 0
    dropped = 0
    t_start = time.time()
    t_deadline = t_start + args.duration
    next_window = t_start

    events = iter_events(args.dataset)
    stop = False
    while not stop:
        now = time.time()
        if now >= t_deadline:
            break
        # Fill this window's quota.
        for _ in range(per_window):
            try:
                ev = next(events)
            except StopIteration:
                print("Dataset exhausted before duration elapsed.")
                stop = True
                break
            ev["_produced_at"] = time.time()
            ev["_lambda"] = lam_tag
            payload = json.dumps(ev).encode("utf-8")
            key = (ev.get(SOURCE_TRACE_ID) or "").encode("utf-8")
            while True:
                try:
                    producer.produce(topic, value=payload, key=key)
                    break
                except BufferError:
                    # Local queue full: let librdkafka drain, then retry.
                    producer.poll(0.05)
            sent += 1
        producer.poll(0)

        # Sleep off the remainder of the window to hold the rate.
        next_window += window_s
        slack = next_window - time.time()
        if slack > 0:
            time.sleep(slack)
        else:
            # Falling behind the target rate (producer-side saturation).
            dropped += 1

    producer.flush(30)
    elapsed = time.time() - t_start
    actual = sent / elapsed if elapsed > 0 else 0.0
    print(f"\nSent {sent} events in {elapsed:.1f}s -> actual rate {actual:.1f} ev/s "
          f"(target {rate:g}); {dropped} windows over budget.")
    if dropped > max(5, 0.2 * (elapsed / window_s)):
        print("WARNING: producer could not sustain the target rate (host-side "
              "bottleneck). The measured knee may reflect the producer, not the "
              "indexer -- consider a lower --rate ceiling.", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description="TTQ BPIC2017 Kafka replay producer")
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--topic", default="ttq_bpic2017")
    ap.add_argument("--rate", type=float, required=True, help="lambda: target events/second")
    ap.add_argument("--duration", type=float, default=300.0, help="replay seconds")
    ap.add_argument("--dataset", default=DEFAULT_DATASET)
    ap.add_argument("--lambda-tag", type=float, default=None,
                    help="value written into each event's _lambda (default: --rate)")
    ap.add_argument("--window-ms", type=float, default=100.0,
                    help="rate-shaping window size in ms (default 100)")
    ap.add_argument("--linger-ms", type=float, default=5.0)
    ap.add_argument("--acks", default="1", help="Kafka acks (0/1/all)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=5, help="dry-run: records to print")
    args = ap.parse_args()

    if not os.path.exists(args.dataset):
        ap.error(f"dataset not found: {args.dataset}")

    if args.dry_run:
        run_dry(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
