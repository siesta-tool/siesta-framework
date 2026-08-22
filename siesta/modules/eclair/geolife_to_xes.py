"""
geolife_to_xes.py

Parses geolife_preprocessed.csv, snaps each trajectory to a lat/lon grid,
extracts OD trips (start-cell -> end-cell, 30-min timeslot of the trip start),
groups data by a chosen function, mines OD-Time flow patterns per group using
the OPT algorithm, and exports a pm4py XES event log.

Each group becomes one Trace; each mined pattern becomes one Event, sorted
ascending by the average timeslot of its time component.

Usage:
    python3 geolife_to_xes.py <csv> <sup_atomic> <sup_extended> <timebound>
        [--cell-size 0.01]
        [--groupby single|day_of_week|user_id|transport_mode]
        [--output geolife_patterns.xes]

Arguments:
    csv            Path to geolife_preprocessed.csv
    sup_atomic     Fraction threshold for atomic patterns  (e.g. 0.001)
    sup_extended   Ratio threshold for extended patterns   (e.g. 0.5)
    timebound      Max consecutive timeslot span           (e.g. 6)
    --cell-size    Grid cell side in degrees               [default: 0.01]
    --groupby      Grouping key                            [default: single]
    --output       Output XES path                        [default: geolife_patterns.xes]
"""

import argparse
import csv
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from math import floor

import pm4py
from pm4py.objects.log.obj import EventLog, Trace, Event
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser(description="GeoLife GPS trajectories → OPT patterns → XES")
parser.add_argument("csv",         help="Path to geolife_preprocessed.csv")
parser.add_argument("sup_atomic",  type=float, help="Atomic support fraction  (e.g. 0.001)")
parser.add_argument("sup_extended",type=float, help="Extended support ratio   (e.g. 0.5)")
parser.add_argument("timebound",   type=int,   help="Max timeslot span        (e.g. 6)")
parser.add_argument("--cell-size", type=float, default=0.01,
                    help="Grid cell size in degrees [default: 0.01 (~1 km)]")
parser.add_argument("--groupby",   default="single",
                    choices=["single", "day_of_week", "date", "user_id", "transport_mode"],
                    help="Grouping key for traces  [default: single]")
parser.add_argument("--output",    default="geolife_patterns.xes",
                    help="Output XES file path     [default: geolife_patterns.xes]")

args = parser.parse_args()

cell_size  = args.cell_size
timebound  = args.timebound - 1   # matches findpatterns.py convention
sup_atomic = args.sup_atomic
min_ratio  = args.sup_extended

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


# ---------------------------------------------------------------------------
# Grid helpers
# ---------------------------------------------------------------------------

def latlon_to_cell(lat, lon, cell_size):
    """Return a hashable (row_bin, col_bin) cell identifier."""
    return (floor(lat / cell_size), floor(lon / cell_size))


# ---------------------------------------------------------------------------
# CSV parse: one streaming pass
# Assumes rows are sorted by trajectory_id (consecutive blocks).
# Collects per-trajectory start/end to form OD trips.
# ---------------------------------------------------------------------------

print(f"Parsing {args.csv} …")

# trips: list of dicts with keys:
#   user_id, trajectory_id, src_cell, dst_cell,
#   timeslot (0-47), day_of_week (0=Mon), transport_mode
trips      = []
all_cells  = set()   # every cell that appears as src or dst

def emit_trip(user_id, trajectory_id, first_row, last_row):
    """Convert a start/end point pair into an OD trip record."""
    src = latlon_to_cell(float(first_row["latitude"]),
                         float(first_row["longitude"]),
                         cell_size)
    dst = latlon_to_cell(float(last_row["latitude"]),
                         float(last_row["longitude"]),
                         cell_size)
    if src == dst:
        return  # no movement across cells — discard

    ts_str = first_row["timestamp"]                     # "2008-10-23 02:53:04"
    dt     = datetime.fromisoformat(ts_str)
    slot   = dt.hour * 2 + dt.minute // 30             # 0-47

    trips.append({
        "user_id":       user_id,
        "trajectory_id": trajectory_id,
        "src_cell":      src,
        "dst_cell":      dst,
        "timeslot":      slot,
        "day_of_week":   dt.weekday(),                 # 0=Monday
        "date":          dt.date().isoformat(),        # e.g. "2008-10-23"
        "transport_mode": first_row["transport_mode"],
    })
    all_cells.add(src)
    all_cells.add(dst)


with open(args.csv, newline="") as fh:
    reader    = csv.DictReader(fh)
    cur_tid   = None
    first_row = None
    last_row  = None
    cur_uid   = None

    for row in reader:
        if row["trajectory_id"] != cur_tid:
            if cur_tid is not None:
                emit_trip(cur_uid, cur_tid, first_row, last_row)
            cur_tid   = row["trajectory_id"]
            cur_uid   = row["user_id"]
            first_row = row
        last_row = row

    # emit the final trajectory
    if cur_tid is not None:
        emit_trip(cur_uid, cur_tid, first_row, last_row)

print(f"  Trajectories parsed → valid OD trips: {len(trips)}")
print(f"  Unique grid cells:  {len(all_cells)}")


# ---------------------------------------------------------------------------
# Build neighborhood graph  (4-connectivity over cells present in the data)
# ---------------------------------------------------------------------------

print("Building neighborhood graph …")

neighbor = defaultdict(list)

for (row_bin, col_bin) in all_cells:
    for delta in [(-1, 0), (1, 0), (0, -1), (0, 1)]:
        nb = (row_bin + delta[0], col_bin + delta[1])
        if nb in all_cells:
            neighbor[(row_bin, col_bin)].append(nb)

print(f"  Neighbor entries:   {len(neighbor)}")


# ---------------------------------------------------------------------------
# Grouping functions
# Each receives a trip dict and returns a string label.
# ---------------------------------------------------------------------------

def group_single(trip):
    return "all"

def group_day_of_week(trip):
    return DAYS[trip["day_of_week"]]

def group_date(trip):
    return trip["date"]

def group_user_id(trip):
    return trip["user_id"]

def group_transport_mode(trip):
    return trip["transport_mode"]

GROUPING_FNS = {
    "single":         group_single,
    "day_of_week":    group_day_of_week,
    "date":           group_date,
    "user_id":        group_user_id,
    "transport_mode": group_transport_mode,
}

grouping_fn = GROUPING_FNS[args.groupby]

# Partition trips into groups
grouped_trips = defaultdict(list)
for trip in trips:
    grouped_trips[grouping_fn(trip)].append(trip)

print(f"  Groups formed ({args.groupby}): {len(grouped_trips)}")


# ---------------------------------------------------------------------------
# OPT helpers
# ---------------------------------------------------------------------------

def get_neighbors(region, other, neighbor):
    """Return cells neighbouring `region` that are not in `region` or `other`."""
    nbrs = []
    for node in region:
        if node in neighbor:
            nbrs.extend(neighbor[node])
    nbrs = set(nbrs)
    nbrs.difference_update(region)
    nbrs.difference_update(other)
    return nbrs


def build_atomic(trip_list, sup_atomic_frac):
    """
    Build the atomic dict and sup_atomic_num from a list of trip records.

    Returns (atomic, sup_atomic_num) where:
        atomic[src][dst][timeslot] = flow_count
        sup_atomic_num             = the flow threshold for top sup_atomic_frac
    """
    flow_counter = defaultdict(int)   # (src, dst, ts) -> count

    for trip in trip_list:
        key = (trip["src_cell"], trip["dst_cell"], trip["timeslot"])
        flow_counter[key] += 1

    atomic = {}
    flows  = []
    for (src, dst, ts), flow in flow_counter.items():
        flows.append(flow)
        if src not in atomic:
            atomic[src] = {}
        if dst not in atomic[src]:
            atomic[src][dst] = {}
        atomic[src][dst][ts] = flow

    if not flows:
        return atomic, 0

    flows.sort(reverse=True)
    threshold_idx  = int(sup_atomic_frac * len(flows))
    threshold_idx  = min(threshold_idx, len(flows) - 1)
    sup_atomic_num = flows[threshold_idx]
    return atomic, sup_atomic_num


def run_OPT(atomic, neighbor, sup_atomic_num, min_ratio, timebound):
    """
    Run the OPT pattern mining algorithm.

    Adapted from findpatterns.py (OPT function) with all globals replaced by
    explicit parameters and get_neighbors made pure.

    Returns the `patterns` list where patterns[s] is a dict of
    {(src_tuple, dst_tuple, ts_tuple): support_count} for pattern size s.
    """
    triples      = [{}, {}, {}, {}]
    patterns     = [{}, {}, {}, {}]
    dests        = {}    # src  -> set of dests with >= 1 atomic pattern
    sources      = {}    # dest -> set of srcs  with >= 1 atomic pattern
    timeprefsums = {}    # (src, dst) -> 49-element prefix-sum list

    for i in atomic:
        for j in atomic[i]:
            cursum = 0
            timeprefsums[(i, j)] = [0]
            for k in range(48):
                if k in atomic[i][j] and atomic[i][j][k] >= sup_atomic_num:
                    patterns[3][(tuple([i]), tuple([j]), tuple([k]))] = 1
                    if i in dests:
                        dests[i].add(j)
                    else:
                        dests[i] = {j}
                    if j in sources:
                        sources[j].add(i)
                    else:
                        sources[j] = {i}
                    cursum += 1
                timeprefsums[(i, j)].append(cursum)

    triples[3] = patterns[3]

    size = 4
    while patterns[size - 1]:
        patterns.append({})
        triples.append({})

        for p in patterns[size - 1]:
            starting_sup = patterns[size - 1][p]

            # ---- expand source ----
            neigh = get_neighbors(p[0], p[1], neighbor)
            for n in neigh:
                expsrc = tuple(sorted(p[0] + (n,)))
                if (expsrc, p[1], p[2]) in triples[size]:
                    continue
                cursupcount = starting_sup

                if n not in dests or not dests[n] & set(p[1]):
                    if n in atomic:
                        if cursupcount >= (len(p[0]) + 1) * len(p[1]) * len(p[2]) * min_ratio:
                            patterns[size][(expsrc, p[1], p[2])] = cursupcount
                        triples[size][(expsrc, p[1], p[2])] = cursupcount
                else:
                    if n in atomic:
                        pprime = ((n,), p[1], p[2])
                        sz_pprime = 1 + len(p[1]) + len(p[2])
                        if pprime in triples[sz_pprime]:
                            cursupcount += triples[sz_pprime][pprime]
                        else:
                            for j in p[1]:
                                if j in atomic[n]:
                                    cursupcount += (
                                        timeprefsums[(n, j)][p[2][-1] + 1]
                                        - timeprefsums[(n, j)][p[2][0]]
                                    )
                        if cursupcount >= (len(p[0]) + 1) * len(p[1]) * len(p[2]) * min_ratio:
                            patterns[size][(expsrc, p[1], p[2])] = cursupcount
                        triples[size][(expsrc, p[1], p[2])] = cursupcount

            # ---- expand dest ----
            neigh = get_neighbors(p[1], p[0], neighbor)
            for n in neigh:
                expdest = tuple(sorted(p[1] + (n,)))
                if (p[0], expdest, p[2]) in triples[size]:
                    continue
                cursupcount = starting_sup

                if n not in sources or not sources[n] & set(p[0]):
                    if cursupcount >= len(p[0]) * (1 + len(p[1])) * len(p[2]) * min_ratio:
                        patterns[size][(p[0], expdest, p[2])] = cursupcount
                    triples[size][(p[0], expdest, p[2])] = cursupcount
                else:
                    pprime = (p[0], (n,), p[2])
                    sz_pprime = len(p[0]) + 1 + len(p[2])
                    if pprime in triples[sz_pprime]:
                        cursupcount += triples[sz_pprime][pprime]
                    else:
                        for i in p[0]:
                            if i in atomic and n in atomic[i]:
                                cursupcount += (
                                    timeprefsums[(i, n)][p[2][-1] + 1]
                                    - timeprefsums[(i, n)][p[2][0]]
                                )
                    if cursupcount >= len(p[0]) * (1 + len(p[1])) * len(p[2]) * min_ratio:
                        patterns[size][(p[0], expdest, p[2])] = cursupcount
                    triples[size][(p[0], expdest, p[2])] = cursupcount

            # ---- expand timeslot forward ----
            nextts = p[2][-1] + 1
            if nextts <= 47 and nextts - p[2][0] <= timebound:
                cursupcount = starting_sup
                pprime = (p[0], p[1], (nextts,))
                sz_pprime = len(p[0]) + len(p[1]) + 1
                if pprime in triples[sz_pprime]:
                    cursupcount += triples[sz_pprime][pprime]
                else:
                    for i in p[0]:
                        if i in atomic:
                            for j in p[1]:
                                if j in atomic[i]:
                                    if nextts in atomic[i][j] and atomic[i][j][nextts] >= sup_atomic_num:
                                        cursupcount += 1
                if cursupcount >= len(p[0]) * len(p[1]) * (1 + len(p[2])) * min_ratio:
                    patterns[size][(p[0], p[1], p[2] + (nextts,))] = cursupcount
                triples[size][(p[0], p[1], p[2] + (nextts,))] = cursupcount

            # ---- expand timeslot backward ----
            prevts = p[2][0] - 1
            if prevts >= 0 and p[2][-1] - prevts <= timebound:
                expts = (prevts,) + p[2]
                if (p[0], p[1], expts) in triples[size]:
                    continue
                cursupcount = starting_sup
                pprime = (p[0], p[1], (prevts,))
                sz_pprime = len(p[0]) + len(p[1]) + 1
                if pprime in triples[sz_pprime]:
                    cursupcount += triples[sz_pprime][pprime]
                else:
                    for i in p[0]:
                        if i in atomic:
                            for j in p[1]:
                                if j in atomic[i]:
                                    if prevts in atomic[i][j] and atomic[i][j][prevts] >= sup_atomic_num:
                                        cursupcount += 1
                if cursupcount >= len(p[0]) * len(p[1]) * (1 + len(p[2])) * min_ratio:
                    patterns[size][(p[0], p[1], expts)] = cursupcount
                triples[size][(p[0], p[1], expts)] = cursupcount

        size += 1

    return patterns


# ---------------------------------------------------------------------------
# Timeslot -> datetime helpers
# Timeslots are 0-47 (each = 30 minutes from midnight 2024-01-01 UTC)
# ---------------------------------------------------------------------------

REFERENCE_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)


def avg_timeslot(ts_tuple):
    return sum(ts_tuple) / len(ts_tuple)


def timeslot_to_datetime(avg_ts):
    return REFERENCE_DATE + timedelta(minutes=avg_ts * 30)


# ---------------------------------------------------------------------------
# Mine patterns per group, build XES log
# ---------------------------------------------------------------------------

log = EventLog()
total_patterns = 0

for group_label in sorted(grouped_trips.keys()):
    trip_list = grouped_trips[group_label]
    print(f"\nGroup '{group_label}': {len(trip_list)} trips")

    atomic, sup_atomic_num = build_atomic(trip_list, sup_atomic)
    if not atomic:
        print("  No valid atomic patterns — skipping group.")
        continue

    t0       = time.time()
    patterns = run_OPT(atomic, neighbor, sup_atomic_num, min_ratio, timebound)
    t1       = time.time()

    group_total = sum(len(patterns[s]) for s in range(len(patterns)))
    total_patterns += group_total
    print(f"  OPT complete in {t1 - t0:.2f}s — patterns: {group_total}")

    # Flatten and sort by avg timeslot
    flat = []
    for s in range(3, len(patterns)):
        for key, support in patterns[s].items():
            flat.append((key, support))

    flat.sort(key=lambda x: avg_timeslot(x[0][2]))

    # Build trace
    trace = Trace()
    trace.attributes["concept:name"] = group_label

    for key, support in flat:
        src_tuple, dst_tuple, ts_tuple = key
        avg_ts  = avg_timeslot(ts_tuple)
        ts_time = timeslot_to_datetime(avg_ts)

        event = Event()
        event["concept:name"]   = f"src={src_tuple} dst={dst_tuple}"
        event["time:timestamp"] = ts_time
        event["support"]        = support
        event["timeslots"]      = str(ts_tuple)
        event["avg_timeslot"]   = avg_ts

        trace.append(event)

    log.append(trace)

print(f"\nEvent log built: {len(log)} trace(s), {sum(len(t) for t in log)} event(s) total")
print(f"Total patterns across all groups: {total_patterns}")


# ---------------------------------------------------------------------------
# Export XES
# ---------------------------------------------------------------------------

print(f"Writing XES to {args.output} …")
xes_exporter.apply(log, args.output)
print("Done.")
