import random
import json
import os
import time
import requests
import pandas as pd
import math
from collections import defaultdict
from pm4py.objects.log.importer.xes import importer as xes_importer


###############################################################################
# CONFIG
###############################################################################
SEED = 42
random.seed(SEED)

ELK_ENDPOINT = "http://localhost:9200"
SIESTA_ENDPOINT = "http://localhost:8000"

NUM_PATTERNS = 100
STRUCTURAL_PATTERNS = 50
ATTRIBUTE_PATTERNS = 50

MIN_LEN = 8
MAX_LEN = 15

ACTIVITY_KEY = "concept:name"
API_TIMEOUT_S = 1000

BLACKLIST_ATTRS = {
    "time:timestamp",
    "@timestamp",
    "event_index",
    "lifecycle:transition"
}


###############################################################################
# STEP 0 — XES → CSV
###############################################################################

def xes_to_csv(xes_file, csv_file):
    log = xes_importer.apply(xes_file)

    rows = []
    for tid, trace in enumerate(log):
        for e in trace:
            row = {
                "trace_id": tid,
                "activity": e.get(ACTIVITY_KEY),
                "timestamp": e.get("time:timestamp")
            }

            for k, v in e.items():
                if k in (ACTIVITY_KEY, "time:timestamp"):
                    continue
                if isinstance(v, (str, int, float, bool)):
                    row[k] = v

            rows.append(row)

    pd.DataFrame(rows).to_csv(csv_file, index=False)
    print(f"[STEP 0] CSV written → {csv_file}")


###############################################################################
# STEP 1 — ELK INDEX CREATION
###############################################################################

def elk_create_index():

    requests.delete(f"{ELK_ENDPOINT}/{ELK_INDEX}", timeout=10)

    time.sleep(1)

    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "-1"
        },
        "mappings": {
            "properties": {
                "trace_id": {"type": "keyword"},
                "activity": {"type": "keyword"},
                "timestamp": {"type": "date", "format": "strict_date_optional_time||epoch_millis"}
            },
            "dynamic": True
        }
    }

    r = requests.put(
        f"{ELK_ENDPOINT}/{ELK_INDEX}",
        json=mapping,
        timeout=30
    )

    r.raise_for_status()

    print("[STEP 1] Index created")


###############################################################################
# STEP 2 — BULK INDEXING
###############################################################################
def normalize_timestamp(ts):
    # FIXED: Preserve milliseconds for accurate sorting, format as ISO 8601
    return str(ts).replace(" ", "T")

def sanitize_value(v):

    # convert pandas NaN / None / float NaN
    if v is None:
        return None

    if isinstance(v, float) and math.isnan(v):
        return None

    return v

def elk_bulk_index(csv_file, batch_size=20000):

    df = pd.read_csv(csv_file)
    df["timestamp"] = df["timestamp"].apply(normalize_timestamp)

    total = len(df)
    print(f"[STEP 2] Indexing {total} events in batches of {batch_size}")

    for start in range(0, total, batch_size):

        chunk = df.iloc[start:start + batch_size]

        bulk_body = []

        for _, row in chunk.iterrows():

            action = {"index": {"_index": ELK_INDEX}}

            doc = {
                k: sanitize_value(v)
                for k, v in row.to_dict().items()
            }
            bulk_body.append(json.dumps(action))
            bulk_body.append(json.dumps(doc, default=str))

        body = "\n".join(bulk_body) + "\n"

        r = requests.post(
            f"{ELK_ENDPOINT}/_bulk",
            data=body,
            headers={"Content-Type": "application/x-ndjson"},
            timeout=60
        )

        if r.status_code >= 400:
            raise RuntimeError(f"Bulk failed: {r.text[:500]}")

        resp = r.json()

        if resp.get("errors"):
            # surface first failure
            first_error = next(
                (item for item in resp["items"] if "error" in item["index"]),
                None
            )
            raise RuntimeError(f"Bulk indexing error sample: {first_error}")

        print(f"[STEP 2] Indexed {min(start+batch_size, total)}/{total}")

    # final refresh
    requests.post(f"{ELK_ENDPOINT}/{ELK_INDEX}/_refresh")

    print("[STEP 2] Bulk indexing completed ✔")


###############################################################################
# TRACE RECONSTRUCTION (for pattern generation)
###############################################################################

def load_traces(csv_file):

    df = pd.read_csv(csv_file)

    traces = defaultdict(list)

    for _, row in df.iterrows():
        traces[row["trace_id"]].append(row.to_dict())

    for t in traces:
        traces[t] = sorted(traces[t], key=lambda x: x.get("timestamp"))

    return list(traces.values())


###############################################################################
# EVENT UTILITIES
###############################################################################

def event_activity(e):
    return str(e.get("activity", "UNKNOWN"))

def valid_attrs(e):
    return [
        k for k, v in e.items()
        if k not in ("activity", "timestamp", "trace_id")
        and k not in BLACKLIST_ATTRS
        and isinstance(v, (str, int, float, bool))
        and not (isinstance(v, float) and math.isnan(v)) # <--- FIX: Reject NaNs
    ]

def sample_trace(trace):
    if len(trace) < MIN_LEN:
        return None

    L = random.randint(MIN_LEN, min(MAX_LEN, len(trace)))
    S = random.randint(0, len(trace) - L)

    return trace[S:S + L]


###############################################################################
# DSL SERIALIZATION
###############################################################################

def pattern_to_string(pattern):
    tokens = []

    for ev in pattern["events"]:
        act = ev["activity"]

        attrs = []
        for k, v in ev.items():
            if k == "activity":
                continue

            if isinstance(v, str) and v.startswith("$"):
                attrs.append(f"{k}={v}")
            else:
                attrs.append(f"{k}='{v}'")

        tokens.append(f"{act}[{','.join(attrs)}]" if attrs else act)

    # FIX: Use a unique delimiter instead of a space
    return " ;; ".join(tokens)

def siesta_create_index(xes_file_path):
    print(f"[STEP 3] Uploading and Indexing dataset in Siesta...")

    payload = {
        "log_name": ELK_INDEX, # Aligning log_name so queries match
        "storage_namespace": "siesta",
        "overwrite_data": True, # Switch to True if running the same log multiple times
        "lookback": "3000000d",
        "field_mappings": {
            "field_mappings": {
                "xes": {
                    "activity": "concept:name",
                    "trace_id": "concept:name",
                    "position": None,
                    "start_timestamp": "time:timestamp",
                    "attributes" : ["*"]
                }
            },
            "trace_level_fields": ["trace_id"],
            "timestamp_fields": ["start_timestamp"]
        }
    }

    try:
        # Open file as binary stream for multipart upload
        with open(xes_file_path, "rb") as f:
            
            # Note: You may need to change "config" and "file" below if the Siesta API 
            # explicitly expects different form-data key names.
            data = {
                "index_config": json.dumps(payload) 
            }
            files = {
                "log_file": (os.path.basename(xes_file_path), f, "application/octet-stream")
            }

            # Timeout increased to 10 minutes because file uploading and indexing takes time
            r = requests.post(
                f"{SIESTA_ENDPOINT}/indexing/run",
                data=data,
                files=files,
                timeout=600 
            )

            r.raise_for_status()
            print("[STEP 3] Siesta dataset indexing completed ✔")

    except requests.exceptions.RequestException as e:
        print(f"[STEP 3] Siesta API Error during indexing: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"API Response Context: {e.response.text}")
        # Not throwing a hard error so the ELK pipeline can still finish if Siesta fails
        print("Continuing with remaining steps...")
        
###############################################################################
# VALIDATION (STRICT SUBSEQUENCE CHECK)
###############################################################################

def is_valid_subsequence(chain, constraints, trace):

    pos = 0

    for ev in trace:

        if pos >= len(chain):
            break

        if ev["activity"] != chain[pos]:
            continue

        ok = True

        if pos in constraints:
            k, v = constraints[pos]
            if str(ev.get(k)) != v:
                continue

        pos += 1

        if pos == len(chain):
            return True

    return False


###############################################################################
# PATTERN GENERATION
###############################################################################

def create_attribute_pattern(subtrace):

    pattern = []
    bindings = {}
    vid = 1

    candidates = list(range(len(subtrace)))
    random.shuffle(candidates)

    num_vars = random.randint(1, max(1, len(subtrace)//4))

    for idx in candidates:

        ev = subtrace[idx]
        attrs = valid_attrs(ev)

        if not attrs:
            continue

        attr = random.choice(attrs)
        val = ev[attr]

        occ = [i for i, e in enumerate(subtrace) if e.get(attr) == val]

        if len(occ) >= 2:
            var = f"${vid}"
            vid += 1
            bindings[(attr, val)] = var

            if len(bindings) >= num_vars:
                break

    for ev in subtrace:
        item = {"activity": event_activity(ev)}

        attrs = valid_attrs(ev)
        chosen = random.sample(attrs, k=min(len(attrs), random.randint(0, 2)))

        for a in chosen:
            v = ev[a]

            bound = False
            for (ba, bv), var in bindings.items():
                if ba == a and bv == v:
                    item[a] = var
                    bound = True
                    break

            if not bound:
                item[a] = v

        pattern.append(item)

    return {"type": "attribute-aware", "events": pattern}


def generate_valid_attribute_pattern(traces, max_tries=50):

    for _ in range(max_tries):

        trace = random.choice(traces)
        sub = sample_trace(trace)

        if not sub:
            continue

        pattern = create_attribute_pattern(sub)
        dsl = pattern_to_string(pattern)

        chain, constraints = parse_pattern(dsl)

        return pattern

    return None


###############################################################################
# PARSER
###############################################################################

def parse_pattern(dsl):
    tokens = dsl.split(" ;; ")
    chain = []
    
    # FIX: Use defaultdict(list) to store multiple attributes per event index
    constraints = defaultdict(list)

    for i, tok in enumerate(tokens):
        if "[" not in tok:
            chain.append(tok)
            continue

        act, rest = tok.split("[", 1)
        chain.append(act)

        inside = rest.rstrip("]")

        for part in inside.split(","):
            if "=" not in part:
                continue

            k, v = part.split("=", 1)
            # FIX: Append to the list instead of overwriting
            constraints[i].append((k, v.strip().strip("'")))

    return chain, dict(constraints)


###############################################################################
# GENERATE PATTERNS
###############################################################################

def generate_patterns(traces):

    patterns = []

    while len(patterns) < STRUCTURAL_PATTERNS:
        t = random.choice(traces)
        sub = sample_trace(t)
        if sub:
            patterns.append({
                "type": "structural",
                "events": [{"activity": e["activity"]} for e in sub]
            })

    while len(patterns) < NUM_PATTERNS:
        p = generate_valid_attribute_pattern(traces)
        if p:
            patterns.append(p)

    return patterns


###############################################################################
# JSONL OUTPUT
###############################################################################

def write_jsonl(patterns, path):

    with open(path, "w", encoding="utf-8") as f:
        for p in patterns:
            f.write(json.dumps({
                "log_name": LOG_NAME,
                "query": {"pattern": pattern_to_string(p)}
            }) + "\n")


###############################################################################
# ELK DETECTION
###############################################################################

def elk_detect(pattern):

    chain, constraints = parse_pattern(pattern)

    acts = sorted(set(chain))

    body = {
        "query": {
            "bool": {
                "filter": [{"terms": {"activity": acts}}]
            }
        },
        "size": 10000,
        "sort": [{"timestamp": "asc"}, {"_doc": "asc"}]
    }

    t0 = time.perf_counter()

    groups = defaultdict(list)
    search_after = None
    total = 0

    while True:

        if search_after:
            body["search_after"] = search_after

        r = requests.post(
            f"{ELK_ENDPOINT}/{ELK_INDEX}/_search",
            json=body,
            timeout=API_TIMEOUT_S
        )

        hits = r.json()["hits"]["hits"]

        if not hits:
            break

        for h in hits:
            src = h["_source"]
            groups[src["trace_id"]].append(src)
            total += 1

        search_after = hits[-1]["sort"]

        if len(hits) < body["size"]:
            break

    matches = 0
    matching_traces = []  # <-- CHANGE 1: Initialize a list to hold valid trace_ids
    for trace_id, events in groups.items():
        match_found = False

        for start_idx in range(len(events)):
            if events[start_idx]["activity"] != chain[0]:
                continue

            pos = 0
            bindings = {}
            seq_ok = False

            for j in range(start_idx, len(events)):
                ev = events[j]

                if ev["activity"] != chain[pos]:
                    continue

                ok = True
                # FIX: Use a temporary dictionary. Only commit bindings if the whole event passes!
                temp_bindings = dict(bindings)

                if pos in constraints:
                    # FIX: Iterate through the newly created LIST of constraints
                    for k, expected_v in constraints[pos]:
                        actual_v = str(ev.get(k))

                        if expected_v.startswith("$"):
                            if expected_v in temp_bindings:
                                if temp_bindings[expected_v] != actual_v:
                                    ok = False
                                    break
                            else:
                                temp_bindings[expected_v] = actual_v
                        else:
                            if actual_v != expected_v:
                                ok = False
                                break

                if ok:
                    pos += 1
                    bindings = temp_bindings # Commit bindings
                    if pos == len(chain):
                        seq_ok = True
                        break

            if seq_ok:
                match_found = True
                break

        if match_found:
            matches += 1
            matching_traces.append(trace_id)

    print(matching_traces)
    return time.perf_counter() - t0, matches, total


###############################################################################
# SIESTA DETECTION
###############################################################################

def siesta_detect(dsl):
    
    chain, constraints = parse_pattern(dsl)
    
    siesta_tokens = []
    
    for i, act in enumerate(chain):
        token = f'"{act}"'
        
        if i in constraints and constraints[i]:
            attrs = []
            for k, v in constraints[i]:
                if v.startswith("$"):
                    attrs.append(f"{k}={v}")     
                else:
                    attrs.append(f"{k}='{v}'")   
            token += f"[{','.join(attrs)}]"
            
        siesta_tokens.append(token)

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    siesta_pattern = " ".join(siesta_tokens)
    print (siesta_pattern)
    payload = {
        "log_name": ELK_INDEX, 
        "query": {
            "pattern": siesta_pattern
        }
    }

    t0 = time.perf_counter()
    
    try:
        r = requests.post(
            f"{SIESTA_ENDPOINT}/querying/detection",
            headers=headers,
            json=payload,
            timeout=API_TIMEOUT_S
        )
        r.raise_for_status()
        resp = r.json()
        
        # Parse the JSON response
        siesta_matches = resp.get("total", 0)
        siesta_backend_time = float(resp.get("time", 0.0))
        
    except requests.exceptions.RequestException as e:
        print(f"Siesta API Error: {e}")
        siesta_matches = 0
        siesta_backend_time = 0.0

    network_time = time.perf_counter() - t0
    
    return network_time, siesta_backend_time, siesta_matches


###############################################################################
# BENCHMARK
###############################################################################

def run_benchmark(jsonl_path):

    results = []

    with open(jsonl_path, "r") as f:
        for line in f:

            obj = json.loads(line)
            pattern = obj["query"]["pattern"]

            # elk_elapsed, elk_matches, total = elk_detect(pattern)
            siesta_net_time, siesta_backend_time, siesta_matches = siesta_detect(pattern)

            # print(f"Pattern: {pattern[:40]}... | Matches: ELK({elk_matches}) | Time: ELK({elk_elapsed:.2f}s)")
            print(f"Pattern: {pattern[:40]}...  Siesta({siesta_matches}) | Time: Siesta({siesta_net_time:.2f}s)")

            results.append({
                "pattern": pattern,
                # "elk_sec": elk_elapsed,
                # "elk_matches": elk_matches,
                "siesta_network_sec": siesta_net_time,
                "siesta_backend_sec": siesta_backend_time,
                "siesta_matches": siesta_matches,
                # "retrieved_events": total
            })

    return results


###############################################################################
# MAIN PIPELINE
###############################################################################
def main():
    for log in ["bpic2017", "bpic2012", "bpic2018", "bpic2015", "bpic2011"]:
        print(f"\n=======================================================")
        print(f"Running benchmark for {log}...")
        print(f"=======================================================\n")

        global LOG_NAME
        LOG_NAME = log
        global XES_FILE
        XES_FILE = "/mnt/datasets/" + LOG_NAME + ".xes"
        global CSV_FILE
        CSV_FILE = "datasets/" + LOG_NAME + ".csv"

        if not os.path.exists(CSV_FILE):
            xes_to_csv(XES_FILE, CSV_FILE)

        global ELK_INDEX
        ELK_INDEX = LOG_NAME.lower() + "_events"

        siesta_create_index(XES_FILE)

        # NOTE: Comment these two lines out if you have already indexed your data for these logs
        # elk_create_index()
        # elk_bulk_index(CSV_FILE)

        # traces = load_traces(CSV_FILE)

        # patterns = generate_patterns(traces)

        # write_jsonl(patterns, "patterns_" + LOG_NAME + ".jsonl")

        results = run_benchmark("patterns" + LOG_NAME + ".jsonl")

        with open("siesta_results" + LOG_NAME + ".jsonl", "w") as f:
            for r in results:
                f.write(json.dumps(r) + "\n")

        print("\nDONE for " + log)
        # print(f"patterns: {len(patterns)}")


if __name__ == "__main__":
    main()