import sys
import json
import re
import math
import pandas as pd
from collections import defaultdict
import os
import csv
import json
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType

import logging
logger = logging.getLogger(__name__)


def discover_ngrams(events: DataFrame, trace_labels: DataFrame, n: int = 2) -> DataFrame:
    """
    Discover consecutive activity n-tuples that discriminate between
    targeted traces (label=1) and non-targeted traces (label=0).

    Parameters
    ----------
    events : pyspark.sql.DataFrame
        Must contain columns: "activity", "trace_id", "start_timestamp".
    trace_labels : pyspark.sql.DataFrame
        Must contain columns: "trace_id", "label" (1 = targeted, 0 = non-targeted).
    n : int, default 2
        Length of the consecutive activity tuples to extract.

    Returns
    -------
    pyspark.sql.DataFrame with columns:
        ngram          : str   - human-readable, e.g. "A -> B -> C"
        count_1        : int   - distinct label-1 traces containing this n-gram
        count_0        : int   - distinct label-0 traces containing this n-gram
        balance        : float - in [-1, 1]
                                 (count_1/total_1) - (count_0/total_0)
                                 +1 -> n-gram appears in ALL label-1 and NO label-0 traces
                                 -1 -> n-gram appears in ALL label-0 and NO label-1 traces
                                  0 -> equal relative coverage in both groups
        confidence_1   : float - in [0, 1]
                                 count_1 / (count_1 + count_0)
                                 P(label_1 | ngram present) - purity toward label-1.
                                  1 -> every trace containing this n-gram is label-1
                                  0 -> every trace containing this n-gram is label-0
                                0.5 -> n-gram is equally split between both groups
        confidence_0   : float - in [0, 1]
                                 count_0 / (count_0 + count_1)
                                 P(label_0 | ngram present) - purity toward label-0.
                                  1 -> every trace containing this n-gram is label-0
                                  0 -> every trace containing this n-gram is label-1
        support_1      : float - in [0, 1]
                                 count_1 / total_1
                                 P(ngram | label_1) - fraction of label-1 traces
                                 that contain this n-gram.
        support_0      : float - in [0, 1]
                                 count_0 / total_0
                                 P(ngram | label_0) - fraction of label-0 traces
                                 that contain this n-gram.
        support        : float - in [0, 1]
                                 (count_1 + count_0) / (total_1 + total_0)
                                 Global frequency of the n-gram across ALL traces,
                                 regardless of label.  High support = common pattern.
        direction      : str   - "label_1" if balance > 0,
                                 "label_0" if balance < 0,
                                 "neutral"  if balance == 0
    """

    # ------------------------------------------------------------------ #
    # Step 1 - count total traces per label
    # ------------------------------------------------------------------ #
    # Total traces per label - needed to normalize balance, confidence_0, and support
    label_counts = (
        trace_labels
        .groupBy("label")
        .agg(F.count("trace_id").alias("total"))
    )
    total_1_row = label_counts.filter(F.col("label") == 1).first()
    total_0_row = label_counts.filter(F.col("label") == 0).first()
    total_1 = total_1_row["total"] if total_1_row else 1   # avoid division by zero
    total_0 = total_0_row["total"] if total_0_row else 1
    total_all = total_1 + total_0

    # ------------------------------------------------------------------ #
    # Step 2 - collect ordered activity sequences per trace
    # ------------------------------------------------------------------ #
    window_spec = Window.partitionBy("trace_id").orderBy("start_timestamp")

    activity_sequences = (
        events
        .withColumn("rank", F.row_number().over(window_spec))
        .withColumn(
            "activity",
            F.concat_ws(" ", F.col("activity"), F.col("attributes").getItem("DISPLAY_NAME"))
        )
        .groupBy("trace_id")
        .agg(
            F.collect_list(
                F.struct(F.col("rank"), F.col("activity"))
            ).alias("activity_structs")
        )
        .withColumn(
            "activities_ordered",
            F.transform(
                F.array_sort(
                    F.col("activity_structs"),
                    lambda x, y:
                        F.when(x["rank"] < y["rank"], F.lit(-1))
                         .when(x["rank"] > y["rank"], F.lit(1))
                         .otherwise(F.lit(0))
                ),
                lambda x: x["activity"]
            )
        )
        .select("trace_id", "activities_ordered")
        .join(trace_labels, on="trace_id", how="inner")
    )

    # ------------------------------------------------------------------ #
    # Step 3 - extract n-grams
    # ------------------------------------------------------------------ #
    ngram_schema = ArrayType(ArrayType(StringType()))

    @F.udf(returnType=ngram_schema)
    def extract_ngrams_udf(activities: list, size: int) -> list:
        if activities is None or len(activities) < size:
            return []
        return [activities[i: i + size] for i in range(len(activities) - size + 1)]

    ngrams_df = (
        activity_sequences
        .withColumn("ngrams", extract_ngrams_udf(F.col("activities_ordered"), F.lit(n)))
        .withColumn("ngram", F.explode("ngrams"))
        .withColumn("ngram_str", F.concat_ws("|||", F.col("ngram")))
        .select("trace_id", "label", "ngram_str")
        .distinct()   # one row per (trace, ngram) - counts traces, not occurrences
    )

    # ------------------------------------------------------------------ #
    # Step 4 - compute balance, confidence_1, confidence_0, and support
    # ------------------------------------------------------------------ #
    balance_df = (
        ngrams_df
        .groupBy("ngram_str")
        .agg(
            F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("count_1"),
            F.sum(F.when(F.col("label") == 0, 1).otherwise(0)).alias("count_0"),
        )
        # balance: normalised coverage difference
        #   (count_1 / total_1) - (count_0 / total_0)  ->  [-1, 1]
        #   +1 = in ALL label-1 traces  and  NO  label-0 traces
        #   -1 = in ALL label-0 traces  and  NO  label-1 traces
        .withColumn(
            "balance",
            (F.col("count_1") / F.lit(total_1)) - (F.col("count_0") / F.lit(total_0))
        )
        # confidence_1: P(label_1 | ngram present) - purity toward label-1
        #   count_1 / (count_1 + count_0)  ->  [0, 1]
        #   1.0 = every trace containing this n-gram is label-1
        #   0.0 = every trace containing this n-gram is label-0
        .withColumn(
            "confidence_1",
            F.col("count_1") / (F.col("count_1") + F.col("count_0"))
        )
        # confidence_0: P(label_0 | ngram present) - purity toward label-0
        #   count_0 / (count_0 + count_1)  ->  [0, 1]
        #   1.0 = every trace containing this n-gram is label-0
        #   0.0 = every trace containing this n-gram is label-1
        .withColumn(
            "confidence_0",
            F.col("count_0") / (F.col("count_0") + F.col("count_1"))
        )
        # support_1: P(ngram | label_1) - fraction of label-1 traces containing this n-gram
        #   count_1 / total_1  ->  [0, 1]
        #   1.0 = n-gram appears in every label-1 trace
        #   0.0 = n-gram never appears in any label-1 trace
        .withColumn(
            "support_1",
            F.col("count_1") / F.lit(total_1)
        )
        # support_0: P(ngram | label_0) - fraction of label-0 traces containing this n-gram
        #   count_0 / total_0  ->  [0, 1]
        #   1.0 = n-gram appears in every label-0 trace
        #   0.0 = n-gram never appears in any label-0 trace
        .withColumn(
            "support_0",
            F.col("count_0") / F.lit(total_0)
        )
        # support: global frequency across ALL traces, regardless of label
        #   (count_1 + count_0) / (total_1 + total_0)  ->  [0, 1]
        #   1.0 = n-gram appears in every trace
        #   0.0 = n-gram appears in no trace
        .withColumn(
            "support",
            (F.col("count_1") + F.col("count_0")) / F.lit(total_all)
        )
        .withColumn("ngram", F.regexp_replace(F.col("ngram_str"), r"\|\|\|", " -> "))
        .withColumn(
            "direction",
            F.when(F.col("balance") > 0, F.lit("label_1"))
             .when(F.col("balance") < 0, F.lit("label_0"))
             .otherwise(F.lit("neutral"))
        )
        .select(
            "ngram", "count_1", "count_0",
            "balance", "confidence_1", "confidence_0", "support_1", "support_0", "support",
            "direction", "ngram_str",
        )
        .orderBy(F.abs(F.col("balance")).desc())
    )

    return balance_df


def save_ngram_results(
    balance_df: DataFrame,
    output_path: str,
    fmt: str = "csv",
    direction: str = None,
    balance_threshold: float = None,
    balance_filter: str = None,
    confidence_1_threshold: float = None,
    confidence_1_filter: str = None,        # "gt", "lt", "gte", "lte"
    confidence_0_threshold: float = None,
    confidence_0_filter: str = None,      # "gt", "lt", "gte", "lte"
    support_threshold: float = None,
    support_filter: str = None,           # "gt", "lt", "gte", "lte"
) -> None:
    """
    Streams results from Spark to the driver's local filesystem
    partition-by-partition using toLocalIterator().

    Parameters
    ----------
    balance_df : DataFrame
        Direct output of discover_ngrams.
    output_path : str
        Local path on the driver (e.g. "/home/user/results/ngrams.csv").
    fmt : str
        "csv" or "json".
    direction : str, optional
        Filter rows by direction. One of: "label_1", "label_0", "neutral".
    balance_threshold : float, optional
        Value to filter balance against. Required if balance_filter is set.
    balance_filter : str, optional
        Comparison operator for balance filtering. One of:
            "gt"     -> balance >  threshold
            "lt"     -> balance <  threshold
            "gte"    -> balance >= threshold
            "lte"    -> balance <= threshold
            "abs_gt" -> |balance| >  threshold
            "abs_lt" -> |balance| <  threshold
    confidence_1_threshold : float, optional
        Value to filter confidence_1 (label-1 purity) against.
        Required if confidence_1_filter is set.
    confidence_1_filter : str, optional
        Comparison operator for confidence_1 filtering. One of: "gt", "lt", "gte", "lte".
        Example: confidence_1_threshold=0.8, confidence_1_filter="gt"
            -> only n-grams where 80%+ of containing traces are label-1.
    confidence_0_threshold : float, optional
        Value to filter confidence_0 (label-0 recall) against.
        Required if confidence_0_filter is set.
    confidence_0_filter : str, optional
        Comparison operator for confidence_0 filtering. One of: "gt", "lt", "gte", "lte".
        Example: confidence_0_threshold=0.5, confidence_0_filter="gt"
            -> only n-grams present in more than 50% of label-0 traces.
    support_threshold : float, optional
        Value to filter support (overall frequency) against.
        Required if support_filter is set.
    support_filter : str, optional
        Comparison operator for support filtering. One of: "gt", "lt", "gte", "lte".
        Example: support_threshold=0.1, support_filter="gt"
            -> only n-grams that appear in more than 10% of all traces.
    """
    fmt = fmt.lower()
    if fmt not in ("csv", "json"):
        raise ValueError(f"Unsupported format '{fmt}'. Choose 'csv' or 'json'.")

    valid_ops        = ("gt", "lt", "gte", "lte", "abs_gt", "abs_lt")
    valid_ops_simple = ("gt", "lt", "gte", "lte")

    if balance_filter is not None and balance_threshold is None:
        raise ValueError("balance_threshold must be set when balance_filter is provided.")
    if balance_filter is not None and balance_filter not in valid_ops:
        raise ValueError(f"Invalid balance_filter '{balance_filter}'. Choose from: {valid_ops}.")

    if confidence_1_filter is not None and confidence_1_threshold is None:
        raise ValueError("confidence_1_threshold must be set when confidence_1_filter is provided.")
    if confidence_1_filter is not None and confidence_1_filter not in valid_ops_simple:
        raise ValueError(f"Invalid confidence_1_filter '{confidence_1_filter}'. Choose from: {valid_ops_simple}.")

    if confidence_0_filter is not None and confidence_0_threshold is None:
        raise ValueError("confidence_0_threshold must be set when confidence_0_filter is provided.")
    if confidence_0_filter is not None and confidence_0_filter not in valid_ops_simple:
        raise ValueError(f"Invalid confidence_0_filter '{confidence_0_filter}'. Choose from: {valid_ops_simple}.")

    if support_filter is not None and support_threshold is None:
        raise ValueError("support_threshold must be set when support_filter is provided.")
    if support_filter is not None and support_filter not in valid_ops_simple:
        raise ValueError(f"Invalid support_filter '{support_filter}'. Choose from: {valid_ops_simple}.")

    if direction is not None and direction not in ("label_1", "label_0", "neutral"):
        raise ValueError(f"Invalid direction '{direction}'. Choose from: 'label_1', 'label_0', 'neutral'.")

    # ------------------------------------------------------------------ #
    # Apply filters - pushed into the DAG, executed on cluster
    # ------------------------------------------------------------------ #
    fields = ["ngram", "count_1", "count_0", "balance", "confidence_1", "confidence_0", "support_1", "support_0", "support", "direction"]
    write_df = balance_df.select(*fields)

    if direction is not None:
        write_df = write_df.filter(F.col("direction") == direction)

    if balance_filter is not None:
        balance_ops = {
            "gt":     F.col("balance")        >  balance_threshold,
            "lt":     F.col("balance")        <  balance_threshold,
            "gte":    F.col("balance")        >= balance_threshold,
            "lte":    F.col("balance")        <= balance_threshold,
            "abs_gt": F.abs(F.col("balance")) >  balance_threshold,
            "abs_lt": F.abs(F.col("balance")) <  balance_threshold,
        }
        write_df = write_df.filter(balance_ops[balance_filter])

    if confidence_1_filter is not None:
        confidence_1_ops = {
            "gt":  F.col("confidence_1") >  confidence_1_threshold,
            "lt":  F.col("confidence_1") <  confidence_1_threshold,
            "gte": F.col("confidence_1") >= confidence_1_threshold,
            "lte": F.col("confidence_1") <= confidence_1_threshold,
        }
        write_df = write_df.filter(confidence_1_ops[confidence_1_filter])

    if confidence_0_filter is not None:
        confidence_0_ops = {
            "gt":  F.col("confidence_0") >  confidence_0_threshold,
            "lt":  F.col("confidence_0") <  confidence_0_threshold,
            "gte": F.col("confidence_0") >= confidence_0_threshold,
            "lte": F.col("confidence_0") <= confidence_0_threshold,
        }
        write_df = write_df.filter(confidence_0_ops[confidence_0_filter])

    if support_filter is not None:
        support_ops = {
            "gt":  F.col("support") >  support_threshold,
            "lt":  F.col("support") <  support_threshold,
            "gte": F.col("support") >= support_threshold,
            "lte": F.col("support") <= support_threshold,
        }
        write_df = write_df.filter(support_ops[support_filter])

    # ------------------------------------------------------------------ #
    # Stream partition-by-partition to driver local file
    # ------------------------------------------------------------------ #
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:

        if fmt == "csv":
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for row in write_df.toLocalIterator():
                writer.writerow({field: row[field] for field in fields})

        elif fmt == "json":
            f.write("[\n")
            first = True
            for row in write_df.toLocalIterator():
                if not first:
                    f.write(",\n")
                json.dump({field: row[field] for field in fields}, f, ensure_ascii=False)
                first = False
            f.write("\n]")

    logger.info(f"Results written to: {output_path} (format={fmt})")


def create_network(input: str | pd.DataFrame | DataFrame) -> str:
    """
    Creates an interactive n-gram network visualisation as an HTML string.
        Parameters:
            input: Path to the input CSV file, or a pandas/Spark DataFrame containing the n-gram data.
        Returns:
            A string containing the complete HTML for the visualisation.
    """
    if isinstance(input, str):
        df = pd.read_csv(input)
    elif isinstance(input, DataFrame):
        df = input.toPandas()
    elif isinstance(input, pd.DataFrame):
        df = input
    else:
        raise ValueError("input must be a pandas DataFrame, a Spark DataFrame, or a CSV file path")
    
    df.columns = df.columns.str.strip()
    df["ngram"]     = df["ngram"].astype(str).str.strip()
    df["direction"] = df["direction"].astype(str).str.strip()

    # Back-compat: if old CSV is missing the new columns, synthesise them
    for col, default in [
        ("confidence_0", float("nan")),
        ("support_1",    float("nan")),
        ("support_0",    float("nan")),
        ("support",      float("nan")),
    ]:
        if col not in df.columns:
            df[col] = default
            print(f"[warn] Column '{col}' not found in CSV - defaulting to NaN. "
                    f"Re-run discover_ngrams() to get real values.")

    # ── Auto-detect gram length ───────────────────────────────────────────────────
    sample_ngram = df["ngram"].dropna().iloc[0]
    gram_length  = sample_ngram.count(" -> ") + 1
    print(f"[info] Detected n-gram length: {gram_length}  (n={gram_length})")

    # ── Direction palette ─────────────────────────────────────────────────────────
    DIRECTION_PALETTE = {
        "label_0": {"edge": "#4C9BE8", "highlight": "#1a6fc4", "name": "Label 0"},
        "label_1": {"edge": "#F4845F", "highlight": "#c94c1e", "name": "Label 1"},
        "neutral":  {"edge": "#A6ADC8", "highlight": "#6C7086", "name": "Neutral"},
    }
    FALLBACK_COLORS = [
        "#74C69D", "#D4A5A5", "#B5838D", "#6D6875", "#A8DADC",
        "#E9C46A", "#2A9D8F", "#E76F51", "#264653", "#F4A261",
    ]

    directions = df["direction"].unique().tolist()
    for i, d in enumerate(directions):
        if d not in DIRECTION_PALETTE:
            c = FALLBACK_COLORS[i % len(FALLBACK_COLORS)]
            DIRECTION_PALETTE[d] = {"edge": c, "highlight": c, "name": d}

    # ── Parse ngrams -> edges ──────────────────────────────────────────────────────
    # Each row:  A -> B -> C  produces edges  (A,B) and (B,C)
    # Stats are accumulated per (from_node, to_node, direction).

    edge_stats: dict = defaultdict(lambda: {
        "count_1": 0, "count_0": 0,
        "balance_sum":      0.0,
        "confidence_1_sum": 0.0,
        "confidence_0_sum": 0.0,
        "support_1_sum":    0.0,
        "support_0_sum":    0.0,
        "support_sum":      0.0,
        "rows": 0,
    })

    nodes_seen: dict       = {}    # full_label -> node_id
    node_support_max: dict = {}    # full_label -> max support seen on any incident edge

    def get_node_id(label: str) -> int:
        if label not in nodes_seen:
            nodes_seen[label] = len(nodes_seen)
            node_support_max[label] = 0.0
        return nodes_seen[label]

    def safe_float(val) -> float:
        try:
            v = float(val)
            return 0.0 if math.isnan(v) else v
        except (TypeError, ValueError):
            return 0.0

    def short_label(full: str) -> str:
        """Compact 2-line label: activity ID on line 1, short name on line 2."""
        m = re.match(r"(ACTIVITY_\d+|END|START)\s*(.*)", full)
        if m:
            act_id   = m.group(1)
            act_name = m.group(2).strip()
            if act_name:
                act_name = (act_name[:20] + "…") if len(act_name) > 22 else act_name
                return f"{act_id}\n{act_name}"
            return act_id
        return (full[:22] + "…") if len(full) > 22 else full

    for _, row in df.iterrows():
        steps = [s.strip() for s in row["ngram"].split(" -> ")]
        if len(steps) < 2:
            continue
        direction    = row["direction"]
        confidence_1 = safe_float(row["confidence_1"])
        confidence_0 = safe_float(row["confidence_0"])
        support_1    = safe_float(row["support_1"])
        support_0    = safe_float(row["support_0"])
        balance      = safe_float(row["balance"])
        support      = safe_float(row["support"])
        c1 = int(row["count_1"])
        c0 = int(row["count_0"])

        for i in range(len(steps) - 1):
            src, tgt = steps[i], steps[i + 1]
            get_node_id(src); get_node_id(tgt)
            # Track max support per node (drives node size)
            node_support_max[src] = max(node_support_max[src], support)
            node_support_max[tgt] = max(node_support_max[tgt], support)

            key = (src, tgt, direction)
            s   = edge_stats[key]
            s["count_1"]          += c1
            s["count_0"]          += c0
            s["balance_sum"]      += balance
            s["confidence_1_sum"] += confidence_1
            s["confidence_0_sum"] += confidence_0
            s["support_1_sum"]    += support_1
            s["support_0_sum"]    += support_0
            s["support_sum"]      += support
            s["rows"]             += 1

    # ── Build vis-network data ────────────────────────────────────────────────────

    # -- Nodes --
    # Node size ->  max support of any connected edge  (mapped to 20-55 px)
    max_supp = max(node_support_max.values()) if node_support_max else 1.0
    max_supp = max_supp if max_supp > 0 else 1.0

    def node_size(label: str) -> int:
        raw = node_support_max.get(label, 0.0) / max_supp
        return int(20 + raw * 35)   # 20 - 55 px

    vis_nodes = []
    for label, nid in nodes_seen.items():
        sl   = short_label(label)
        size = node_size(label)
        vis_nodes.append({
            "id":    nid,
            "label": sl,
            "title": label,
            "font":  {"size": 11, "face": "monospace"},
            "widthConstraint": {"minimum": size, "maximum": size + 20},
            "color": {
                "background": "#1e1e2e",
                "border":     "#6272a4",
                "highlight":  {"background": "#313244", "border": "#cba6f7"},
                "hover":      {"background": "#313244", "border": "#cba6f7"},
            },
            "shape":  "box",
            "margin": 8,
            "_support": round(node_support_max.get(label, 0.0), 4),
        })


    vis_edges = []
    eid = 0

    # Detect bidirectional pairs for curve offsets
    pair_count: dict  = defaultdict(int)
    for (src, tgt, _) in edge_stats:
        pair = (min(nodes_seen[src], nodes_seen[tgt]),
                max(nodes_seen[src], nodes_seen[tgt]))
        pair_count[pair] += 1

    pair_offset: dict = defaultdict(int)

    for (src, tgt, direction), s in edge_stats.items():
        src_id = nodes_seen[src]
        tgt_id = nodes_seen[tgt]
        rows   = s["rows"]

        avg_confidence_1 = s["confidence_1_sum"] / rows
        avg_confidence_0 = s["confidence_0_sum"] / rows
        avg_support_1    = s["support_1_sum"]    / rows
        avg_support_0    = s["support_0_sum"]    / rows
        avg_balance      = s["balance_sum"]      / rows
        avg_support      = s["support_sum"]      / rows
        abs_balance      = abs(avg_balance)

        palette  = DIRECTION_PALETTE[direction]

        # Width: 1 px (conf=0) -> 8 px (conf=1)
        width = max(1, round(avg_confidence_1 * 8))

        # Curve offset for bidirectional edges
        pair       = (min(src_id, tgt_id), max(src_id, tgt_id))
        total_dir  = pair_count[pair]
        offset_idx = pair_offset[pair]
        pair_offset[pair] += 1

        smooth = {"type": "dynamic"}
        if total_dir > 1:
            roundness = 0.15 + 0.15 * offset_idx
            smooth = {"type": "curvedCW", "roundness": roundness}

        dashes = abs_balance < 0.3

        title = (
            f"<b>{src}</b> -> <b>{tgt}</b><br>"
            f"Direction: <b>{palette['name']}</b><br>"
            f"Avg confidence\u2081: {avg_confidence_1:.3f}<br>"
            f"Avg confidence\u2080: {avg_confidence_0:.3f}<br>"
            f"Avg support\u2081: {avg_support_1:.3f}<br>"
            f"Avg support\u2080: {avg_support_0:.3f}<br>"
            f"Avg balance: {avg_balance:.3f}<br>"
            f"Global support: {avg_support:.4f}<br>"
            f"\u03a3 count_1: {s['count_1']}   \u03a3 count_0: {s['count_0']}<br>"
            f"Ngram rows: {rows}"
        )

        vis_edges.append({
            "id":     eid,
            "from":   src_id,
            "to":     tgt_id,
            "title":  title,
            "width":  width,
            "dashes": dashes,
            "smooth": smooth,
            "color": {
                "color":     palette["edge"],
                "highlight": palette["highlight"],
                "hover":     palette["highlight"],
                "opacity":   round(0.35 + 0.65 * abs_balance, 3),
            },
            "arrows": {"to": {"enabled": True, "scaleFactor": 0.7}},
            # raw values for JS-side filtering
            "_direction":    direction,
            "_confidence_1": round(avg_confidence_1, 4),
            "_confidence_0": round(avg_confidence_0, 4),
            "_support_1":    round(avg_support_1,    4),
            "_support_0":    round(avg_support_0,    4),
            "_balance":      round(avg_balance,      4),
            "_support":      round(avg_support,      4),
        })
        eid += 1

    # ── Legend meta ───────────────────────────────────────────────────────────────
    legend_items = [
        {"direction": d, "name": DIRECTION_PALETTE[d]["name"], "color": DIRECTION_PALETTE[d]["edge"]}
        for d in directions if d in DIRECTION_PALETTE
    ]

    # ── Serialize ─────────────────────────────────────────────────────────────────
    nodes_json  = json.dumps(vis_nodes,    ensure_ascii=False, indent=2)
    edges_json  = json.dumps(vis_edges,    ensure_ascii=False, indent=2)
    legend_json = json.dumps(legend_items, ensure_ascii=False)

    support_values  = [e["_support"] for e in vis_edges if e["_support"] > 0]
    support_max_pct = int(max(support_values) * 100) if support_values else 100

    # ── HTML ──────────────────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
    <html lang="en">
    <head>
    <meta charset="UTF-8">
    <title>N-gram Network (n={gram_length})</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.9/standalone/umd/vis-network.min.js"></script>
    <style>
        *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

        body {{
        background: #11111b;
        color: #cdd6f4;
        font-family: 'Segoe UI', system-ui, sans-serif;
        display: flex;
        flex-direction: column;
        height: 100vh;
        overflow: hidden;
        }}

        header {{
        background: #1e1e2e;
        border-bottom: 1px solid #313244;
        padding: 10px 18px;
        display: flex;
        align-items: center;
        gap: 14px;
        flex-shrink: 0;
        }}
        header h1 {{ font-size: 1rem; font-weight: 600; color: #cba6f7; white-space: nowrap; }}
        .badge {{
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 600;
        white-space: nowrap;
        background: #313244;
        }}
        .badge.green  {{ color: #a6e3a1; }}
        .badge.blue   {{ color: #89dceb; }}
        .badge.purple {{ color: #cba6f7; }}

        .controls {{
        background: #181825;
        border-bottom: 1px solid #313244;
        padding: 7px 18px;
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
        flex-shrink: 0;
        }}
        .ctrl-group {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }}
        .ctrl-group label {{
        font-size: 0.74rem;
        color: #a6adc8;
        display: flex;
        align-items: center;
        gap: 5px;
        cursor: help;
        }}
        .ctrl-group input[type=range] {{ accent-color: #cba6f7; width: 88px; }}
        .ctrl-group select, .ctrl-group button {{
        background: #313244;
        border: 1px solid #45475a;
        color: #cdd6f4;
        border-radius: 6px;
        padding: 3px 9px;
        font-size: 0.74rem;
        cursor: pointer;
        }}
        .ctrl-group button:hover {{ background: #45475a; }}
        .sep {{ width:1px; height:22px; background:#313244; flex-shrink:0; }}

        .main {{ display:flex; flex:1; overflow:hidden; }}

        aside {{
        width: 215px;
        background: #1e1e2e;
        border-right: 1px solid #313244;
        padding: 12px;
        overflow-y: auto;
        flex-shrink: 0;
        display: flex;
        flex-direction: column;
        gap: 14px;
        }}
        aside h2 {{
        font-size: 0.68rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: .08em;
        color: #6c7086;
        margin-bottom: 5px;
        }}
        .legend-item  {{ display:flex; align-items:center; gap:8px; font-size:.77rem; margin-bottom:5px; }}
        .legend-swatch {{ width:22px; height:5px; border-radius:3px; flex-shrink:0; }}

        .enc-guide {{ display:flex; flex-direction:column; gap:5px; font-size:.72rem; color:#a6adc8; }}
        .enc-row   {{ display:flex; align-items:center; gap:8px; }}
        .line-solid  {{ width:32px; height:3px; background:#cba6f7; border-radius:2px; }}
        .line-medium {{ width:32px; height:3px; background:rgba(203,166,247,.5); border-radius:2px; }}
        .line-dashed {{
        width:32px; height:3px; border-radius:2px;
        background:repeating-linear-gradient(90deg,rgba(203,166,247,.5) 0 5px,transparent 5px 9px);
        }}
        .wline {{ background:#cba6f7; border-radius:2px; width:32px; }}

        .node-guide {{ display:flex; flex-direction:column; gap:5px; font-size:.72rem; color:#a6adc8; }}
        .node-row   {{ display:flex; align-items:center; gap:8px; }}
        .nbox {{
        background:#1e1e2e; border:1.5px solid #6272a4; border-radius:3px;
        display:flex; align-items:center; justify-content:center;
        font-size:.58rem; color:#a6adc8;
        }}

        .stats {{
        background: #181825;
        border: 1px solid #313244;
        border-radius: 8px;
        padding: 9px;
        font-size: 0.72rem;
        color: #a6adc8;
        display: flex;
        flex-direction: column;
        gap: 4px;
        }}
        .stats b {{ color: #cdd6f4; }}
        .stat-row   {{ display:flex; justify-content:space-between; }}
        .stat-label {{ color:#6c7086; }}

        .vis-tooltip {{
        background: #313244 !important;
        color: #cdd6f4 !important;
        border: 1px solid #45475a !important;
        border-radius: 8px !important;
        padding: 8px 12px !important;
        font-size: 0.74rem !important;
        font-family: 'Segoe UI', system-ui, sans-serif !important;
        max-width: 300px;
        line-height: 1.6;
        }}
    </style>
    </head>
    <body>

    <header>
        <h1>🔗 N-gram Network</h1>
        <span class="badge purple">n = {gram_length}</span>
        <span class="badge green"  id="badge-nodes"></span>
        <span class="badge blue"   id="badge-edges"></span>
    </header>

    <div class="controls">
        <div class="ctrl-group">
        <label title="confidence₁ = count_1 / (count_1 + count_0) - P(label_1 | ngram present)">
            Min conf₁
            <input type="range" id="conf1-slider" min="0" max="100" value="0" step="1">
            <span id="conf1-val">0.00</span>
        </label>
        <label title="confidence₀ = count_0 / (count_0 + count_1) - P(label_0 | ngram present)">
            Min conf₀
            <input type="range" id="conf0-slider" min="0" max="100" value="0" step="1">
            <span id="conf0-val">0.00</span>
        </label>
        </div>
        <div class="sep"></div>
        <div class="ctrl-group">
        <label title="support₁ = count_1 / total_1 - fraction of label-1 traces containing this ngram">
            Min sup₁
            <input type="range" id="sup1-slider" min="0" max="100" value="0" step="1">
            <span id="sup1-val">0.00</span>
        </label>
        <label title="support₀ = count_0 / total_0 - fraction of label-0 traces containing this ngram">
            Min sup₀
            <input type="range" id="sup0-slider" min="0" max="100" value="0" step="1">
            <span id="sup0-val">0.00</span>
        </label>
        </div>
        <div class="sep"></div>
        <div class="ctrl-group">
        <label title="balance = (count_1/total_1) - (count_0/total_0)  ∈ [-1, 1]">
            Min |balance|
            <input type="range" id="bal-slider" min="0" max="100" value="0" step="1">
            <span id="bal-val">0.00</span>
        </label>
        <label title="global support = (count_1+count_0)/(total_1+total_0) - fraction of all traces">
            Min global sup
            <input type="range" id="sup-slider" min="0" max="{support_max_pct}" value="0" step="1">
            <span id="sup-val">0.00</span>
        </label>
        </div>
        <div class="sep"></div>
        <div class="ctrl-group">
        <label>Direction
            <select id="dir-filter">
            <option value="all">All</option>
            </select>
        </label>
        <label>Layout
            <select id="layout-select">
            <option value="physics">Physics</option>
            <option value="hierarchical">Hierarchical</option>
            </select>
        </label>
        <button id="btn-fit">⊞ Fit</button>
        <button id="btn-freeze">❄ Freeze</button>
        </div>
    </div>

    <div class="main">
        <aside>

        <div>
            <h2>Direction -> colour</h2>
            <div id="legend-dir"></div>
        </div>

        <div>
            <h2>|Balance| -> opacity</h2>
            <div class="enc-guide">
            <div class="enc-row"><div class="line-solid"></div>  Strong (≥0.6)</div>
            <div class="enc-row"><div class="line-medium"></div> Moderate (0.3-0.6)</div>
            <div class="enc-row"><div class="line-dashed"></div> Weak &lt;0.3 (dashed)</div>
            </div>
        </div>

        <div>
            <h2>confidence₁ -> width</h2>
            <div class="enc-guide">
            <div class="enc-row"><div class="wline" style="height:7px"></div> High (≥0.8)</div>
            <div class="enc-row"><div class="wline" style="height:4px"></div> Medium (0.4-0.8)</div>
            <div class="enc-row"><div class="wline" style="height:1px"></div> Low (&lt;0.4)</div>
            </div>
        </div>

        <div>
            <h2>Support -> node size</h2>
            <div class="node-guide">
            <div class="node-row"><div class="nbox" style="width:48px;height:22px">high</div> High support</div>
            <div class="node-row"><div class="nbox" style="width:34px;height:16px">mid</div>  Medium</div>
            <div class="node-row"><div class="nbox" style="width:22px;height:12px">low</div>  Low support</div>
            </div>
        </div>

        <div class="stats" id="stats-panel">
            <div style="color:#6c7086">Hover a node or edge<br>for details.</div>
        </div>

        </aside>

        <div id="network"></div>
    </div>

    <script>
    const ALL_NODES  = {nodes_json};
    const ALL_EDGES  = {edges_json};
    const LEGEND     = {legend_json};

    // ── Direction filter UI ──────────────────────────────────────────────────────
    const dirFilter = document.getElementById('dir-filter');
    LEGEND.forEach(item => {{
        const opt = document.createElement('option');
        opt.value = item.direction;
        opt.textContent = item.name;
        dirFilter.appendChild(opt);

        const div = document.createElement('div');
        div.className = 'legend-item';
        div.innerHTML = `<div class="legend-swatch" style="background:${{item.color}}"></div>${{item.name}}`;
        document.getElementById('legend-dir').appendChild(div);
    }});

    // ── vis datasets ─────────────────────────────────────────────────────────────
    const nodesDS = new vis.DataSet(ALL_NODES);
    const edgesDS = new vis.DataSet(ALL_EDGES);

    // ── Network options ──────────────────────────────────────────────────────────
    function buildOptions(layout) {{
        return {{
        nodes: {{
            shape: 'box', borderWidth: 1.5, shadow: false,
            font: {{ color: '#cdd6f4', size: 11, face: 'monospace' }},
        }},
        edges: {{ selectionWidth: 3, hoverWidth: 0.5 }},
        interaction: {{ hover: true, tooltipDelay: 100, zoomView: true, dragView: true }},
        physics: {{
            enabled: layout !== 'hierarchical',
            solver: 'forceAtlas2Based',
            forceAtlas2Based: {{
            gravitationalConstant: -60, centralGravity: 0.01,
            springLength: 130, springConstant: 0.08, damping: 0.6,
            }},
            stabilization: {{ iterations: 400, updateInterval: 25 }},
        }},
        layout: layout === 'hierarchical'
            ? {{ hierarchical: {{ direction: 'LR', sortMethod: 'hubsize', shakeTowards: 'leaves' }} }}
            : {{ randomSeed: 42 }},
        }};
    }}

    const container = document.getElementById('network');
    let network = new vis.Network(container, {{ nodes: nodesDS, edges: edgesDS }}, buildOptions('physics'));

    // ── Badges ───────────────────────────────────────────────────────────────────
    function updateBadges() {{
        document.getElementById('badge-nodes').textContent = nodesDS.length + ' nodes';
        document.getElementById('badge-edges').textContent = edgesDS.length + ' edges';
    }}
    updateBadges();

    // ── Filtering ────────────────────────────────────────────────────────────────
    function applyFilters() {{
        const minConf1 = parseFloat(document.getElementById('conf1-slider').value) / 100;
        const minConf0 = parseFloat(document.getElementById('conf0-slider').value) / 100;
        const minSup1  = parseFloat(document.getElementById('sup1-slider').value)  / 100;
        const minSup0  = parseFloat(document.getElementById('sup0-slider').value)  / 100;
        const minBal   = parseFloat(document.getElementById('bal-slider').value)   / 100;
        const minSup   = parseFloat(document.getElementById('sup-slider').value)   / 100;
        const dir      = dirFilter.value;

        const filtered = ALL_EDGES.filter(e =>
        e._confidence_1 >= minConf1 &&
        e._confidence_0 >= minConf0 &&
        e._support_1    >= minSup1  &&
        e._support_0    >= minSup0  &&
        Math.abs(e._balance) >= minBal &&
        e._support      >= minSup  &&
        (dir === 'all' || e._direction === dir)
        );

        const usedNodes = new Set();
        filtered.forEach(e => {{ usedNodes.add(e.from); usedNodes.add(e.to); }});
        nodesDS.clear(); edgesDS.clear();
        nodesDS.add(ALL_NODES.filter(n => usedNodes.has(n.id)));
        edgesDS.add(filtered);
        updateBadges();
    }}

    function wireSlider(sliderId, valId, divisor, decimals) {{
        document.getElementById(sliderId).addEventListener('input', function() {{
        document.getElementById(valId).textContent = (this.value / divisor).toFixed(decimals);
        applyFilters();
        }});
    }}
    wireSlider('conf1-slider', 'conf1-val', 100, 2);
    wireSlider('conf0-slider', 'conf0-val', 100, 2);
    wireSlider('sup1-slider',  'sup1-val',  100, 2);
    wireSlider('sup0-slider',  'sup0-val',  100, 2);
    wireSlider('bal-slider',   'bal-val',   100, 2);
    wireSlider('sup-slider',   'sup-val',   100, 2);
    dirFilter.addEventListener('change', applyFilters);

    // ── Layout ───────────────────────────────────────────────────────────────────
    document.getElementById('layout-select').addEventListener('change', function() {{
        network.setOptions(buildOptions(this.value));
    }});

    // ── Fit / freeze ──────────────────────────────────────────────────────────────
    document.getElementById('btn-fit').addEventListener('click',
        () => network.fit({{ animation: true }}));

    let frozen = false;
    document.getElementById('btn-freeze').addEventListener('click', function() {{
        frozen = !frozen;
        network.setOptions({{ physics: {{ enabled: !frozen }} }});
        this.textContent = frozen ? '▶ Unfreeze' : '❄ Freeze';
    }});

    // ── Sidebar stats ─────────────────────────────────────────────────────────────
    const statsPanel = document.getElementById('stats-panel');
    const emptyStats = '<div style="color:#6c7086">Hover a node or edge<br>for details.</div>';

    function statRow(label, value) {{
        return `<div class="stat-row"><span class="stat-label">${{label}}</span><b>${{value}}</b></div>`;
    }}

    network.on('hoverNode', params => {{
        const n = ALL_NODES.find(x => x.id === params.node);
        if (!n) return;
        statsPanel.innerHTML =
        '<div><b>Activity</b></div>' +
        `<div style="font-size:.69rem;color:#89b4fa;margin-bottom:4px">${{n.title}}</div>` +
        statRow('Connected edges', network.getConnectedEdges(params.node).length) +
        statRow('Max support',     n._support.toFixed(4));
    }});
    network.on('blurNode',  () => {{ statsPanel.innerHTML = emptyStats; }});

    network.on('hoverEdge', params => {{
        const e = ALL_EDGES.find(x => x.id === params.edge);
        if (!e) return;
        statsPanel.innerHTML =
        '<div><b>Edge</b></div>' +
        statRow('Direction',  e._direction) +
        statRow('confidence₁', e._confidence_1.toFixed(3)) +
        statRow('confidence₀', e._confidence_0.toFixed(3)) +
        statRow('support₁',   e._support_1.toFixed(3)) +
        statRow('support₀',   e._support_0.toFixed(3)) +
        statRow('Balance',    e._balance.toFixed(3)) +
        statRow('Global sup', e._support.toFixed(4));
    }});
    network.on('blurEdge', () => {{ statsPanel.innerHTML = emptyStats; }});

    // ── Double-click neighbourhood zoom ──────────────────────────────────────────
    network.on('doubleClick', params => {{
        if (params.nodes.length > 0) {{
        const nid = params.nodes[0];
        network.fit({{
            nodes: [nid, ...network.getConnectedNodes(nid)],
            animation: {{ duration: 600, easingFunction: 'easeInOutQuad' }},
        }});
        }} else {{
        network.fit({{ animation: true }});
        }}
    }});
    </script>
    </body>
    </html>
    """
    return html


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 ngrams.py <input.csv> [output.html]")
        print("  <input.csv>: CSV file with n-gram data (from discover_ngrams output)")
        sys.exit(1)

    html = create_network(sys.argv[1])

    with open(sys.argv[2] if len(sys.argv) > 2 else "ngram_network.html", "w", encoding="utf-8") as f:
        f.write(html)
