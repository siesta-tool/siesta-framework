"""
Attribute deviation analysis pipeline - pure PySpark implementation.

Steps (configurable via the `steps` list parameter):
  0  value_freq_inter     - global rarity: inter-trace support surprise per (attribute, value)
  0  value_freq_intra     - within-trace overuse: robust z-score of intra-trace occurrence count
  1  activity_attribute   - per-(activity, attribute) distribution: surprise / z-score
  2  position_conditioned - per-(activity, position-bucket, attribute): surprise / z-score
  3  ngram_context        - per-(n-gram ending at event, attribute): surprise / z-score
  4  value_transition     - (prev→curr) value transition rarity, categorical only

Attribute keys matching a timestamp pattern are excluded automatically.
Attribute values are stored as strings; numeric keys are detected via cast-success fraction.
"""
from __future__ import annotations

import logging
from functools import reduce
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Step name constants
# ---------------------------------------------------------------------------

STEP_FREQ_INTER    = "value_freq_inter"
STEP_FREQ_INTRA    = "value_freq_intra"
STEP_ACTIVITY_ATTR = "activity_attribute"
STEP_POSITION      = "position_conditioned"
STEP_NGRAM         = "ngram_context"
STEP_TRANSITION    = "value_transition"

_STEP_TO_NAMES: Dict[int, List[str]] = {
    0: [STEP_FREQ_INTER, STEP_FREQ_INTRA],
    1: [STEP_ACTIVITY_ATTR],
    2: [STEP_POSITION],
    3: [STEP_NGRAM],
    4: [STEP_TRANSITION],
}

ALL_STEPS: List[int] = list(_STEP_TO_NAMES.keys())

# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _explode_attrs(events_df: DataFrame, excluded_keys: Optional[List[str]]) -> DataFrame:
    """Explode the attributes MapType to (trace_id, position, activity, attr_key, attr_value).

    Drops null values and automatically filters keys whose names contain
    time / timestamp / date (case-insensitive).
    """
    result = (
        events_df
        .select(
            "trace_id", "position", "activity",
            F.explode_outer("attributes").alias("attr_key", "attr_value"),
        )
        .filter(F.col("attr_key").isNotNull() & F.col("attr_value").isNotNull())
        .filter(~F.col("attr_key").rlike(r"(?i)\b(time|timestamp|date)\b"))
    )
    if excluded_keys:
        result = result.filter(~F.col("attr_key").isin(excluded_keys))
    return result


def _detect_numeric_keys(exploded: DataFrame) -> List[str]:
    """Return attr_key values where ≥90 % of values successfully cast to double."""
    rows = (
        exploded
        .withColumn("_n", F.expr("try_cast(attr_value as double)").isNotNull().cast("integer"))
        .groupBy("attr_key")
        .agg((F.sum("_n") / F.count("*")).alias("_frac"))
        .filter(F.col("_frac") >= 0.9)
        .select("attr_key")
        .collect()
    )
    return [r.attr_key for r in rows]


def _dev_record(df: DataFrame, step: str, ctx_expr: Any, threshold: float) -> DataFrame:
    """Project a scored DataFrame to the standard deviation record schema.

    ctx_expr: a Spark Column expression that becomes the 'context' field.
    """
    return df.select(
        F.col("trace_id"),
        F.col("position"),
        F.col("activity"),
        F.col("attr_key"),
        F.col("attr_value"),
        F.lit(step).alias("step"),
        ctx_expr.cast("string").alias("context"),
        F.col("score").cast("double"),
        F.lit(float(threshold)).alias("threshold"),
        (F.col("score").cast("double") >= float(threshold)).alias("is_deviation"),
    )


def _score_cat(
    df: DataFrame,
    group_cols: List[str],
    threshold: float,
    step: str,
    ctx_expr: Any,
    alpha: float,
) -> DataFrame:
    """Laplace-smoothed categorical surprise per (group_cols, attr_key, attr_value)."""
    cnt = df.groupBy(*group_cols, "attr_key", "attr_value").agg(
        F.count("*").alias("_cnt")
    )
    stats = df.groupBy(*group_cols, "attr_key").agg(
        F.count("*").alias("_total"),
        F.countDistinct("attr_value").alias("_vocab"),
    )
    scored = (
        cnt.join(F.broadcast(stats), group_cols + ["attr_key"])
        .withColumn(
            "score",
            -F.log2(
                (F.col("_cnt") + alpha)
                / (F.col("_total") + alpha * (F.col("_vocab") + 1))
            ),
        )
        .select(*group_cols, "attr_key", "attr_value", "score")
    )
    return _dev_record(df.join(scored, group_cols + ["attr_key", "attr_value"]), step, ctx_expr, threshold)


def _score_num(
    df: DataFrame,
    group_cols: List[str],
    threshold: float,
    step: str,
    ctx_expr: Any,
) -> DataFrame:
    """Robust z-score (MAD-based) per (group_cols, attr_key) for numeric attr_values."""
    # ANSI-safe numeric parsing: malformed values become NULL instead of raising.
    num = df.withColumn("_v", F.expr("try_cast(attr_value as double)")).filter(F.col("_v").isNotNull())

    med = num.groupBy(*group_cols, "attr_key").agg(
        F.percentile_approx("_v", 0.5).alias("_med")
    )
    with_med = (
        num.join(F.broadcast(med), group_cols + ["attr_key"])
        .withColumn("_ad", F.abs(F.col("_v") - F.col("_med")))
    )
    mad = with_med.groupBy(*group_cols, "attr_key").agg(
        F.percentile_approx("_ad", 0.5).alias("_mad")
    )
    scored = (
        with_med.join(F.broadcast(mad), group_cols + ["attr_key"])
        .withColumn(
            "score",
            F.when(F.col("_mad") == 0, F.lit(0.0)).otherwise(
                F.abs(F.lit(0.6745) * (F.col("_v") - F.col("_med")) / F.col("_mad"))
            ),
        )
    )
    return _dev_record(scored, step, ctx_expr, threshold)


def _add_bucket_col(events_df: DataFrame, n_buckets: int) -> DataFrame:
    """Return (trace_id, position, _bucket) with equal-width relative-position buckets."""
    w = Window.partitionBy("trace_id")
    return (
        events_df.select("trace_id", "position")
        .withColumn("_mxp", F.max("position").over(w))
        .withColumn(
            "_rel",
            F.when(F.col("_mxp") == 0, F.lit(0.0))
             .otherwise(F.col("position").cast("double") / F.col("_mxp").cast("double")),
        )
        .withColumn(
            "_bucket",
            F.least(
                F.floor(F.col("_rel") * n_buckets).cast("integer"),
                F.lit(n_buckets - 1),
            ),
        )
        .select("trace_id", "position", "_bucket")
    )


def _add_ngram_col(events_df: DataFrame, n: int) -> DataFrame:
    """Return (trace_id, position, _ngram); n-gram is the window of n activities ending at the event."""
    if n <= 1:
        return events_df.select("trace_id", "position", F.col("activity").alias("_ngram"))

    w = Window.partitionBy("trace_id").orderBy("position")
    df = events_df.select("trace_id", "position", "activity")
    lag_cols: List[str] = []
    for i in range(n - 1, 0, -1):
        c = f"_lag_{i}"
        df = df.withColumn(c, F.lag("activity", i).over(w))
        lag_cols.append(c)

    all_valid = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in lag_cols])
    df = df.withColumn(
        "_ngram",
        F.when(all_valid, F.concat_ws("->", *[F.col(c) for c in lag_cols], F.col("activity"))),
    )
    return df.select("trace_id", "position", "_ngram")


# ---------------------------------------------------------------------------
# Per-step implementations
# ---------------------------------------------------------------------------

def _step0a(exploded: DataFrame, total_traces: int, threshold: float, alpha: float) -> DataFrame:
    """Inter-trace support surprise: −log₂(n_traces_with_value / total_traces)."""
    support = (
        exploded.groupBy("attr_key", "attr_value")
        .agg(
            (F.countDistinct("trace_id") / F.lit(float(max(total_traces, 1)))).alias("_sup")
        )
        .withColumn("score", -F.log2(F.greatest(F.col("_sup").cast("double"), F.lit(1e-9))))
        .select("attr_key", "attr_value", "score")
    )
    result = exploded.join(F.broadcast(support), ["attr_key", "attr_value"])
    return _dev_record(result, STEP_FREQ_INTER, F.lit("inter_trace_support"), threshold)


def _step0b(exploded: DataFrame, threshold: float) -> DataFrame:
    """Intra-trace count z-score: how often does the value appear within one trace vs. typically."""
    intra = (
        exploded.groupBy("trace_id", "attr_key", "attr_value")
        .agg(F.count("*").alias("_cnt"))
    )
    med = intra.groupBy("attr_key", "attr_value").agg(
        F.percentile_approx("_cnt", 0.5).alias("_med")
    )
    with_med = (
        intra.join(F.broadcast(med), ["attr_key", "attr_value"])
        .withColumn("_ad", F.abs(F.col("_cnt") - F.col("_med")))
    )
    mad = with_med.groupBy("attr_key", "attr_value").agg(
        F.percentile_approx("_ad", 0.5).alias("_mad")
    )
    scored = (
        with_med.join(F.broadcast(mad), ["attr_key", "attr_value"])
        .withColumn(
            "_z",
            F.when(F.col("_mad") == 0, F.lit(0.0)).otherwise(
                F.abs(F.lit(0.6745) * (F.col("_cnt") - F.col("_med")) / F.col("_mad"))
            ),
        )
        .select("trace_id", "attr_key", "attr_value", F.col("_z").alias("score"))
    )
    result = exploded.join(F.broadcast(scored), ["trace_id", "attr_key", "attr_value"])
    return _dev_record(result, STEP_FREQ_INTRA, F.lit("intra_trace_count"), threshold)


def _step1(
    cat_exp: DataFrame, num_exp: DataFrame,
    s_thr: float, z_thr: float, alpha: float,
) -> DataFrame:
    """Per-(activity, attribute) distribution anomaly."""
    parts = [
        _score_cat(cat_exp, ["activity"], s_thr, STEP_ACTIVITY_ATTR, F.col("activity"), alpha),
        _score_num(num_exp, ["activity"], z_thr, STEP_ACTIVITY_ATTR, F.col("activity")),
    ]
    return parts[0].unionByName(parts[1])


def _step2(
    cat_exp: DataFrame, num_exp: DataFrame,
    n_buckets: int, s_thr: float, z_thr: float, alpha: float,
    events_df: DataFrame,
) -> DataFrame:
    """Per-(activity, position-bucket, attribute) distribution anomaly."""
    buckets = _add_bucket_col(events_df, n_buckets)
    ctx = F.concat_ws("|", F.concat(F.lit("act="), F.col("activity")),
                      F.concat(F.lit("bucket="), F.col("_bucket").cast("string")))

    cat_b = cat_exp.join(buckets, ["trace_id", "position"])
    num_b = num_exp.join(buckets, ["trace_id", "position"])

    return _score_cat(cat_b, ["activity", "_bucket"], s_thr, STEP_POSITION, ctx, alpha) \
        .unionByName(_score_num(num_b, ["activity", "_bucket"], z_thr, STEP_POSITION, ctx))


def _step3(
    cat_exp: DataFrame, num_exp: DataFrame,
    n: int, min_grp: int, s_thr: float, z_thr: float, alpha: float,
    events_df: DataFrame,
) -> DataFrame:
    """Per-(n-gram, attribute) distribution anomaly; groups < min_grp skipped."""
    ngrams = _add_ngram_col(events_df, n)

    cat_ng = cat_exp.join(ngrams, ["trace_id", "position"]).filter(F.col("_ngram").isNotNull())
    num_ng = num_exp.join(ngrams, ["trace_id", "position"]).filter(F.col("_ngram").isNotNull())

    # Keep only n-gram × attr_key groups that meet minimum support
    valid_ng_cat = (
        cat_ng.groupBy("_ngram", "attr_key").agg(F.count("*").alias("_gc"))
        .filter(F.col("_gc") >= min_grp).select("_ngram", "attr_key")
    )
    valid_ng_num = (
        num_ng.groupBy("_ngram", "attr_key").agg(F.count("*").alias("_gc"))
        .filter(F.col("_gc") >= min_grp).select("_ngram", "attr_key")
    )

    cat_valid = cat_ng.join(F.broadcast(valid_ng_cat), ["_ngram", "attr_key"])
    num_valid = num_ng.join(F.broadcast(valid_ng_num), ["_ngram", "attr_key"])

    return _score_cat(cat_valid, ["_ngram"], s_thr, STEP_NGRAM, F.col("_ngram"), alpha) \
        .unionByName(_score_num(num_valid, ["_ngram"], z_thr, STEP_NGRAM, F.col("_ngram")))


def _step4(
    cat_exp: DataFrame, min_grp: int, s_thr: float, alpha: float,
) -> DataFrame:
    """Value-transition rarity within traces (categorical only)."""
    w = Window.partitionBy("trace_id", "attr_key").orderBy("position")
    trans = (
        cat_exp
        .withColumn("_prev", F.lag("attr_value", 1).over(w))
        .filter(F.col("_prev").isNotNull())
        .withColumn("_trans", F.concat_ws("->", F.col("_prev"), F.col("attr_value")))
    )
    # Drop attr_keys with fewer than min_grp total transitions
    valid_keys = (
        trans.groupBy("attr_key").agg(F.count("*").alias("_n"))
        .filter(F.col("_n") >= min_grp).select("attr_key")
    )
    trans = trans.join(F.broadcast(valid_keys), "attr_key")

    trans_cnt = trans.groupBy("attr_key", "_trans").agg(F.count("*").alias("_cnt"))
    trans_stats = trans.groupBy("attr_key").agg(
        F.count("*").alias("_total"),
        F.countDistinct("_trans").alias("_vocab"),
    )
    scored = (
        trans_cnt.join(F.broadcast(trans_stats), "attr_key")
        .withColumn(
            "score",
            -F.log2(
                (F.col("_cnt") + alpha)
                / (F.col("_total") + alpha * (F.col("_vocab") + 1))
            ),
        )
        .select("attr_key", "_trans", "score")
    )
    result = trans.join(scored, ["attr_key", "_trans"])
    return _dev_record(result, STEP_TRANSITION, F.col("_prev"), s_thr)


# ---------------------------------------------------------------------------
# Output assembly
# ---------------------------------------------------------------------------

def _assemble(
    flagged_df: DataFrame,
    exploded: DataFrame,
    total_traces: int,
    support_threshold: Optional[float],
    filter_out: bool,
) -> List[Dict[str, Any]]:
    """Collect flagged rows, join support, apply optional filter, build summary records."""
    support = (
        exploded.groupBy("attr_key", "attr_value")
        .agg(
            (F.countDistinct("trace_id") / F.lit(float(max(total_traces, 1))))
            .alias("inter_trace_support")
        )
    )
    # Per-trace/activity frequency of a value for a given attribute key.
    intra_base = (
        exploded.groupBy("trace_id", "activity", "attr_key")
        .agg(F.count("*").alias("_intra_total"))
    )
    intra_val = (
        exploded.groupBy("trace_id", "activity", "attr_key", "attr_value")
        .agg(F.count("*").alias("_intra_cnt"))
    )
    intra_support = (
        intra_val.join(
            F.broadcast(intra_base),
            ["trace_id", "activity", "attr_key"],
        )
        .withColumn(
            "intra_trace_support",
            (F.col("_intra_cnt").cast("double") / F.greatest(F.col("_intra_total").cast("double"), F.lit(1.0))),
        )
        .select("trace_id", "activity", "attr_key", "attr_value", "intra_trace_support")
    )

    df = (
        flagged_df
        .join(F.broadcast(support), ["attr_key", "attr_value"])
        .join(F.broadcast(intra_support), ["trace_id", "activity", "attr_key", "attr_value"])
    )

    if support_threshold is not None:
        cond = (
            F.col("inter_trace_support") <= support_threshold
            if filter_out
            else F.col("inter_trace_support") >= support_threshold
        )
        df = df.filter(cond)

    rows = df.collect()
    if not rows:
        return []

    # Aggregate to one record per (trace_id, position, activity, attr_key, attr_value)
    summary: Dict[tuple, Dict[str, Any]] = {}
    for r in rows:
        key = (r.trace_id, r.position, r.activity, r.attr_key, r.attr_value)
        if key not in summary:
            summary[key] = {
                "inter_trace_support": round(float(r.inter_trace_support), 4),
                "intra_trace_support": round(float(r.intra_trace_support), 4),
                "flagged_by": set(),
                "scores": {},
            }
        d = summary[key]
        d["flagged_by"].add(r.step)
        d["scores"][r.step] = round(max(d["scores"].get(r.step, 0.0), float(r.score)), 4)

    return [
        {
            "trace_id": k[0],
            "position": int(k[1]),
            "activity": k[2],
            "attribute": k[3],
            "value": str(k[4]),
            "inter_trace_support": v["inter_trace_support"],
            "intra_trace_support": v["intra_trace_support"],
            "flagged_by": sorted(v["flagged_by"]),
            "scores": v["scores"],
        }
        for k, v in summary.items()
    ]


# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

_STEP_LABELS = {
    STEP_FREQ_INTER:    "Step 0a - Inter-trace rarity",
    STEP_FREQ_INTRA:    "Step 0b - Intra-trace count",
    STEP_ACTIVITY_ATTR: "Step 1 - Activity × Attribute",
    STEP_POSITION:      "Step 2 - Position-conditioned",
    STEP_NGRAM:         "Step 3 - N-gram context",
    STEP_TRANSITION:    "Step 4 - Value transitions",
}

_STEP_TIPS = {
    STEP_FREQ_INTER:    "Score = −log₂(fraction of traces containing this value). High = globally rare.",
    STEP_FREQ_INTRA:    "Robust z-score of how many times the value appears within one trace vs. typically.",
    STEP_ACTIVITY_ATTR: "Laplace-surprise (categorical) or MAD z-score (numeric) per (activity, attribute) pair.",
    STEP_POSITION:      "Same as Step 1 but conditioned on the relative position bucket within the trace.",
    STEP_NGRAM:         "Same as Step 1 but conditioned on the n-gram of activities ending at this event.",
    STEP_TRANSITION:    "Surprise of the (prev_value→curr_value) transition for categorical attributes.",
}


def render_html(records: List[Dict[str, Any]], log_name: str, active_steps: List[str]) -> str:
    """Render an interactive HTML deviation report from summary records."""
    import json as _json

    step_cols   = [s for s in _STEP_LABELS if s in active_steps]
    disp_cols   = ["trace_id", "position", "activity", "attribute", "value",
                   "inter_trace_support", "intra_trace_support"] + step_cols

    rows_data = []
    for rec in records:
        r: Dict[str, Any] = {
            "trace_id": rec["trace_id"], "position": rec["position"],
            "activity": rec["activity"], "attribute": rec["attribute"],
            "value": rec["value"],
            "inter_trace_support": rec.get("inter_trace_support"),
            "intra_trace_support": rec.get("intra_trace_support"),
        }
        for sc in step_cols:
            r[sc] = rec["scores"].get(sc)
        rows_data.append(r)

    all_scores  = [r[s] for r in rows_data for s in step_cols if r.get(s) is not None]
    min_s       = round(min(all_scores, default=3.0), 1)
    max_s       = round(max(all_scores, default=10.0), 1)
    slider_max  = round(max(max_s + 1.0, 10.0), 1)

    n_rec    = len(records)
    n_traces = len({r["trace_id"] for r in records})
    n_attrs  = len({r["attribute"] for r in records})

    col_tips = {
        "trace_id": "Unique process instance identifier.",
        "position": "0-based event index within the trace.",
        "activity": "Activity type of the event.",
        "attribute": "Attribute whose value was flagged.",
        "value": "Actual attribute value at this event.",
        "inter_trace_support": "Fraction of traces containing this value [0-1]. Lower = rarer.",
        "intra_trace_support": "Within this trace and activity, fraction of events with this attribute key that take this value [0-1]. Higher = more frequent in local activity context.",
        **_STEP_TIPS,
    }

    data_j  = _json.dumps(rows_data, ensure_ascii=False)
    scols_j = _json.dumps(step_cols)
    dcols_j = _json.dumps(disp_cols)
    labs_j  = _json.dumps(_STEP_LABELS)
    tips_j  = _json.dumps(col_tips)

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Attribute Deviations - {log_name}</title>
<style>
*,*::before,*::after{{box-sizing:border-box}}
body{{font-family:sans-serif;padding:20px;background:#f7f8fc;color:#222;margin:0}}
.banner{{background:#1a1a2e;color:#fff;padding:14px 20px;border-radius:8px;margin-bottom:14px}}
.banner h2{{margin:0 0 10px;font-size:18px}}
.stats{{font-size:13px;display:flex;flex-wrap:wrap;gap:4px 0;align-items:center}}
.stats .sep{{margin:0 10px;opacity:.4}}
.ctrl-bar{{background:#fff;border:1px solid #e0e4ef;border-radius:8px;padding:14px 18px;
  margin-bottom:12px;display:flex;flex-wrap:wrap;gap:18px;align-items:flex-end}}
.ctrl{{display:flex;flex-direction:column;gap:5px}}
.ctrl>label{{font-size:12px;font-weight:600;color:#555}}
.ctrl input[type=range]{{width:220px;accent-color:#1a1a2e;cursor:pointer}}
.ctrl input[type=text]{{padding:5px 10px;border:1px solid #ccc;border-radius:5px;font-size:13px;width:220px}}
.ctrl input[type=text]:focus{{outline:none;border-color:#1a1a2e}}
.step-group{{display:flex;flex-wrap:wrap;gap:6px}}
.slbl{{display:flex;align-items:center;gap:5px;font-size:12px;cursor:pointer;
  background:#f0f2f8;padding:4px 10px;border-radius:12px;user-select:none}}
.slbl:hover{{background:#dde2f0}}
.slbl input{{accent-color:#1a1a2e;cursor:pointer}}
.tv{{display:inline-block;background:#1a1a2e;color:#fff;
  padding:2px 9px;border-radius:4px;font-size:13px;min-width:40px;text-align:center}}
.rc{{font-size:13px;color:#555;margin-left:auto;align-self:center}}
.rc b{{color:#1a1a2e;font-size:17px}}
.legend{{font-size:12px;color:#666;margin-bottom:10px;display:flex;gap:6px;flex-wrap:wrap}}
.legend span{{padding:3px 10px;border-radius:4px}}
.tw{{overflow-x:auto;border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,.08)}}
table{{border-collapse:collapse;width:100%;font-size:12px;background:#fff}}
th{{background:#1a1a2e;color:#fff;padding:8px 11px;text-align:left;white-space:nowrap;position:sticky;top:0;z-index:2}}
td{{padding:6px 11px;border-bottom:1px solid #eef0f5;white-space:nowrap}}
tr:hover td{{background:#e8f0fe!important}}
.sm{{background:#ffe066}}.sh{{background:#ffa500;color:#fff}}.sc{{background:#ff4c4c;color:#fff;font-weight:bold}}
.sn{{background:#f5f5f5;color:#bbb;text-align:center}}
#tt{{display:none;position:fixed;background:#1a1a2e;color:#fff;font-size:12px;line-height:1.6;
  padding:9px 13px;border-radius:6px;width:300px;white-space:normal;
  box-shadow:0 4px 16px rgba(0,0,0,.3);z-index:99999;pointer-events:none}}
.tip{{cursor:help}}
</style></head><body>
<div class="banner">
  <h2>Attribute Deviation Report - {log_name}</h2>
  <div class="stats">
    <span>Flagged records: <b>{n_rec}</b></span><span class="sep">|</span>
    <span>Affected traces: <b>{n_traces}</b></span><span class="sep">|</span>
    <span>Affected attributes: <b>{n_attrs}</b></span>
  </div>
</div>
<div class="ctrl-bar">
  <div class="ctrl">
    <label>Min score threshold &nbsp;<span class="tv" id="tv">{min_s}</span></label>
    <input type="range" id="ts" min="{min_s}" max="{slider_max}" step="0.1" value="{min_s}"
           oninput="onT(this.value)">
  </div>
  <div class="ctrl"><label>Active steps</label><div class="step-group" id="sg"></div></div>
  <div class="ctrl">
    <label>Search</label>
    <input type="text" id="sr" placeholder="trace / activity / attribute / value…" oninput="render()">
  </div>
  <div class="rc">Showing <b id="rc">0</b> rows</div>
</div>
<div class="legend">
  <span class="sm">moderate (≥ threshold)</span>
  <span class="sh">high (≥ 4.0)</span>
  <span class="sc">critical (≥ 6.0)</span>
  <span class="sn">- absent / below threshold</span>
</div>
<div class="tw"><table><thead><tr id="hd"></tr></thead><tbody id="tb"></tbody></table></div>
<div id="tt"></div>
<script>
var _tt=document.getElementById('tt');
document.addEventListener('mouseover',function(e){{
  var el=e.target.closest('.tip');if(!el){{_tt.style.display='none';return;}}
  var t=el.getAttribute('data-tip');if(!t){{_tt.style.display='none';return;}}
  _tt.textContent=t;_tt.style.display='block';pos(e);
}});
document.addEventListener('mousemove',function(e){{if(_tt.style.display==='block')pos(e);}});
document.addEventListener('mouseout',function(e){{if(!e.target.closest('.tip'))_tt.style.display='none';}});
function pos(e){{var x=e.clientX+14,y=e.clientY-10;
  if(x+310>window.innerWidth)x=e.clientX-310;
  if(y+_tt.offsetHeight>window.innerHeight)y=e.clientY-_tt.offsetHeight-10;
  _tt.style.left=x+'px';_tt.style.top=y+'px';}}
const D={data_j};const SC={scols_j};const DC={dcols_j};const SL={labs_j};const TP={tips_j};
SC.forEach(function(c){{
  var l=document.createElement('label');l.className='slbl';
  var cb=document.createElement('input');cb.type='checkbox';cb.id='cb-'+c;cb.checked=true;
  cb.addEventListener('change',render);l.appendChild(cb);
  l.appendChild(document.createTextNode(' '+(SL[c]||c)));
  document.getElementById('sg').appendChild(l);
}});
DC.forEach(function(c){{
  var th=document.createElement('th');var tip=TP[c];
  if(tip){{th.className='tip';th.setAttribute('data-tip',tip);th.textContent=c;
    var h=document.createElement('span');h.style.cssText='opacity:.5;font-size:10px;margin-left:4px';
    h.textContent='(?)';th.appendChild(h);}}else{{th.textContent=c;}}
  document.getElementById('hd').appendChild(th);
}});
function onT(v){{document.getElementById('tv').textContent=parseFloat(v).toFixed(1);render();}}
function asc(){{return SC.filter(function(c){{var cb=document.getElementById('cb-'+c);return cb&&cb.checked;}});}}
function cls(v,t){{if(v===null||v===undefined)return'sn';if(v>=6)return'sc';if(v>=4)return'sh';if(v>=t)return'sm';return'sn';}}
function esc(s){{return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}}
function render(){{
  var t=parseFloat(document.getElementById('ts').value);
  var ac=asc();var sr=document.getElementById('sr').value.toLowerCase().trim();
  var rows=[];
  D.forEach(function(row){{
    var mx=-Infinity;ac.forEach(function(c){{if(row[c]!==null&&row[c]>mx)mx=row[c];}});
    if(mx<t)return;
    if(sr){{var h=['trace_id','activity','attribute','value'].map(function(k){{return(row[k]||'').toLowerCase();}}).join(' ');if(h.indexOf(sr)===-1)return;}}
    var cells='';
    DC.forEach(function(c){{
            if(SC.indexOf(c)>=0){{var v=row[c];cells+='<td class="'+cls(v,t)+'">'+(v===null||v===undefined?'&ndash;':v.toFixed(3))+'</td>';}}
      else{{var v=row[c];cells+='<td>'+(v===null||v===undefined?'':esc(String(v)))+'</td>';}}
    }});
    rows.push('<tr>'+cells+'</tr>');
  }});
  document.getElementById('tb').innerHTML=rows.join('');
  document.getElementById('rc').textContent=rows.length;
}}
render();
</script></body></html>"""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def compute_attribute_deviations(
    events_df: DataFrame,
    total_traces: int,
    steps: List[int],
    excluded_keys: Optional[List[str]],
    surprise_threshold: float,
    zscore_threshold: float,
    n_buckets: int,
    ngram_n: int,
    min_group_size: int,
    support_threshold: Optional[float],
    filter_out: bool,
    alpha: float = 1.0,
) -> tuple[List[Dict[str, Any]], List[str]]:
    """Run the attribute deviation pipeline on a Spark sequence table.

    Returns:
        (records, active_step_names) where records is the list of deviation
        summary dicts and active_step_names is needed by render_html.
    """
    exploded = _explode_attrs(events_df, excluded_keys)
    exploded.cache()

    numeric_keys = _detect_numeric_keys(exploded) if any(s in steps for s in [1, 2, 3]) else []

    cat_exp = exploded.filter(~F.col("attr_key").isin(numeric_keys)) if numeric_keys else exploded
    num_exp = exploded.filter(F.col("attr_key").isin(numeric_keys)) if numeric_keys else exploded.filter(F.lit(False))

    parts: List[DataFrame] = []
    active: List[str] = []

    if 0 in steps:
        logger.info("Step 0 - value frequency …")
        parts.append(_step0a(exploded, total_traces, surprise_threshold, alpha))
        parts.append(_step0b(exploded, zscore_threshold))
        active += [STEP_FREQ_INTER, STEP_FREQ_INTRA]

    if 1 in steps:
        logger.info("Step 1 - activity × attribute …")
        parts.append(_step1(cat_exp, num_exp, surprise_threshold, zscore_threshold, alpha))
        active.append(STEP_ACTIVITY_ATTR)

    if 2 in steps:
        logger.info("Step 2 - position-conditioned …")
        parts.append(_step2(cat_exp, num_exp, n_buckets, surprise_threshold, zscore_threshold, alpha, events_df))
        active.append(STEP_POSITION)

    if 3 in steps:
        logger.info("Step 3 - %d-gram context …", ngram_n)
        parts.append(_step3(cat_exp, num_exp, ngram_n, min_group_size, surprise_threshold, zscore_threshold, alpha, events_df))
        active.append(STEP_NGRAM)

    if 4 in steps:
        logger.info("Step 4 - value transitions …")
        parts.append(_step4(cat_exp, min_group_size, surprise_threshold, alpha))
        active.append(STEP_TRANSITION)

    if not parts:
        exploded.unpersist()
        return [], []

    combined = reduce(lambda a, b: a.unionByName(b), parts)
    flagged  = combined.filter(F.col("is_deviation"))
    flagged.cache()

    records = _assemble(flagged, exploded, total_traces, support_threshold, filter_out)

    flagged.unpersist()
    exploded.unpersist()

    logger.info("Attribute deviation pipeline complete - %d flagged records.", len(records))
    return records, active
