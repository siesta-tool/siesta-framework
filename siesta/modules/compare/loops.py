from __future__ import annotations

import csv
import json
import logging
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, functions as F

from siesta.modules.analyse.loop_detection import compute_loop_detection

logger = logging.getLogger(__name__)



def discover_loops(
    events_df: DataFrame,
    trace_labels: DataFrame,
    trace_count: int,
    support_threshold: float = 0.0,
    include_trace_ids: bool = True,
) -> Dict[str, Any]:
    """Detect self-loops and minimal non-self-loops in three comparison scopes.

    Args:
        events_df:          Sequence table DataFrame (activity, trace_id, position, start_timestamp, attributes).
        trace_labels:       DataFrame with columns ``trace_id`` and ``label`` (integer 0 / 1 produced by the comparator's 
                            separating_key logic).
        trace_count:        Total number of distinct traces in the log (``metadata.trace_count``).
        support_threshold:  Fraction [0, 1].  Loops whose ``trace_ids`` list has ``<= support_threshold * trace_count`` entries
                            are removed.  Default 0.0 keeps every loop that appears in at least one trace.
        include_trace_ids:  When *True* (default) each loop entry contains a ``trace_ids`` list.  When *False* the list is
                            stripped but ``support_count`` is still present.

    Returns:
        Dictionary with keys ``"global"``, ``"per_label"``, and
        ``"exclusive"``.  Each value is a dict with ``"self_loops"`` and
        ``"non_self_loops"`` lists.  Every loop entry contains:

        * ``pattern``       activity name or ``"A -> B ->  -> A"`` string.
        * ``support_count`` number of traces in which the loop was found.
        * ``trace_ids``     sorted list of those trace IDs (omitted when *include_trace_ids* is *False*).
    """

    # ------------------------------------------------------------------
    # Global scope
    # ------------------------------------------------------------------
    logger.info("discover_loops: computing global scope ")
    global_raw = compute_loop_detection(events_df, trace_based=True)
    global_result = _annotate_support_count(global_raw)


    # ------------------------------------------------------------------
    # Per-label scope
    # ------------------------------------------------------------------
    label_values: List[Any] = sorted(
        [row["label"] for row in trace_labels.select("label").distinct().collect()],
        key=str,
    )
    logger.info(
        "discover_loops: computing per-label scope for labels %s ", label_values
    )

    # Attach label to every event so we can filter cheaply per label.
    events_with_label = events_df.join(trace_labels, on="trace_id", how="inner")
    events_with_label.cache()

    per_label: Dict[str, Dict] = {}
    # Keyed by label-str -> set of (loop_type, pattern) found in that group.
    per_label_patterns: Dict[str, set] = {}

    for lv in label_values:
        lv_str = str(lv)
        logger.info("discover_loops:\tlabel=%s", lv_str)
        label_events = events_with_label.filter(F.col("label") == lv).drop("label")
        raw = compute_loop_detection(label_events, trace_based=True)
        result = _annotate_support_count(raw)
        per_label[lv_str] = result

        patterns: set = set()
        for entry in result["self_loops"]:
            patterns.add(("self_loop", entry["pattern"]))
        for entry in result["non_self_loops"]:
            patterns.add(("non_self_loop", entry["pattern"]))
        per_label_patterns[lv_str] = patterns

    events_with_label.unpersist()

    # ------------------------------------------------------------------
    # Exclusive scope  (loops in label L but absent from all others)
    # ------------------------------------------------------------------
    logger.info("discover_loops: computing exclusive scope ")
    exclusive: Dict[str, Dict] = {}

    for lv_str in per_label_patterns:
        other_patterns: set = set()
        for other_str, p_set in per_label_patterns.items():
            if other_str != lv_str:
                other_patterns |= p_set

        exclusive_set = per_label_patterns[lv_str] - other_patterns

        exclusive[lv_str] = {
            "self_loops": [
                e for e in per_label[lv_str]["self_loops"]
                if ("self_loop", e["pattern"]) in exclusive_set
            ],
            "non_self_loops": [
                e for e in per_label[lv_str]["non_self_loops"]
                if ("non_self_loop", e["pattern"]) in exclusive_set
            ],
        }

    # ------------------------------------------------------------------
    # Apply support threshold across all three scopes
    # ------------------------------------------------------------------
    min_count: float = support_threshold * trace_count
    global_result = _apply_threshold(global_result, min_count)
    per_label = {
        lv_str: _apply_threshold(res, min_count)
        for lv_str, res in per_label.items()
    }
    exclusive = {
        lv_str: _apply_threshold(res, min_count)
        for lv_str, res in exclusive.items()
    }

    # ------------------------------------------------------------------
    # Optionally strip trace_ids from entries
    # ------------------------------------------------------------------
    if not include_trace_ids:
        global_result = _strip_trace_ids(global_result)
        per_label = {lv_str: _strip_trace_ids(res) for lv_str, res in per_label.items()}
        exclusive = {lv_str: _strip_trace_ids(res) for lv_str, res in exclusive.items()}

    return {
        "global": global_result,
        "per_label": per_label,
        "exclusive": exclusive,
    }


# ---------------------------------------------------------------------------
# Output serialisation
# ---------------------------------------------------------------------------

def save_loops_results(result: Dict[str, Any], output_path: str, fmt: str = "json") -> None:
    """Write *result* to *output_path* in the requested format.

    Args:
        result:      The dict returned by :func:`discover_loops`.
        output_path: Destination file path (extension should already be set by
                     the caller).
        fmt:         ``"json"`` (default) or ``"csv"``.

    Raises:
        ValueError: If *fmt* is not ``"json"`` or ``"csv"``.
    """
    if fmt == "json":
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2)
        logger.info("Loops results written as JSON to %s", output_path)

    elif fmt == "csv":
        _save_csv(result, output_path)
        logger.info("Loops results written as CSV to %s", output_path)

    else:
        raise ValueError(
            f"Unsupported output format '{fmt}'. Choose 'json' or 'csv'."
        )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _annotate_support_count(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Add a ``support_count`` field (= len(trace_ids)) to every loop entry.

    ``compute_loop_detection`` is always called with ``trace_based=True`` so
    every entry already carries a ``trace_ids`` list.  We derive
    ``support_count`` here before any stripping happens.
    """
    def _add(loops: list) -> list:
        out = []
        for entry in loops:
            e = dict(entry)
            e["support_count"] = len(e.get("trace_ids", []))
            out.append(e)
        return out

    return {
        "self_loops": _add(raw.get("self_loops", [])),
        "non_self_loops": _add(raw.get("non_self_loops", [])),
    }


def _apply_threshold(
    result: Dict[str, Any], min_count: float
) -> Dict[str, Any]:
    """Keep only entries with ``support_count > min_count``."""
    def _filter(loops: list) -> list:
        return [e for e in loops if e.get("support_count", 0) > min_count]

    return {
        "self_loops": _filter(result["self_loops"]),
        "non_self_loops": _filter(result["non_self_loops"]),
    }


def _strip_trace_ids(result: Dict[str, Any]) -> Dict[str, Any]:
    """Remove ``trace_ids`` from every loop entry, keeping ``support_count``."""
    def _strip(loops: list) -> list:
        return [{k: v for k, v in e.items() if k != "trace_ids"} for e in loops]

    return {
        "self_loops": _strip(result["self_loops"]),
        "non_self_loops": _strip(result["non_self_loops"]),
    }


def _save_csv(result: Dict[str, Any], output_path: str) -> None:
    """Flatten the nested loops result into a CSV with a consistent schema.

    Columns (always present, even when trace_ids were stripped):

      scope          "global" | "per_label" | "exclusive"
      label          label value string; empty string for global scope
      loop_type      "self_loop" | "non_self_loop"
      pattern        activity name or "A -> B ->  -> A"
      support_count  number of traces containing this loop
      trace_ids      JSON-encoded sorted list; empty string when not included
    """
    FIELDNAMES = ["scope", "label", "loop_type", "pattern", "support_count", "trace_ids"]

    _LOOP_TYPE_KEYS = [
        ("self_loops", "self_loop"),
        ("non_self_loops", "non_self_loop"),
    ]

    rows: List[Dict[str, Any]] = []

    def _collect(scope: str, label_str: str, loops_dict: Dict[str, Any]) -> None:
        for list_key, type_str in _LOOP_TYPE_KEYS:
            for entry in loops_dict.get(list_key, []):
                row: Dict[str, Any] = {
                    "scope": scope,
                    "label": label_str,
                    "loop_type": type_str,
                    "pattern": entry["pattern"],
                    "support_count": entry.get("support_count", ""),
                    "trace_ids": (
                        json.dumps(entry["trace_ids"])
                        if "trace_ids" in entry
                        else ""
                    ),
                }
                rows.append(row)

    _collect("global", "", result["global"])

    for lv_str, loops in result.get("per_label", {}).items():
        _collect("per_label", lv_str, loops)

    for lv_str, loops in result.get("exclusive", {}).items():
        _collect("exclusive", lv_str, loops)

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

import pandas as pd

def create_loops_html(input: "str | Dict[str, Any]") -> str:
    """Render loop-detection results as a self-contained interactive HTML page.
 
    Follows the same approach as :func:`create_network` in ngrams.py: a single
    HTML file, vis-network 9.1.9 standalone UMD from CDN, data embedded as
    ``const`` JS arrays, filtering by clearing and re-adding to ``vis.DataSet``.
 
    Three scope tabs (global / per_label / exclusive) share one network
    container.  Switching tabs replaces the DataSet contents and resets filters.
    The sidebar offers a min-support slider, label-group checkboxes, a node
    search box, and layout / fit / freeze controls.
 
    Args:
        input: The dict returned by :func:`discover_loops`, or a path to the
               CSV written by :func:`save_loops_results`.
    """
    from collections import defaultdict
 
    # ── Load input ────────────────────────────────────────────────────────────
    if isinstance(input, str):
        import pandas as _pd
        df = _pd.read_csv(input)
        df.columns = df.columns.str.strip()
        df["scope"]         = df["scope"].astype(str).str.strip()
        df["label"]         = df["label"].fillna("").astype(str).str.strip()
        df["loop_type"]     = df["loop_type"].astype(str).str.strip()
        df["pattern"]       = df["pattern"].astype(str).str.strip()
        df["support_count"] = (
            _pd.to_numeric(df["support_count"], errors="coerce").fillna(0).astype(int)
        )
        result: Dict[str, Any] = {
            "global": {"self_loops": [], "non_self_loops": []},
            "per_label": {}, "exclusive": {},
        }
        for _, row in df.iterrows():
            sc, lv, lt = row["scope"], row["label"], row["loop_type"]
            entry = {"pattern": row["pattern"], "support_count": int(row["support_count"])}
            lk = "self_loops" if lt == "self_loop" else "non_self_loops"
            if sc == "global":
                result["global"][lk].append(entry)
            else:
                result.setdefault(sc, {}).setdefault(
                    lv, {"self_loops": [], "non_self_loops": []}
                )[lk].append(entry)
    elif isinstance(input, dict):
        result = input
    else:
        raise ValueError("input must be the discover_loops result dict or a CSV file path")
 
    # ── Colour palettes ───────────────────────────────────────────────────────
    LOOP_TYPE_COLORS = {"self_loop": "#E76F51", "non_self_loop": "#4C9BE8"}
    LABEL_PALETTE    = ["#4C9BE8", "#F4845F", "#74C69D", "#E9C46A", "#B5838D", "#6D6875"]
    labels       = sorted(result.get("per_label", {}).keys(), key=str)
    label_colors = {lv: LABEL_PALETTE[i % len(LABEL_PALETTE)] for i, lv in enumerate(labels)}
 
    # ── Helpers ───────────────────────────────────────────────────────────────
    def _merge_labeled(scope_key: str) -> dict:
        merged: dict = {"self_loops": [], "non_self_loops": []}
        for lv, ld in result.get(scope_key, {}).items():
            for entry in ld.get("self_loops", []):
                merged["self_loops"].append({**entry, "_label": lv})
            for entry in ld.get("non_self_loops", []):
                merged["non_self_loops"].append({**entry, "_label": lv})
        return merged
 
    def _build_scope(loops_dict: dict, color_by_label: bool = False):
        """Return (vis_nodes, vis_edges, max_support, label_groups)."""
        node_supp: dict = defaultdict(float)
        for lk in ("self_loops", "non_self_loops"):
            for entry in loops_dict.get(lk, []):
                sc = entry.get("support_count", 0)
                for step in entry["pattern"].split(" -> "):
                    node_supp[step.strip()] = max(node_supp[step.strip()], sc)
 
        if not node_supp:
            return [], [], 0, []
 
        vis_nodes = [
            {
                "id":    name,
                "label": name,
                "value": int(supp),   # vis-network uses `value` for size scaling
                "title": f"<b>{name}</b><br>Max support: {int(supp)}",
                "_support": int(supp),
                "color": {
                    "background": "#89B4FA", "border": "#5c9ee8",
                    "highlight":  {"background": "#CBA6F7", "border": "#9b72cf"},
                },
                "font": {"color": "#1E1E2E"},
            }
            for name, supp in node_supp.items()
        ]
 
        # Aggregate edges: one per unique (src, tgt, loop_type, label_group)
        edge_agg: dict = {}
        for lt, lk in [("self_loop", "self_loops"), ("non_self_loop", "non_self_loops")]:
            for entry in loops_dict.get(lk, []):
                pattern = entry["pattern"]
                sc      = entry.get("support_count", 0)
                lv      = entry.get("_label", "")
                steps   = [s.strip() for s in pattern.split(" -> ")]
                color   = (label_colors.get(lv, LABEL_PALETTE[0])
                           if color_by_label else LOOP_TYPE_COLORS[lt])
                pairs   = [(steps[0], steps[0])] if lt == "self_loop" else [
                    (steps[i], steps[i + 1]) for i in range(len(steps) - 1)
                ]
                for src, tgt in pairs:
                    key = (src, tgt, lt, lv)
                    if key not in edge_agg:
                        edge_agg[key] = {
                            "source": src, "target": tgt, "loop_type": lt,
                            "label_group": lv, "color": color,
                            "support": sc, "patterns": [pattern],
                        }
                    else:
                        edge_agg[key]["support"] = max(edge_agg[key]["support"], sc)
                        if pattern not in edge_agg[key]["patterns"]:
                            edge_agg[key]["patterns"].append(pattern)
 
        max_support   = max((a["support"] for a in edge_agg.values()), default=1) or 1
        scope_lgs: set = set()
        vis_edges: list = []
 
        for eid, agg in enumerate(edge_agg.values()):
            sc = agg["support"]
            lv = agg["label_group"]
            lt = agg["loop_type"]
            if lv:
                scope_lgs.add(lv)
 
            n_pat  = len(agg["patterns"])
            p_disp = agg["patterns"][0] if n_pat == 1 else f"{n_pat} patterns"
            p_all  = " | ".join(agg["patterns"][:15])
 
            tip = (f"<b>{p_disp}</b><br>Support: {sc}"
                   f"<br>Type: {lt.replace('_', ' ')}")
            if lv:
                tip += f"<br>Label: {lv}"
            if n_pat > 1:
                tip += f"<br><span style='color:#a6adc8'>{p_all}</span>"
 
            edge: dict = {
                "id":     eid,
                "from":   agg["source"],
                "to":     agg["target"],
                "color":  {"color": agg["color"], "highlight": agg["color"],
                            "hover": agg["color"]},
                "label":  str(sc),
                "title":  tip,
                "width":  1 + 5 * (sc / max_support),
                "arrows": "to",
                "font":   {"size": 10, "color": "#a6adc8", "align": "top"},
                "_support":      sc,
                "_loop_type":    lt,
                "_label_group":  lv,
                "_pattern":      p_disp,
            }
            if lt == "self_loop":
                edge["selfReference"] = {"size": 20, "angle": 0.7853981633974483}
 
            vis_edges.append(edge)
 
        return vis_nodes, vis_edges, max_support, sorted(scope_lgs, key=str)
 
    # ── Build scopes ──────────────────────────────────────────────────────────
    g_nodes,  g_edges,  g_max,  _      = _build_scope(result.get("global", {}),       color_by_label=False)
    pl_nodes, pl_edges, pl_max, pl_lgs = _build_scope(_merge_labeled("per_label"),     color_by_label=True)
    ex_nodes, ex_edges, ex_max, ex_lgs = _build_scope(_merge_labeled("exclusive"),     color_by_label=True)
 
    scope_data_js = json.dumps({
        "global":    {"nodes": g_nodes,  "edges": g_edges,  "max": g_max,  "lgs": []},
        "per_label": {"nodes": pl_nodes, "edges": pl_edges, "max": pl_max, "lgs": pl_lgs},
        "exclusive": {"nodes": ex_nodes, "edges": ex_edges, "max": ex_max, "lgs": ex_lgs},
    }, ensure_ascii=False)
 
    # ── Legend snippets (rendered in Python -> embedded as JS strings) ────────
    def _swatch(color: str, text: str) -> str:
        return (f'<div class="legend-item">'
                f'<div class="legend-swatch" style="background:{color}"></div>'
                f'{text}</div>')
 
    global_legend = "".join(
        _swatch(c, lt.replace("_", " ").title()) for lt, c in LOOP_TYPE_COLORS.items()
    )
    label_legend = "".join(
        _swatch(label_colors.get(lv, LABEL_PALETTE[0]), f"Label {lv}") for lv in labels
    ) or '<div style="color:#6c7086;font-size:.78rem">No label groups</div>'
 
    legend_js = json.dumps({
        "global":    global_legend,
        "per_label": label_legend,
        "exclusive": label_legend,
    })
 
    # ── Label-group checkboxes (per scope, rendered as HTML strings) ──────────
    def _lg_cbs(lgs: list) -> str:
        return "".join(
            f'<label class="cb-row">'
            f'<input type="checkbox" class="lg-cb" value="{lv}" checked>'
            f'<span class="lg-dot" style="background:{label_colors.get(lv, LABEL_PALETTE[0])}"></span>'
            f'Label {lv}</label>'
            for lv in lgs
        )
 
    lg_controls_js = json.dumps({
        "global":    "",
        "per_label": _lg_cbs(pl_lgs),
        "exclusive": _lg_cbs(ex_lgs),
    })
 
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Loop Detection - Comparative View</title>
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
  gap: 12px;
  flex-shrink: 0;
}}
header h1 {{ font-size: 1rem; font-weight: 600; color: #cba6f7; }}
.badge {{
  padding: 2px 10px; border-radius: 20px; font-size: 0.72rem;
  font-weight: 600; background: #313244;
}}
.badge.green  {{ color: #a6e3a1; }}
.badge.blue   {{ color: #89dceb; }}
 
.tabs {{
  background: #181825;
  border-bottom: 1px solid #313244;
  padding: 0 14px;
  display: flex;
  gap: 2px;
  flex-shrink: 0;
}}
.tab {{
  padding: 8px 20px; border: none; border-radius: 5px 5px 0 0;
  background: transparent; color: #6c7086; font-size: .84rem;
  cursor: pointer; transition: background .15s, color .15s;
  margin-bottom: -1px; border-bottom: 2px solid transparent;
}}
.tab:hover  {{ color: #cdd6f4; }}
.tab.active {{ color: #cba6f7; font-weight: 600; border-bottom-color: #cba6f7; }}
 
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
.ctrl-group {{ display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }}
.ctrl-group label {{
  font-size: 0.74rem; color: #a6adc8;
  display: flex; align-items: center; gap: 5px;
}}
.ctrl-group input[type=range] {{ accent-color: #cba6f7; width: 100px; }}
.ctrl-group select, .ctrl-group button {{
  background: #313244; border: 1px solid #45475a;
  color: #cdd6f4; border-radius: 6px;
  padding: 3px 9px; font-size: 0.74rem; cursor: pointer;
}}
.ctrl-group button:hover {{ background: #45475a; }}
.ctrl-group input[type=text] {{
  background: #313244; border: 1px solid #45475a;
  color: #cdd6f4; border-radius: 6px;
  padding: 3px 8px; font-size: 0.74rem; outline: none; width: 120px;
}}
.ctrl-group input[type=text]:focus {{ border-color: #cba6f7; }}
.sep {{ width:1px; height:22px; background:#313244; flex-shrink:0; }}
 
.main {{ display:flex; flex:1; overflow:hidden; }}
 
aside {{
  width: 210px; background: #1e1e2e;
  border-right: 1px solid #313244; padding: 12px;
  overflow-y: auto; flex-shrink: 0;
  display: flex; flex-direction: column; gap: 14px;
}}
aside h2 {{
  font-size: 0.68rem; font-weight: 700;
  text-transform: uppercase; letter-spacing: .08em;
  color: #6c7086; margin-bottom: 5px;
}}
.legend-item  {{ display:flex; align-items:center; gap:8px; font-size:.77rem; margin-bottom:5px; }}
.legend-swatch {{ width:22px; height:5px; border-radius:3px; flex-shrink:0; }}
 
.cb-row {{
  display: flex; align-items: center; gap: 7px;
  font-size: .8rem; color: #a6adc8; cursor: pointer; padding: 2px 0;
}}
.cb-row input {{ accent-color: #cba6f7; cursor: pointer; }}
.lg-dot {{
  width: 10px; height: 10px; border-radius: 50%;
  flex-shrink: 0; display: inline-block;
}}
 
.stats {{
  background: #181825; border: 1px solid #313244;
  border-radius: 8px; padding: 9px;
  font-size: 0.72rem; color: #a6adc8;
  display: flex; flex-direction: column; gap: 4px;
}}
.stats b {{ color: #cdd6f4; }}
.stat-row   {{ display:flex; justify-content:space-between; }}
.stat-label {{ color:#6c7086; }}
 
#network {{ flex:1; height:100%; }}
 
.vis-tooltip {{
  background: #313244 !important; color: #cdd6f4 !important;
  border: 1px solid #45475a !important; border-radius: 8px !important;
  padding: 8px 12px !important; font-size: 0.74rem !important;
  font-family: 'Segoe UI', system-ui, sans-serif !important;
  max-width: 320px; line-height: 1.6;
}}
</style>
</head>
<body>
 
<header>
  <h1>&#x1f501; Loop Detection</h1>
  <span class="badge green" id="badge-nodes"></span>
  <span class="badge blue"  id="badge-edges"></span>
</header>
 
<div class="tabs">
  <button class="tab active" onclick="switchScope('global')">Global</button>
  <button class="tab"        onclick="switchScope('per_label')">Per Label</button>
  <button class="tab"        onclick="switchScope('exclusive')">Exclusive</button>
</div>
 
<div class="controls">
  <div class="ctrl-group">
    <label>Min support
      <input type="range" id="supp-slider" min="0" max="100" value="0" step="1">
      <span id="supp-val">0</span>
    </label>
  </div>
  <div class="sep"></div>
  <div class="ctrl-group" id="lg-ctrl" style="display:none">
    <span style="font-size:.74rem;color:#6c7086">Labels:</span>
    <div id="lg-cbs" style="display:flex;gap:6px;flex-wrap:wrap"></div>
  </div>
  <div class="sep" id="lg-sep" style="display:none"></div>
  <div class="ctrl-group">
    <label>Node search
      <input type="text" id="search" placeholder="Activity name...">
    </label>
  </div>
  <div class="sep"></div>
  <div class="ctrl-group">
    <label>Layout
      <select id="layout-select">
        <option value="physics">Physics</option>
        <option value="hierarchical">Hierarchical</option>
      </select>
    </label>
    <button id="btn-fit">&#x229E; Fit</button>
    <button id="btn-freeze">&#x2744; Freeze</button>
  </div>
</div>
 
<div class="main">
  <aside>
    <div>
      <h2>Colour legend</h2>
      <div id="legend-div"></div>
    </div>
    <div id="lg-aside" style="display:none">
      <h2>Label groups</h2>
      <div id="lg-aside-cbs"></div>
    </div>
    <div class="stats" id="stats-panel">
      <div style="color:#6c7086">Hover a node or edge<br>for details.</div>
    </div>
  </aside>
  <div id="network"></div>
</div>
 
<script>
const SCOPE_DATA   = {scope_data_js};
const LEGENDS      = {legend_js};
const LG_CONTROLS  = {lg_controls_js};
const SCOPES       = ['global', 'per_label', 'exclusive'];
 
let curScope   = 'global';
let curNodes   = [];
let curEdges   = [];
let frozen     = false;
 
// ── vis DataSets ────────────────────────────────────────────────────────────
const nodesDS = new vis.DataSet([]);
const edgesDS = new vis.DataSet([]);
 
// ── Network options ─────────────────────────────────────────────────────────
function buildOptions(layout) {{
  return {{
    nodes: {{
      shape: 'dot', borderWidth: 1.5, shadow: false,
      scaling: {{ min: 15, max: 55, label: {{ enabled: false }} }},
      font: {{ color: '#1E1E2E', size: 11, face: 'Segoe UI' }},
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
      ? {{ hierarchical: {{ direction: 'LR', sortMethod: 'hubsize' }} }}
      : {{ randomSeed: 42 }},
  }};
}}
 
const network = new vis.Network(
  document.getElementById('network'),
  {{ nodes: nodesDS, edges: edgesDS }},
  buildOptions('physics')
);
 
// ── Scope switching ─────────────────────────────────────────────────────────
function switchScope(scope) {{
  curScope = scope;
 
  // Tab styles
  SCOPES.forEach((s, i) =>
    document.querySelectorAll('.tab')[i].classList.toggle('active', s === scope)
  );
 
  // Legend
  document.getElementById('legend-div').innerHTML = LEGENDS[scope];
 
  // Label group controls
  const lgHtml = LG_CONTROLS[scope];
  const hasLg  = lgHtml.length > 0;
  ['lg-ctrl', 'lg-sep', 'lg-aside'].forEach(id =>
    document.getElementById(id).style.display = hasLg ? '' : 'none'
  );
  if (hasLg) {{
    document.getElementById('lg-cbs').innerHTML       = lgHtml;
    document.getElementById('lg-aside-cbs').innerHTML = lgHtml;
    // Wire checkboxes - both copies
    document.querySelectorAll('.lg-cb').forEach(c => c.addEventListener('change', applyFilters));
  }}
 
  // Slider range
  const max = SCOPE_DATA[scope].max || 1;
  const sl  = document.getElementById('supp-slider');
  sl.max    = max; sl.value = 0;
  document.getElementById('supp-val').textContent = '0';
 
  // Reset search
  document.getElementById('search').value = '';
 
  // Load scope data
  curNodes = SCOPE_DATA[scope].nodes;
  curEdges = SCOPE_DATA[scope].edges;
  nodesDS.clear(); edgesDS.clear();
  nodesDS.add(curNodes);
  edgesDS.add(curEdges);
  updateBadges();
 
  document.getElementById('stats-panel').innerHTML =
    '<div style="color:#6c7086">Hover a node or edge<br>for details.</div>';
}}
 
// ── Filtering (same pattern as ngrams) ─────────────────────────────────────
function applyFilters() {{
  const minSupp  = parseInt(document.getElementById('supp-slider').value);
  const search   = document.getElementById('search').value.toLowerCase().trim();
  const selLGs   = new Set(
    [...document.querySelectorAll('.lg-cb:checked')].map(c => c.value)
  );
 
  const filteredEdges = curEdges.filter(e =>
    e._support >= minSupp &&
    (selLGs.size === 0 || e._label_group === '' || selLGs.has(e._label_group))
  );
 
  const usedNodes = new Set();
  filteredEdges.forEach(e => {{ usedNodes.add(e.from); usedNodes.add(e.to); }});
 
  const filteredNodes = curNodes.filter(n =>
    usedNodes.has(n.id) &&
    (!search || n.label.toLowerCase().includes(search))
  );
 
  nodesDS.clear(); edgesDS.clear();
  nodesDS.add(filteredNodes);
  edgesDS.add(filteredEdges);
  updateBadges();
}}
 
function updateBadges() {{
  document.getElementById('badge-nodes').textContent = nodesDS.length + ' nodes';
  document.getElementById('badge-edges').textContent = edgesDS.length + ' edges';
}}
 
// ── Slider wiring ───────────────────────────────────────────────────────────
document.getElementById('supp-slider').addEventListener('input', function() {{
  document.getElementById('supp-val').textContent = this.value;
  applyFilters();
}});
document.getElementById('search').addEventListener('input', applyFilters);
 
// ── Layout / Fit / Freeze ───────────────────────────────────────────────────
document.getElementById('layout-select').addEventListener('change', function() {{
  network.setOptions(buildOptions(this.value));
}});
document.getElementById('btn-fit').addEventListener('click',
  () => network.fit({{ animation: true }})
);
document.getElementById('btn-freeze').addEventListener('click', function() {{
  frozen = !frozen;
  network.setOptions({{ physics: {{ enabled: !frozen }} }});
  this.textContent = frozen ? '\u25B6 Unfreeze' : '\u2744 Freeze';
}});
 
// ── Sidebar stats (hover) ───────────────────────────────────────────────────
const statsPanel = document.getElementById('stats-panel');
const emptyStats = '<div style="color:#6c7086">Hover a node or edge<br>for details.</div>';
function statRow(label, value) {{
  return `<div class="stat-row"><span class="stat-label">${{label}}</span><b>${{value}}</b></div>`;
}}
 
network.on('hoverNode', params => {{
  const n = curNodes.find(x => x.id === params.node);
  if (!n) return;
  statsPanel.innerHTML =
    `<div><b>${{n.label}}</b></div>` +
    statRow('Max support', n._support) +
    statRow('Degree', network.getConnectedEdges(params.node).length);
}});
network.on('blurNode', () => {{ statsPanel.innerHTML = emptyStats; }});
 
network.on('hoverEdge', params => {{
  const e = curEdges.find(x => x.id === params.edge);
  if (!e) return;
  statsPanel.innerHTML =
    `<div><b>${{e._pattern}}</b></div>` +
    statRow('Support', e._support) +
    statRow('Type', e._loop_type.replace('_', ' ')) +
    (e._label_group ? statRow('Label', e._label_group) : '');
}});
network.on('blurEdge', () => {{ statsPanel.innerHTML = emptyStats; }});
 
// ── Double-click zoom ───────────────────────────────────────────────────────
network.on('doubleClick', params => {{
  if (params.nodes.length > 0) {{
    network.fit({{
      nodes: [params.nodes[0], ...network.getConnectedNodes(params.nodes[0])],
      animation: {{ duration: 600, easingFunction: 'easeInOutQuad' }},
    }});
  }} else {{
    network.fit({{ animation: true }});
  }}
}});
 
// ── Boot ────────────────────────────────────────────────────────────────────
switchScope('global');
</script>
</body>
</html>"""