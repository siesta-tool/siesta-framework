#!/usr/bin/env python3
"""
filter_loops.py  —  Post-process comparator loop-detection output.

FILTERING RULE
--------------
A (pattern, trace) occurrence is removed when BOTH conditions hold:

  1. Every consecutive activity step in the pattern is a known valid
     transition (present in the valid-transitions CSV).
  2. The loop appeared exactly once in that trace  (occurrence_count == 1).

Rationale: a single traversal of a path that is entirely made up of valid
transitions could simply be normal process flow that accidentally "looks"
like a loop.  If the same loop fires more than once in a trace it is
considered a genuine repetition regardless of whether the transitions are
individually valid.

When ``trace_occurrences`` is absent from the input (produced with
``include_trace_occurrences=false``), condition 2 cannot be evaluated and
the ENTIRE ENTRY is dropped whenever condition 1 holds.

After filtering per entry:
  - ``trace_occurrences``, ``support_count``, and ``support`` are recomputed.
  - Entries with no surviving traces are dropped.
  - ``support_per_label`` on global-scope entries is rebuilt from the
    filtered per-label data so all scopes stay consistent.

USAGE
-----
    python filter_loops.py \\
        --loops   comparator_output.json \\
        --valid   valid_transitions.csv  \\
        --output  filtered_output        \\
        --format  json                   \\
        [--from-col SOURCE_COL]          \\
        [--to-col   TARGET_COL]          \\
        [--vis]

    # Both JSON and CSV output at once:
    python filter_loops.py --loops out.json --valid tr.csv --output filtered --format both --vis

ARGUMENTS
---------
--loops       Path to the comparator loops result  (.json or .csv).
              For JSON, pass the .json path directly.
              For CSV, pass the .csv path; a sibling .json with the same
              stem is also accepted.
--valid       Path to the valid-transitions CSV.
--output      Output path prefix (extension(s) appended automatically).
--format      Output format: json (default), csv, or both.
--from-col    Column name for the source activity in the valid-transitions
              CSV (default: auto-detected).
--to-col      Column name for the target activity (default: auto-detected).
--vis         Also write an HTML visualisation  (<output>.html).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Shared constants (must stay in sync with loops.py)
# ---------------------------------------------------------------------------

_ALL_LOOP_LIST_KEYS: List[Tuple[str, str]] = [
    ("self_loops",        "self_loop"),
    ("non_self_loops",    "non_self_loop"),
    ("repeated_patterns", "repeated_pattern"),
]

_TYPE_TO_LIST_KEY: Dict[str, str] = {lt: lk for lk, lt in _ALL_LOOP_LIST_KEYS}

_EMPTY_SCOPE = lambda: {lk: [] for lk, _ in _ALL_LOOP_LIST_KEYS}


# ===========================================================================
# 1.  Load valid transitions
# ===========================================================================

def load_valid_transitions(
    path: str,
    from_col: Optional[str] = None,
    to_col:   Optional[str] = None,
) -> Set[Tuple[str, str]]:
    """Return a set of (source, target) string pairs from *path*.

    Column auto-detection tries these names (case-insensitive) in order:
      source: from, source, activity_from, src, a, first
      target: to,   target, activity_to,   tgt, b, second

    If the file has exactly two columns and no explicit overrides are given,
    those two columns are used regardless of their names.
    """
    _FROM = ["from", "source", "activity_from", "src", "a", "first"]
    _TO   = ["to",   "target", "activity_to",   "tgt", "b", "second"]

    with open(path, newline="", encoding="utf-8") as fh:
        reader  = csv.DictReader(fh)
        headers = reader.fieldnames or []
        lower   = [h.strip().lower() for h in headers]

        if len(headers) == 2 and from_col is None and to_col is None:
            fc, tc = headers[0], headers[1]
        else:
            fc = from_col or next(
                (headers[i] for i, h in enumerate(lower) if h in _FROM), None
            )
            tc = to_col or next(
                (headers[i] for i, h in enumerate(lower) if h in _TO), None
            )

        if not fc or not tc:
            raise ValueError(
                f"Cannot detect source/target columns in '{path}'.\n"
                f"  Found headers: {headers}\n"
                f"  Use --from-col / --to-col to specify them explicitly."
            )

        valid: Set[Tuple[str, str]] = set()
        for row in reader:
            src = str(row.get(fc, "")).strip()
            tgt = str(row.get(tc, "")).strip()
            if src and tgt:
                valid.add((src, tgt))

    return valid


# ===========================================================================
# 2.  Pattern → transitions
# ===========================================================================

def extract_transitions(pattern: str, loop_type: str) -> List[Tuple[str, str]]:
    """Return the (from, to) activity pairs encoded in a loop pattern string.

    Examples
    --------
    self_loop        "A"           → [("A", "A")]
    non_self_loop    "A -> B -> A" → [("A", "B"), ("B", "A")]
    repeated_pattern "A -> B -> C" → [("A", "B"), ("B", "C")]
    """
    if loop_type == "self_loop":
        act = pattern.strip()
        return [(act, act)]
    acts = [a.strip() for a in pattern.split(" -> ")]
    return [(acts[i], acts[i + 1]) for i in range(len(acts) - 1)]


def all_transitions_valid(
    pattern:   str,
    loop_type: str,
    valid:     Set[Tuple[str, str]],
) -> bool:
    """True when every step in the pattern is a known valid transition."""
    return all(t in valid for t in extract_transitions(pattern, loop_type))


# ===========================================================================
# 3.  Entry-level filtering
# ===========================================================================

def _infer_total(entry: Dict[str, Any]) -> int:
    """Infer the original group/trace total from support_count / support."""
    sc = entry.get("support_count", 0)
    s  = entry.get("support", 0.0)
    if s and s > 0:
        return max(sc, round(sc / s))
    return sc or 1


def filter_entry(
    entry:     Dict[str, Any],
    loop_type: str,
    valid:     Set[Tuple[str, str]],
) -> Optional[Dict[str, Any]]:
    """Filter a single loop entry against *valid*.

    Returns the (possibly modified) entry dict, or ``None`` if the entry
    should be dropped entirely.
    """
    pattern = entry["pattern"]

    # Fast exit: at least one step is not a valid transition → keep as-is.
    if not all_transitions_valid(pattern, loop_type, valid):
        return entry

    trace_occ: Dict[str, int] = entry.get("trace_occurrences", {})

    # No per-trace occurrence data → we cannot apply the occurrence-count
    # condition, so we conservatively drop the whole entry.
    if not trace_occ:
        return None

    # Retain only traces where the loop fired more than once.
    surviving = {tid: cnt for tid, cnt in trace_occ.items() if cnt > 1}

    if surviving == trace_occ:
        return entry  # nothing to filter

    if not surviving:
        return None   # all occurrences were single-fire → drop

    total     = _infer_total(entry)
    new_entry = dict(entry)
    new_entry["trace_occurrences"] = surviving
    new_entry["support_count"]     = len(surviving)
    new_entry["support"]           = round(len(surviving) / total, 6)
    return new_entry


# ===========================================================================
# 4.  Scope-level filtering
# ===========================================================================

def filter_scope(
    scope_dict: Dict[str, Any],
    valid:      Set[Tuple[str, str]],
) -> Dict[str, Any]:
    """Apply ``filter_entry`` across all loop lists in a scope dict."""
    result: Dict[str, Any] = {}
    for list_key, loop_type in _ALL_LOOP_LIST_KEYS:
        kept = []
        for entry in scope_dict.get(list_key, []):
            out = filter_entry(entry, loop_type, valid)
            if out is not None:
                kept.append(out)
        result[list_key] = kept
    return result


# ===========================================================================
# 5.  Rebuild support_per_label on global entries
# ===========================================================================

def rebuild_support_per_label(
    global_result:      Dict[str, Any],
    filtered_per_label: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Recompute ``support_per_label`` on global entries from filtered
    per-label data so that global and per-label scopes stay consistent."""

    label_values = list(filtered_per_label.keys())
    _ZERO        = {"support": 0.0, "support_count": 0}

    # Build lookup: (loop_type, pattern) → {label → {support, support_count}}
    lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for lv, label_res in filtered_per_label.items():
        for list_key, loop_type in _ALL_LOOP_LIST_KEYS:
            for entry in label_res.get(list_key, []):
                key = (loop_type, entry["pattern"])
                lookup.setdefault(key, {})[lv] = {
                    "support":       entry["support"],
                    "support_count": entry["support_count"],
                }

    def _rebuild(loops: List[Dict], loop_type: str) -> List[Dict]:
        out = []
        for entry in loops:
            e = dict(entry)
            e["support_per_label"] = {
                lv: lookup.get((loop_type, entry["pattern"]), {}).get(lv, _ZERO)
                for lv in label_values
            }
            out.append(e)
        return out

    return {
        list_key: _rebuild(global_result.get(list_key, []), loop_type)
        for list_key, loop_type in _ALL_LOOP_LIST_KEYS
    }


# ===========================================================================
# 6.  Top-level filter orchestration
# ===========================================================================

def filter_result(
    result: Dict[str, Any],
    valid:  Set[Tuple[str, str]],
) -> Dict[str, Any]:
    """Apply filtering across all scopes and rebuild derived fields."""

    filtered_global    = filter_scope(result.get("global",   {}), valid)
    filtered_per_label = {
        lv: filter_scope(scope, valid)
        for lv, scope in result.get("per_label", {}).items()
    }
    filtered_exclusive = {
        lv: filter_scope(scope, valid)
        for lv, scope in result.get("exclusive", {}).items()
    }

    # Rebuild support_per_label on global entries from filtered per-label data.
    if filtered_per_label:
        filtered_global = rebuild_support_per_label(
            filtered_global, filtered_per_label
        )

    return {
        "global":    filtered_global,
        "per_label": filtered_per_label,
        "exclusive": filtered_exclusive,
    }


# ===========================================================================
# 7.  Diff / stats
# ===========================================================================

def _count_entries(result: Dict[str, Any]) -> int:
    total = 0
    for scope_dict in [result.get("global", {})] + \
                       list(result.get("per_label", {}).values()) + \
                       list(result.get("exclusive", {}).values()):
        for lk, _ in _ALL_LOOP_LIST_KEYS:
            total += len(scope_dict.get(lk, []))
    return total


def _count_trace_pairs(result: Dict[str, Any]) -> int:
    """Count total (entry, trace) pairs across all scopes."""
    total = 0
    for scope_dict in [result.get("global", {})] + \
                       list(result.get("per_label", {}).values()) + \
                       list(result.get("exclusive", {}).values()):
        for lk, _ in _ALL_LOOP_LIST_KEYS:
            for entry in scope_dict.get(lk, []):
                total += len(entry.get("trace_occurrences", {})) or 1
    return total


# ===========================================================================
# 8.  I/O
# ===========================================================================

def load_loops(path: str) -> Dict[str, Any]:
    """Load a comparator loops result from a JSON or flat CSV file."""
    p = Path(path)

    # Prefer JSON: accept explicit .json path or a bare stem
    json_candidates = [p, p.with_suffix(".json"), Path(str(p) + ".json")]
    for candidate in json_candidates:
        if candidate.exists() and candidate.suffix.lower() == ".json":
            with open(candidate, encoding="utf-8") as fh:
                return json.load(fh)

    # Fall back to CSV
    if p.exists() and p.suffix.lower() == ".csv":
        return _load_loops_csv(str(p))

    raise FileNotFoundError(
        f"Cannot find a JSON or CSV loops file at '{path}'. "
        f"Tried: {[str(c) for c in json_candidates]}"
    )


def _load_loops_csv(path: str) -> Dict[str, Any]:
    """Reconstruct the nested result dict from a flat loops CSV."""
    result: Dict[str, Any] = {
        "global":    _EMPTY_SCOPE(),
        "per_label": {},
        "exclusive": {},
    }

    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            scope    = row.get("scope", "").strip()
            label    = row.get("label", "").strip()
            lt       = row.get("loop_type", "").strip()
            list_key = _TYPE_TO_LIST_KEY.get(lt)
            if not list_key:
                continue

            entry: Dict[str, Any] = {"pattern": row.get("pattern", "").strip()}

            raw_support = row.get("support", "").strip()
            if raw_support:
                try:
                    entry["support"] = float(raw_support)
                except ValueError:
                    pass

            raw_count = row.get("support_count", "").strip()
            if raw_count:
                try:
                    entry["support_count"] = int(raw_count)
                except ValueError:
                    pass

            raw_spl = row.get("support_per_label", "").strip()
            if raw_spl:
                try:
                    entry["support_per_label"] = json.loads(raw_spl)
                except (json.JSONDecodeError, ValueError):
                    pass

            raw_to = row.get("trace_occurrences", "").strip()
            if raw_to:
                try:
                    entry["trace_occurrences"] = json.loads(raw_to)
                except (json.JSONDecodeError, ValueError):
                    pass

            if scope == "global":
                result["global"][list_key].append(entry)
            elif scope == "per_label":
                result["per_label"].setdefault(label, _EMPTY_SCOPE())[list_key].append(entry)
            elif scope == "exclusive":
                result["exclusive"].setdefault(label, _EMPTY_SCOPE())[list_key].append(entry)

    return result


def save_json(result: Dict[str, Any], path: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2, ensure_ascii=False)


def save_csv(result: Dict[str, Any], path: str) -> None:
    FIELDNAMES = [
        "scope", "label", "loop_type", "pattern",
        "support", "support_count", "support_per_label", "trace_occurrences",
    ]
    rows: List[Dict[str, Any]] = []

    def _collect(scope: str, label_str: str, loops_dict: Dict[str, Any]) -> None:
        for list_key, type_str in _ALL_LOOP_LIST_KEYS:
            for entry in loops_dict.get(list_key, []):
                rows.append({
                    "scope":             scope,
                    "label":             label_str,
                    "loop_type":         type_str,
                    "pattern":           entry.get("pattern", ""),
                    "support":           entry.get("support", ""),
                    "support_count":     entry.get("support_count", ""),
                    "support_per_label": (
                        json.dumps(entry["support_per_label"], ensure_ascii=False)
                        if "support_per_label" in entry else ""
                    ),
                    "trace_occurrences": (
                        json.dumps(entry["trace_occurrences"], ensure_ascii=False)
                        if "trace_occurrences" in entry else ""
                    ),
                })

    _collect("global", "", result.get("global", {}))
    for lv, scope in result.get("per_label", {}).items():
        _collect("per_label", lv, scope)
    for lv, scope in result.get("exclusive", {}).items():
        _collect("exclusive", lv, scope)

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


# ===========================================================================
# 9.  HTML rendering  (identical layout to loops.py :: create_loops_html)
# ===========================================================================

def save_html(
    result:       Dict[str, Any],
    path:         str,
    valid:        Set[Tuple[str, str]],
    stats_before: int,
    stats_after:  int,
) -> None:
    """Write a self-contained interactive HTML table to *path*."""

    rows: List[Dict[str, Any]] = []

    def _collect(scope: str, label: str, loops_dict: Dict[str, Any]) -> None:
        for lk, lt in _ALL_LOOP_LIST_KEYS:
            for entry in loops_dict.get(lk, []):
                rows.append({
                    "scope":             scope,
                    "label":             label,
                    "loop_type":         lt,
                    "pattern":           entry.get("pattern", ""),
                    "support":           round(float(entry.get("support", 0.0)), 6),
                    "support_count":     int(entry.get("support_count", 0)),
                    "support_per_label": entry.get("support_per_label", {}),
                    "trace_occurrences": entry.get("trace_occurrences", {}),
                })

    _collect("global", "", result.get("global", {}))
    for lv, scope in result.get("per_label", {}).items():
        _collect("per_label", lv, scope)
    for lv, scope in result.get("exclusive", {}).items():
        _collect("exclusive", lv, scope)

    max_support = max((r["support_count"] for r in rows), default=1)
    rows_json   = json.dumps(rows, ensure_ascii=False)

    removed          = stats_before - stats_after
    valid_count      = len(valid)
    filter_summary   = (
        f"{removed} entr{'y' if removed == 1 else 'ies'} removed &nbsp;·&nbsp; "
        f"{stats_after} remaining &nbsp;·&nbsp; "
        f"{valid_count} valid transition{'s' if valid_count != 1 else ''} loaded"
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Filtered Loop Detection</title>
<style>
* {{ box-sizing: border-box; }}

body {{
  margin: 0;
  background: #11111b;
  color: #cdd6f4;
  font-family: "Segoe UI", system-ui, sans-serif;
  height: 100vh;
  display: flex;
  flex-direction: column;
}}

header {{
  background: #1e1e2e;
  border-bottom: 1px solid #313244;
  padding: 12px 18px;
  display: flex;
  align-items: baseline;
  gap: 16px;
  flex-wrap: wrap;
}}

header h1 {{
  margin: 0;
  color: #cba6f7;
  font-size: 1rem;
  white-space: nowrap;
}}

.filter-summary {{
  font-size: 0.76rem;
  color: #a6adc8;
}}
.filter-summary b {{ color: #f38ba8; }}

.tabs {{
  display: flex;
  gap: 4px;
  padding: 10px 14px 0 14px;
  background: #181825;
  border-bottom: 1px solid #313244;
}}

.tab {{
  background: transparent;
  color: #6c7086;
  border: none;
  padding: 9px 18px;
  cursor: pointer;
  border-bottom: 2px solid transparent;
  font-size: 0.82rem;
}}

.tab.active {{
  color: #cba6f7;
  border-bottom-color: #cba6f7;
  font-weight: 600;
}}

.controls {{
  display: flex;
  gap: 18px;
  align-items: center;
  flex-wrap: wrap;
  padding: 12px 18px;
  background: #181825;
  border-bottom: 1px solid #313244;
}}

.ctrl {{
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.8rem;
}}

.ctrl input[type=text],
.ctrl select {{
  background: #313244;
  border: 1px solid #45475a;
  color: #cdd6f4;
  border-radius: 6px;
  padding: 6px 8px;
}}

.ctrl input[type=range] {{ accent-color: #cba6f7; }}

.badges {{ margin-left: auto; display: flex; gap: 8px; }}

.badge {{
  background: #313244;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 0.75rem;
}}

.table-container {{ flex: 1; overflow: auto; }}

table {{
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}}

th:nth-child(1) {{ width:  7%; }}
th:nth-child(2) {{ width:  5%; }}
th:nth-child(3) {{ width:  9%; }}
th:nth-child(4) {{ width: 18%; }}
th:nth-child(5) {{ width:  7%; }}
th:nth-child(6) {{ width:  7%; }}
th:nth-child(7) {{ width: 17%; }}
th:nth-child(8) {{ width: 30%; }}

thead {{ position: sticky; top: 0; z-index: 10; }}

th {{
  background: #1e1e2e;
  color: #cba6f7;
  text-align: left;
  padding: 12px;
  border-bottom: 1px solid #313244;
  cursor: pointer;
  user-select: none;
  font-size: 0.82rem;
}}

td {{
  padding: 11px 12px;
  border-bottom: 1px solid #1f2330;
  font-size: 0.8rem;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}

td.pattern, td.trace-box, td.per-label-cell {{
  white-space: normal;
  overflow: visible;
  word-break: break-word;
  overflow-wrap: anywhere;
}}

tr:hover {{ background: #181825; }}

.type-pill {{
  display: inline-block;
  padding: 3px 9px;
  border-radius: 999px;
  font-size: 0.72rem;
  font-weight: 600;
  white-space: nowrap;
}}

.self_loop      {{ background: rgba(231,111,81,0.15);  color: #E76F51; }}
.non_self_loop  {{ background: rgba(76,155,232,0.15);  color: #4C9BE8; }}
.repeated_pattern {{ background: rgba(166,227,161,0.15); color: #a6e3a1; }}

.support-bar {{
  height: 7px;
  background: #313244;
  border-radius: 999px;
  overflow: hidden;
  margin-top: 4px;
}}
.support-fill {{ height: 100%; background: #cba6f7; }}

.empty {{ padding: 30px; text-align: center; color: #6c7086; }}

.trace-box {{
  color: #a6adc8;
  line-height: 1.4;
  font-family: Consolas, monospace;
  font-size: 0.74rem;
}}
</style>
</head>

<body>

<header>
  <h1>&#x1F9F9; Filtered Loop Detection</h1>
  <span class="filter-summary">{filter_summary}</span>
</header>

<div class="tabs">
  <button class="tab active" onclick="switchScope('all', this)">All</button>
  <button class="tab" onclick="switchScope('global', this)">Global</button>
  <button class="tab" onclick="switchScope('per_label', this)">Per Label</button>
  <button class="tab" onclick="switchScope('exclusive', this)">Exclusive</button>
</div>

<div class="controls">
  <div class="ctrl">
    <span>Min support</span>
    <input type="range" id="supportSlider" min="0" max="{max_support}" value="0">
    <span id="supportValue">0</span>
  </div>
  <div class="ctrl">
    <span>Search</span>
    <input type="text" id="searchInput" placeholder="Pattern...">
  </div>
  <div class="ctrl">
    <span>Type</span>
    <select id="typeFilter">
      <option value="all">All</option>
      <option value="self_loop">Self Loop</option>
      <option value="non_self_loop">Non Self Loop</option>
      <option value="repeated_pattern">Repeated Pattern</option>
    </select>
  </div>
  <div class="badges">
    <div class="badge" id="rowsBadge">0 rows</div>
  </div>
</div>

<div class="table-container">
  <table>
    <thead>
      <tr>
        <th onclick="sortBy('scope')">Scope</th>
        <th onclick="sortBy('label')">Label</th>
        <th onclick="sortBy('loop_type')">Type</th>
        <th onclick="sortBy('pattern')">Pattern</th>
        <th onclick="sortBy('support_count')">Count</th>
        <th onclick="sortBy('support')">Support %</th>
        <th>Per-label support</th>
        <th>Trace occurrences</th>
      </tr>
    </thead>
    <tbody id="tableBody"></tbody>
  </table>
</div>

<script>
const ALL_ROWS   = {rows_json};
const MAX_SUPP   = {max_support};

let currentScope = 'all';
let currentSort  = 'support_count';
let sortAsc      = false;

function escapeHtml(s) {{
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}}

function switchScope(scope, btn) {{
  currentScope = scope;
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  render();
}}

function sortBy(field) {{
  if (currentSort === field) {{ sortAsc = !sortAsc; }}
  else {{ currentSort = field; sortAsc = true; }}
  render();
}}

function render() {{
  const minSupport = parseInt(document.getElementById('supportSlider').value);
  const search     = document.getElementById('searchInput').value.toLowerCase().trim();
  const typeFilter = document.getElementById('typeFilter').value;

  let rows = ALL_ROWS.filter(r => {{
    if (currentScope !== 'all' && r.scope !== currentScope) return false;
    if (r.support_count < minSupport)                        return false;
    if (typeFilter !== 'all' && r.loop_type !== typeFilter)  return false;
    if (search && !r.pattern.toLowerCase().includes(search)) return false;
    return true;
  }});

  rows.sort((a, b) => {{
    let av = a[currentSort], bv = b[currentSort];
    if (typeof av === 'string') av = av.toLowerCase();
    if (typeof bv === 'string') bv = bv.toLowerCase();
    if (av < bv) return sortAsc ? -1 :  1;
    if (av > bv) return sortAsc ?  1 : -1;
    return 0;
  }});

  const tbody = document.getElementById('tableBody');

  if (!rows.length) {{
    tbody.innerHTML = `<tr><td colspan="8" class="empty">No rows match the current filters.</td></tr>`;
    document.getElementById('rowsBadge').textContent = '0 rows';
    return;
  }}

  tbody.innerHTML = rows.map(r => {{
    const pct        = Math.max(3, (r.support_count / MAX_SUPP) * 100);
    const supportPct = typeof r.support === 'number'
      ? (r.support * 100).toFixed(2) + '%' : '-';

    // Per-label support
    const spl     = r.support_per_label || {{}};
    const splKeys  = Object.keys(spl);
    const splHtml  = splKeys.length === 0
      ? '<span style="color:#6c7086">—</span>'
      : splKeys.map(lv => {{
          const d  = spl[lv] || {{}};
          const sc = d.support_count ?? 0;
          const sp = typeof d.support === 'number'
            ? (d.support * 100).toFixed(1) + '%' : '-';
          return `<div style="font-size:.74rem;margin-bottom:3px">` +
                 `<span style="color:#6c7086">label ${{escapeHtml(lv)}}:</span> ` +
                 `<b>${{sc}}</b> <span style="color:#a6adc8">(${{sp}})</span></div>`;
        }}).join('');

    // Trace occurrences
    const toMap   = r.trace_occurrences || {{}};
    const toKeys  = Object.keys(toMap);
    const toCount = toKeys.length;
    const toInner = toCount === 0
      ? '<span style="color:#6c7086">—</span>'
      : toKeys.sort((a,b) => String(a).localeCompare(String(b)))
          .map(tid =>
            `<div><span style="color:#89b4fa">${{escapeHtml(String(tid))}}</span>` +
            ` &times; ${{toMap[tid]}}</div>`
          ).join('');

    return `
      <tr>
        <td>${{escapeHtml(r.scope)}}</td>
        <td>${{escapeHtml(r.label || '-')}}</td>
        <td><span class="type-pill ${{r.loop_type}}">${{r.loop_type.replace(/_/g,' ')}}</span></td>
        <td class="pattern">${{escapeHtml(r.pattern)}}</td>
        <td>
          <div><b>${{r.support_count}}</b></div>
          <div class="support-bar"><div class="support-fill" style="width:${{pct}}%"></div></div>
        </td>
        <td style="color:#cba6f7;font-weight:600">${{supportPct}}</td>
        <td class="per-label-cell">${{splHtml}}</td>
        <td class="trace-box">
          <details>
            <summary style="cursor:pointer;color:#89b4fa;user-select:none">
              Show ${{toCount}} trace${{toCount === 1 ? '' : 's'}}
            </summary>
            <div style="margin-top:6px;padding:8px;background:#181825;
                        border:1px solid #313244;border-radius:6px;
                        max-height:220px;overflow:auto">
              ${{toInner}}
            </div>
          </details>
        </td>
      </tr>`;
  }}).join('');

  document.getElementById('rowsBadge').textContent = rows.length + ' rows';
}}

document.getElementById('supportSlider').addEventListener('input', function() {{
  document.getElementById('supportValue').textContent = this.value;
  render();
}});
document.getElementById('searchInput').addEventListener('input', render);
document.getElementById('typeFilter').addEventListener('change', render);

render();
</script>
</body>
</html>"""

    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html)


# ===========================================================================
# 10.  CLI
# ===========================================================================

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--loops",    required=True,
                   help="Comparator loops output file (.json or .csv).")
    p.add_argument("--valid",    required=True,
                   help="Valid-transitions CSV file.")
    p.add_argument("--output",   required=True,
                   help="Output path prefix (extension appended automatically).")
    p.add_argument("--format",   choices=["json", "csv", "both"], default="json",
                   help="Output format (default: json).")
    p.add_argument("--from-col", default=None,
                   help="Column name for the source activity in --valid.")
    p.add_argument("--to-col",   default=None,
                   help="Column name for the target activity in --valid.")
    p.add_argument("--vis",      action="store_true",
                   help="Also write an HTML visualisation (<output>.html).")
    return p


def main(argv: Optional[List[str]] = None) -> None:
    args = _build_parser().parse_args(argv)

    # ── Load inputs ───────────────────────────────────────────────────────────
    print(f"Loading loops from  : {args.loops}")
    result = load_loops(args.loops)

    print(f"Loading transitions : {args.valid}")
    valid = load_valid_transitions(args.valid, args.from_col, args.to_col)
    print(f"  {len(valid)} valid transition(s) loaded.")

    # ── Filter ────────────────────────────────────────────────────────────────
    entries_before = _count_entries(result)
    filtered       = filter_result(result, valid)
    entries_after  = _count_entries(filtered)
    removed        = entries_before - entries_after

    print(f"Entries before : {entries_before}")
    print(f"Entries after  : {entries_after}  ({removed} removed)")

    # ── Save outputs ──────────────────────────────────────────────────────────
    out = args.output

    if args.format in ("json", "both"):
        json_path = out if out.endswith(".json") else out + ".json"
        save_json(filtered, json_path)
        print(f"JSON written   : {json_path}")

    if args.format in ("csv", "both"):
        csv_path = out if out.endswith(".csv") else out + ".csv"
        save_csv(filtered, csv_path)
        print(f"CSV written    : {csv_path}")

    if args.vis:
        html_path = Path(out).with_suffix("").with_suffix("").as_posix() + ".html"
        save_html(filtered, html_path, valid, entries_before, entries_after)
        print(f"HTML written   : {html_path}")


if __name__ == "__main__":
    main()