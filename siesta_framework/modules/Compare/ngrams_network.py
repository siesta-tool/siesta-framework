#!/usr/bin/env python3
"""
ngram_network.py
Reads an ngram CSV and generates a self-contained interactive HTML
network visualisation using vis-network.

Columns expected:
    ngram | count_1 | count_0 | balance | confidence_1 | confidence_0 | support | direction

The n-gram length (bigram / trigram / …) is auto-detected from the
number of ' -> ' separators in the first non-null ngram value.

Visual encoding summary
-----------------------
  Edge colour    →  direction      (label_0 = blue, label_1 = orange, …)
  Edge width     →  confidence_1   (purity toward label-1;  1–8 px)
  Edge opacity   →  |balance|      (strong signal = opaque, weak = faint)
  Edge dashes    →  |balance| < 0.3  (ambiguous / mixed pattern)
  Node size      →  support        (how common this activity is across all traces)

Usage:
    python3 ngram_network.py input.csv [output.html] -- CLI
    create_network("input.csv") -> HTML str -- Python function
"""

import sys
import json
import re
import math
import pandas as pd
from collections import defaultdict
from pyspark.sql import DataFrame as SparkDataFrame


def create_network(input: str | pd.DataFrame | SparkDataFrame | None) -> str:
  """
  Creates an interactive n-gram network visualisation as an HTML string.
    Parameters:
        input: Path to the input CSV file, or a pandas/Spark DataFrame containing the n-gram data.
    Returns:
        A string containing the complete HTML for the visualisation.
  """
  if isinstance(input, str):
    INPUT_CSV = input
    df = None

  # ── Load ─────────────────────────────────────────────────────────────────────
  if df is not None:
    if isinstance(df, SparkDataFrame):
        df = df.toPandas()
    elif not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas DataFrame or a Spark DataFrame")
  else:
    df = pd.read_csv(INPUT_CSV)
  
  df.columns = df.columns.str.strip()
  df["ngram"]     = df["ngram"].astype(str).str.strip()
  df["direction"] = df["direction"].astype(str).str.strip()

  # Back-compat: if old CSV is missing the new columns, synthesise them
  for col, default in [("confidence_0", float("nan")), ("support", float("nan"))]:
      if col not in df.columns:
          df[col] = default
          print(f"[warn] Column '{col}' not found in CSV — defaulting to NaN. "
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

  # ── Parse ngrams → edges ──────────────────────────────────────────────────────
  # Each row:  A -> B -> C  produces edges  (A,B) and (B,C)
  # Stats are accumulated per (from_node, to_node, direction).

  edge_stats: dict = defaultdict(lambda: {
      "count_1": 0, "count_0": 0,
      "balance_sum":      0.0,
      "confidence_1_sum":   0.0,
      "confidence_0_sum": 0.0,
      "support_sum":      0.0,
      "rows": 0,
  })

  nodes_seen: dict       = {}    # full_label → node_id
  node_support_max: dict = {}    # full_label → max support seen on any incident edge

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
      confidence_1   = safe_float(row["confidence_1"])
      confidence_0 = safe_float(row["confidence_0"])
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
          s["confidence_1_sum"]   += confidence_1
          s["confidence_0_sum"] += confidence_0
          s["support_sum"]      += support
          s["rows"]             += 1

  # ── Build vis-network data ────────────────────────────────────────────────────

  # -- Nodes --
  # Node size →  max support of any connected edge  (mapped to 20–55 px)
  max_supp = max(node_support_max.values()) if node_support_max else 1.0
  max_supp = max_supp if max_supp > 0 else 1.0

  def node_size(label: str) -> int:
      raw = node_support_max.get(label, 0.0) / max_supp
      return int(20 + raw * 35)   # 20 – 55 px

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

      avg_confidence_1   = s["confidence_1_sum"]   / rows
      avg_confidence_0 = s["confidence_0_sum"] / rows
      avg_balance      = s["balance_sum"]      / rows
      avg_support      = s["support_sum"]      / rows
      abs_balance      = abs(avg_balance)

      palette  = DIRECTION_PALETTE[direction]

      # Width: 1 px (conf=0) → 8 px (conf=1)
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
          f"<b>{src}</b> → <b>{tgt}</b><br>"
          f"Direction: <b>{palette['name']}</b><br>"
          f"Avg confidence_1\u2081: {avg_confidence_1:.3f}<br>"
          f"Avg confidence_1\u2080: {avg_confidence_0:.3f}<br>"
          f"Avg balance: {avg_balance:.3f}<br>"
          f"Avg support: {avg_support:.4f}<br>"
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
          "_confidence_1":   round(avg_confidence_1,   4),
          "_confidence_0": round(avg_confidence_0, 4),
          "_balance":      round(avg_balance,       4),
          "_support":      round(avg_support,       4),
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
      <label title="confidence_1₁ = count_1 / (count_1 + count_0) — purity toward label-1">
        Min conf₁
        <input type="range" id="conf-slider"  min="0" max="100" value="0" step="1">
        <span id="conf-val">0.00</span>
      </label>
      <label title="confidence_1₀ = count_0 / total_0 — normalised recall for label-0">
        Min conf₀
        <input type="range" id="conf0-slider" min="0" max="100" value="0" step="1">
        <span id="conf0-val">0.00</span>
      </label>
    </div>
    <div class="sep"></div>
    <div class="ctrl-group">
      <label title="balance = (count_1/total_1) − (count_0/total_0)  ∈ [−1, 1]">
        Min |balance|
        <input type="range" id="bal-slider" min="0" max="100" value="0" step="1">
        <span id="bal-val">0.00</span>
      </label>
      <label title="support = (count_1+count_0)/(total_1+total_0) — fraction of all traces">
        Min support
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
        <h2>Direction → colour</h2>
        <div id="legend-dir"></div>
      </div>

      <div>
        <h2>|Balance| → opacity</h2>
        <div class="enc-guide">
          <div class="enc-row"><div class="line-solid"></div>  Strong (≥0.6)</div>
          <div class="enc-row"><div class="line-medium"></div> Moderate (0.3–0.6)</div>
          <div class="enc-row"><div class="line-dashed"></div> Weak &lt;0.3 (dashed)</div>
        </div>
      </div>

      <div>
        <h2>confidence_1₁ → width</h2>
        <div class="enc-guide">
          <div class="enc-row"><div class="wline" style="height:7px"></div> High (≥0.8)</div>
          <div class="enc-row"><div class="wline" style="height:4px"></div> Medium (0.4–0.8)</div>
          <div class="enc-row"><div class="wline" style="height:1px"></div> Low (&lt;0.4)</div>
        </div>
      </div>

      <div>
        <h2>Support → node size</h2>
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
    const minConf  = parseFloat(document.getElementById('conf-slider').value)  / 100;
    const minConf0 = parseFloat(document.getElementById('conf0-slider').value) / 100;
    const minBal   = parseFloat(document.getElementById('bal-slider').value)   / 100;
    const minSup   = parseFloat(document.getElementById('sup-slider').value)   / 100;
    const dir      = dirFilter.value;

    const filtered = ALL_EDGES.filter(e =>
      e._confidence_1   >= minConf  &&
      e._confidence_0 >= minConf0 &&
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
  wireSlider('conf-slider',  'conf-val',  100, 2);
  wireSlider('conf0-slider', 'conf0-val', 100, 2);
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
      statRow('Direction',    e._direction) +
      statRow('confidence_1₁',  e._confidence_1.toFixed(3)) +
      statRow('confidence_1₀',  e._confidence_0.toFixed(3)) +
      statRow('Balance',      e._balance.toFixed(3)) +
      statRow('Support',      e._support.toFixed(4));
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
    print("Usage: python3 ngram_network.py <input.csv> [output.html]")
    sys.exit(1)

  html = create_network(sys.argv[1])

  with open(sys.argv[2] if len(sys.argv) > 2 else "ngram_network.html", "w", encoding="utf-8") as f:
    f.write(html)
