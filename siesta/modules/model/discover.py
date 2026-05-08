"""
Process model discovery — no graphviz required.

Model formats:  DFG -> XML (.xml),  BPMN (inductive) -> BPMN 2.0 (.bpmn)
PNG:            matplotlib, layered L->R layout, orthogonal edge routing
HTML:           pyvis (interactive physics simulation) if installed,
                else static matplotlib SVG fallback
"""
import io
import logging
import math
import tempfile
from collections import defaultdict

import networkx as nx
import matplotlib.patches as mpatches
import matplotlib.path as mpath
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.backends.backend_svg import FigureCanvasSVG
from matplotlib.figure import Figure

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.statistics.start_activities.log import get as start_activities_module
from pm4py.statistics.end_activities.log import get as end_activities_module
from pm4py.objects.conversion.process_tree import converter as pt_converter
from pm4py.objects.bpmn.exporter import exporter as bpmn_exporter

logger = logging.getLogger(__name__)

# ── Geometry constants (data-coordinate units) ────────────────────────────────
_TASK_W  = 1.8    # task rectangle full width
_TASK_H  = 0.6    # task rectangle full height
_EVENT_R = 0.28   # start/end event circle radius
_GW_SIZE = 0.32   # gateway diamond half-diagonal
_X_GAP   = 3.0    # horizontal distance between layers
_Y_GAP   = 1.2    # vertical distance between nodes in the same layer


# ── Generic utilities ─────────────────────────────────────────────────────────

def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    elif seconds < 86400:
        return f"{seconds/3600:.1f}h"
    else:
        return f"{seconds/86400:.1f}d"


def _new_tempfile(suffix: str) -> str:
    f = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode='wb')
    path = f.name
    f.close()
    return path


# ── Layout ────────────────────────────────────────────────────────────────────

def _layered_layout(G: nx.DiGraph) -> dict:
    """Left-to-right hierarchical layout via longest-path layering.
    Cycles (loop constructs) are handled by removing back edges before layering."""
    if not G.nodes:
        return {}

    # Remove back edges one at a time until the graph is acyclic
    dag = G.copy()
    while True:
        try:
            cycle = nx.find_cycle(dag)
            dag.remove_edge(cycle[-1][0], cycle[-1][1])
        except nx.exception.NetworkXNoCycle:
            break

    topo = list(nx.topological_sort(dag))
    layer: dict = defaultdict(int)
    for n in topo:
        for s in dag.successors(n):
            layer[s] = max(layer[s], layer[n] + 1)

    by_layer: dict = defaultdict(list)
    for n in G.nodes():
        by_layer[layer.get(n, 0)].append(n)

    pos = {}
    for l, nodes in by_layer.items():
        x = float(l) * _X_GAP
        for i, n in enumerate(nodes):
            y = (float(i) - (len(nodes) - 1) / 2.0) * _Y_GAP
            pos[n] = (x, y)
    return pos


# ── Node-boundary margin helpers ──────────────────────────────────────────────

def _rect_margin(hw: float, hh: float, ux: float, uy: float) -> float:
    """Distance from rectangle centre to its boundary in direction (ux, uy)."""
    t = []
    if abs(ux) > 1e-9:
        t.append(hw / abs(ux))
    if abs(uy) > 1e-9:
        t.append(hh / abs(uy))
    return min(t) if t else 0.0


def _diamond_margin(size: float, ux: float, uy: float) -> float:
    """Distance from diamond centre to its boundary in direction (ux, uy)."""
    d = abs(ux) + abs(uy)
    return size / d if d > 1e-9 else size


def _margin(kind: str, ux: float, uy: float) -> float:
    if kind == 'task':
        return _rect_margin(_TASK_W / 2, _TASK_H / 2, ux, uy)
    if kind in ('start', 'end'):
        return _EVENT_R
    # gateway (xor / and / or)
    return _diamond_margin(_GW_SIZE, ux, uy)


# ── Drawing primitives ────────────────────────────────────────────────────────

def _draw_node(ax, x: float, y: float, kind: str, label: str):
    if kind == 'task':
        ax.add_patch(mpatches.FancyBboxPatch(
            (x - _TASK_W / 2, y - _TASK_H / 2), _TASK_W, _TASK_H,
            boxstyle="round,pad=0.06",
            facecolor='#2E6DA4', edgecolor='#1A4A75', lw=1.5, zorder=3,
        ))
        lines = label.split('\n')
        lines[0] = lines[0] if len(lines[0]) <= 20 else lines[0][:18] + '…'
        ax.text(x, y, '\n'.join(lines), ha='center', va='center',
                fontsize=7.5, color='white', fontweight='bold',
                linespacing=1.3, zorder=4)

    elif kind == 'start':
        ax.add_patch(mpatches.Circle((x, y), _EVENT_R,
                                     fc='white', ec='black', lw=2, zorder=3))
        ax.text(x, y - _EVENT_R - 0.14, 'start',
                ha='center', va='top', fontsize=6.5, zorder=4)

    elif kind == 'end':
        ax.add_patch(mpatches.Circle((x, y), _EVENT_R,
                                     fc='white', ec='black', lw=4, zorder=3))
        ax.add_patch(mpatches.Circle((x, y), _EVENT_R * 0.55,
                                     fc='black', ec='none', zorder=4))
        ax.text(x, y - _EVENT_R - 0.14, 'end',
                ha='center', va='top', fontsize=6.5, zorder=4)

    else:  # gateway
        symbol = {'xor': '*', 'and': '+', 'or': 'O'}.get(kind, '?')
        ax.add_patch(mpatches.Polygon(
            [(x, y + _GW_SIZE), (x + _GW_SIZE, y),
             (x, y - _GW_SIZE), (x - _GW_SIZE, y)],
            fc='white', ec='black', lw=2, zorder=3,
        ))
        ax.text(x, y, symbol, ha='center', va='center',
                fontsize=11, fontweight='bold', zorder=4)


def _draw_edge(ax, p1, k1: str, p2, k2: str, label: str = ''):
    x1, y1 = p1
    x2, y2 = p2
    dx, dy = x2 - x1, y2 - y1
    dist = math.sqrt(dx ** 2 + dy ** 2)

    if dist < 1e-10:
        # Self-loop: arc that exits from the top of the node and loops back.
        top_m = _margin(k1, 0, 1)
        offset = 0.15
        ax.add_patch(mpatches.FancyArrowPatch(
            posA=(x1 - offset, y1 + top_m),
            posB=(x1 + offset, y1 + top_m),
            connectionstyle='arc3,rad=1.8',
            arrowstyle='->',
            color='#555555', lw=1.4, mutation_scale=11, zorder=2,
        ))
        if label:
            ax.text(x1, y1 + top_m + 0.55, str(label),
                    ha='center', va='bottom', fontsize=6.5, zorder=5,
                    bbox=dict(boxstyle='round,pad=0.1', fc='white', ec='none', alpha=0.85))
        return

    if x2 > x1 + 0.5:
        # Forward edge: orthogonal H-V-H routing (exit right -> drop/rise -> enter left).
        sx = x1 + _margin(k1, 1, 0)
        tx = x2 - _margin(k2, 1, 0)
        mid_x = (sx + tx) / 2
        # Segments 1 + 2: horizontal then vertical, drawn as a plain line (no arrowhead).
        ax.plot([sx, mid_x, mid_x], [y1, y1, y2],
                color='#555555', lw=1.4, zorder=2, solid_capstyle='butt')
        # Segment 3: horizontal to target — annotate draws the line AND the arrowhead.
        ax.annotate('', xy=(tx, y2), xytext=(mid_x, y2),
                    arrowprops=dict(arrowstyle='->', color='#555555', lw=1.4,
                                    mutation_scale=11), zorder=3)
        if label:
            ax.text(sx + 0.12, y1 + 0.08, str(label), ha='left', va='bottom',
                    fontsize=6.5, zorder=5,
                    bbox=dict(boxstyle='round,pad=0.1', fc='white', ec='none', alpha=0.85))
    else:
        # Back edge or same-layer: curved arc so it doesn't overlap the nodes.
        ux, uy = dx / dist, dy / dist
        sx = x1 + _margin(k1, ux, uy) * ux
        sy = y1 + _margin(k1, ux, uy) * uy
        tx = x2 - _margin(k2, ux, uy) * ux
        ty = y2 - _margin(k2, ux, uy) * uy
        ax.annotate('', xy=(tx, ty), xytext=(sx, sy),
                    arrowprops=dict(arrowstyle='->', color='#888888', lw=1.2,
                                    mutation_scale=11,
                                    connectionstyle='arc3,rad=0.4'),
                    zorder=2)
        if label:
            mx, my = (sx + tx) / 2, (sy + ty) / 2
            ax.text(mx, my + 0.07, str(label), ha='center', va='bottom',
                    fontsize=6.5, zorder=5,
                    bbox=dict(boxstyle='round,pad=0.1', fc='white', ec='none', alpha=0.85))


# ── Figure builder ────────────────────────────────────────────────────────────

def _build_figure(G: nx.DiGraph, pos: dict, node_info: dict, edge_labels: dict) -> Figure:
    if not pos:
        return Figure(figsize=(4, 2))

    xs = [p[0] for p in pos.values()]
    ys = [p[1] for p in pos.values()]
    x_pad = _TASK_W * 1.4
    y_pad = _TASK_H * 3.5

    xlim = (min(xs) - x_pad, max(xs) + x_pad)
    ylim = (min(ys) - y_pad, max(ys) + y_pad)

    # Figure size in inches ≈ data range in units -> 1 unit ≈ 1 inch in both axes,
    # keeping patch shapes undistorted without needing set_aspect('equal').
    fig_w = max(8.0, xlim[1] - xlim[0])
    fig_h = max(4.0, ylim[1] - ylim[0])

    fig = Figure(figsize=(fig_w, fig_h), facecolor='white')
    ax = fig.add_subplot(111)
    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.axis('off')

    # Edges first (drawn under nodes)
    for src, tgt in G.edges():
        _draw_edge(ax, pos[src], node_info[src]['kind'],
                   pos[tgt], node_info[tgt]['kind'],
                   edge_labels.get((src, tgt), ''))

    # Nodes on top
    for nid, (x, y) in pos.items():
        _draw_node(ax, x, y, node_info[nid]['kind'], node_info[nid]['label'])

    return fig


# ── Save helpers ──────────────────────────────────────────────────────────────

def _save_png(fig: Figure, path: str):
    FigureCanvasAgg(fig)
    fig.savefig(path, dpi=150, bbox_inches='tight')


def _pm4py_png_dfg(dfg: dict, start_acts: dict, end_acts: dict, png_path: str,
                   activity_durations: dict = None) -> bool:
    """Try PM4Py's graphviz DFG renderer. Returns True on success, False if graphviz absent."""
    try:
        from pm4py.visualization.dfg import visualizer as dfg_visualizer
        if activity_durations:
            def _ann(a):
                return f"{a}\n({_format_duration(activity_durations[a])})" if a in activity_durations else a
            dfg_disp    = {(_ann(s), _ann(t)): v for (s, t), v in dfg.items()}
            start_disp  = {_ann(a): v for a, v in start_acts.items()}
            end_disp    = {_ann(a): v for a, v in end_acts.items()}
        else:
            dfg_disp, start_disp, end_disp = dfg, start_acts, end_acts
        gviz = dfg_visualizer.apply(dfg_disp, parameters={
            "start_activities": start_disp,
            "end_activities": end_disp,
        })
        dfg_visualizer.save(gviz, png_path)
        return True
    except Exception as e:
        logger.debug("PM4Py DFG visualizer unavailable (%s); falling back to matplotlib.", e)
        return False


def _pm4py_png_bpmn(bpmn_model, png_path: str, activity_durations: dict = None) -> bool:
    """Try PM4Py's graphviz BPMN renderer. Returns True on success, False if graphviz absent."""
    try:
        from pm4py.visualization.bpmn import visualizer as bpmn_visualizer
        gviz = bpmn_visualizer.apply(bpmn_model)
        if activity_durations and hasattr(gviz, 'body'):
            # Inject durations by patching the DOT body before rendering.
            # Sort longest-first to prevent short names being matched inside longer ones.
            for act, dur in sorted(activity_durations.items(), key=lambda x: -len(x[0])):
                dur_str = _format_duration(dur)
                for i in range(len(gviz.body)):
                    gviz.body[i] = gviz.body[i].replace(
                        f'label="{act}"',
                        f'label="{act}\\n({dur_str})"',
                    )
        bpmn_visualizer.save(gviz, png_path)
        return True
    except Exception as e:
        logger.debug("PM4Py BPMN visualizer unavailable (%s); falling back to matplotlib.", e)
        return False


def _save_html_static(fig: Figure, path: str, title: str = 'Process Model'):
    """Fallback: embed matplotlib figure as SVG in a standalone HTML file."""
    buf = io.StringIO()
    FigureCanvasSVG(fig).print_svg(buf)
    svg = buf.getvalue()
    html = (
        '<!DOCTYPE html><html>'
        f'<head><meta charset="utf-8"><title>{title}</title>'
        '<style>body{margin:0;background:#fff;overflow:auto}</style></head>'
        f'<body>{svg}</body></html>'
    )
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)


def _save_html_pyvis(G: nx.DiGraph, node_info: dict, edge_labels: dict,
                     path: str, title: str = 'Process Model'):
    """Interactive pyvis HTML with physics simulation. Returns False if pyvis is missing."""
    try:
        from pyvis.network import Network
    except ImportError:
        logger.info("pyvis not installed — HTML will be static SVG. "
                    "Install with: pip install pyvis")
        return False

    try:
        net = Network(height='100vh', width='100%', bgcolor='#ffffff',
                      directed=True, notebook=False)
        # set_options requires the 'var options = {...}' wrapper
        net.set_options("""var options = {
          "physics": {
            "solver": "barnesHut",
            "barnesHut": {
              "gravitationalConstant": -8000,
              "springLength": 160,
              "springConstant": 0.04,
              "damping": 0.15
            }
          },
          "edges": {
            "arrows": {"to": {"enabled": true}},
            "smooth": {"type": "curvedCW", "roundness": 0.2}
          },
          "interaction": {"hover": true, "navigationButtons": true}
        }""")

        _SHAPE = {
            'task':  ('box',     '#2E6DA4', '#1A4A75', 'white'),
            'start': ('dot',     'white',   'black',   'black'),
            'end':   ('dot',     'black',   'black',   'white'),
            'xor':   ('diamond', 'white',   'black',   'black'),
            'and':   ('diamond', 'white',   'black',   'black'),
            'or':    ('diamond', 'white',   'black',   'black'),
        }
        _GW_SYMBOL = {'xor': '*', 'and': '+', 'or': 'O'}

        for nid, info in node_info.items():
            kind  = info['kind']
            label = info['label'] or _GW_SYMBOL.get(kind, '')
            shape, bg, border, fc = _SHAPE.get(kind, ('box', '#aaa', '#666', 'black'))
            net.add_node(str(nid), label=label, shape=shape,
                         color={'background': bg, 'border': border},
                         font={'color': fc, 'size': 13},
                         size=20 if kind == 'task' else 12)

        for src, tgt in G.edges():
            lbl = edge_labels.get((src, tgt), '')
            net.add_edge(str(src), str(tgt),
                         label=str(lbl) if lbl else '',
                         color='#555555')

        # generate_html() is the most reliable write path across pyvis versions
        if hasattr(net, 'generate_html'):
            html_str = net.generate_html()
            with open(path, 'w', encoding='utf-8') as f:
                f.write(html_str)
        else:
            net.write_html(path)

        return True

    except Exception:
        logger.exception("pyvis HTML generation failed; falling back to static SVG.")
        return False




# ── DFG ───────────────────────────────────────────────────────────────────────

def _export_dfg_xml(dfg: dict, start_activities: dict, end_activities: dict, path: str):
    activities = sorted({a for pair in dfg for a in pair})
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<directly-follows-graph>',
        '  <nodes>',
        *[f'    <node id="{a}"/>' for a in activities],
        '  </nodes>',
        '  <start-activities>',
        *[f'    <activity id="{a}" frequency="{f}"/>' for a, f in sorted(start_activities.items())],
        '  </start-activities>',
        '  <end-activities>',
        *[f'    <activity id="{a}" frequency="{f}"/>' for a, f in sorted(end_activities.items())],
        '  </end-activities>',
        '  <edges>',
        *[f'    <edge source="{s}" target="{t}" frequency="{f}"/>'
          for (s, t), f in sorted(dfg.items())],
        '  </edges>',
        '</directly-follows-graph>',
    ]
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write('\n'.join(lines))


def _build_dfg_graph(dfg: dict, start_activities: dict, end_activities: dict,
                     activity_durations: dict):
    G = nx.DiGraph()
    node_info: dict = {}
    edge_labels: dict = {}

    START, END = '__start__', '__end__'
    G.add_node(START)
    node_info[START] = {'kind': 'start', 'label': ''}
    G.add_node(END)
    node_info[END] = {'kind': 'end', 'label': ''}

    for act in {a for pair in dfg for a in pair}:
        G.add_node(act)
        lbl = act
        if activity_durations and act in activity_durations:
            lbl += f'\n({_format_duration(activity_durations[act])})'
        node_info[act] = {'kind': 'task', 'label': lbl}

    for act, freq in start_activities.items():
        G.add_edge(START, act)
        edge_labels[(START, act)] = str(freq)

    for act, freq in end_activities.items():
        G.add_edge(act, END)
        edge_labels[(act, END)] = str(freq)

    for (src, tgt), freq in dfg.items():
        G.add_edge(src, tgt)
        edge_labels[(src, tgt)] = str(freq)

    return G, node_info, edge_labels


def _discover_dfg(event_log, noise_threshold: float = 0.0, activity_durations: dict = None):
    dfg = dfg_discovery.apply(event_log)
    start_acts = start_activities_module.get_start_activities(event_log)
    end_acts = end_activities_module.get_end_activities(event_log)

    if noise_threshold > 0.0:
        from pm4py.algo.filtering.dfg import dfg_filtering
        dfg, start_acts, end_acts = dfg_filtering.filter_dfg_on_paths_percentage(
            dfg, start_acts, end_acts, 1.0 - noise_threshold
        )

    model_path = _new_tempfile('.xml')
    png_path   = _new_tempfile('.png')
    html_path  = _new_tempfile('.html')

    _export_dfg_xml(dfg, start_acts, end_acts, model_path)

    try:
        G, node_info, edge_labels = _build_dfg_graph(dfg, start_acts, end_acts,
                                                      activity_durations or {})
        pos = _layered_layout(G)
        mpl_fig = None

        # PNG: PM4Py/graphviz (best quality) -> matplotlib fallback
        if not _pm4py_png_dfg(dfg, start_acts, end_acts, png_path, activity_durations):
            mpl_fig = _build_figure(G, pos, node_info, edge_labels)
            _save_png(mpl_fig, png_path)

        # HTML: pyvis (interactive) -> matplotlib SVG fallback
        if not _save_html_pyvis(G, node_info, edge_labels, html_path, 'Directly-Follows Graph'):
            if mpl_fig is None:
                mpl_fig = _build_figure(G, pos, node_info, edge_labels)
            _save_html_static(mpl_fig, html_path, 'Directly-Follows Graph')
    except Exception:
        logger.exception("DFG visualization failed.")

    return model_path, 'xml', png_path, html_path


# ── BPMN ──────────────────────────────────────────────────────────────────────

def _classify_bpmn_node(node) -> tuple[str, str]:
    cls = type(node).__name__
    if 'StartEvent' in cls:
        return 'start', ''
    if 'EndEvent' in cls:
        return 'end', ''
    if 'Exclusive' in cls or 'XOR' in cls:
        return 'xor', ''
    if 'Parallel' in cls:
        return 'and', ''
    if 'Inclusive' in cls or 'OR' in cls:
        return 'or', ''
    label = (node.get_name() if hasattr(node, 'get_name') else '') or cls
    return 'task', label


def _build_bpmn_graph(bpmn_model, activity_durations: dict = None):
    G = nx.DiGraph()
    node_info: dict = {}

    for node in bpmn_model.get_nodes():
        nid = id(node)
        kind, label = _classify_bpmn_node(node)
        if kind == 'task' and activity_durations and label in activity_durations:
            label = f"{label}\n({_format_duration(activity_durations[label])})"
        G.add_node(nid)
        node_info[nid] = {'kind': kind, 'label': label}

    for flow in bpmn_model.get_flows():
        src_id = id(flow.get_source())
        tgt_id = id(flow.get_target())
        if src_id in node_info and tgt_id in node_info:
            G.add_edge(src_id, tgt_id)

    return G, node_info


def _export_bpmn_model(bpmn_model, activity_durations: dict = None):
    model_path = _new_tempfile('.bpmn')
    png_path   = _new_tempfile('.png')
    html_path  = _new_tempfile('.html')

    bpmn_exporter.apply(bpmn_model, model_path)

    try:
        G, node_info = _build_bpmn_graph(bpmn_model, activity_durations)
        pos = _layered_layout(G)
        mpl_fig = None

        # PNG: PM4Py/graphviz (classical BPMN rendering) -> matplotlib fallback
        if not _pm4py_png_bpmn(bpmn_model, png_path, activity_durations):
            mpl_fig = _build_figure(G, pos, node_info, {})
            _save_png(mpl_fig, png_path)

        # HTML: pyvis (interactive) -> matplotlib SVG fallback
        if not _save_html_pyvis(G, node_info, {}, html_path, 'BPMN Process Model'):
            if mpl_fig is None:
                mpl_fig = _build_figure(G, pos, node_info, {})
            _save_html_static(mpl_fig, html_path, 'BPMN Process Model')
    except Exception:
        logger.exception("BPMN visualization failed.")

    return model_path, 'bpmn', png_path, html_path


# ── Public API ────────────────────────────────────────────────────────────────

def discover_process_model(df: SparkDataFrame, algo: str = "inductive",
                           noise_threshold: float = 0.0, filter_percentile: float = 0.0,
                           activity_durations: dict = None):
    """
    Discovers a process model from an event log Spark DataFrame.

    Args:
        df: Spark DataFrame — columns 'case:concept:name', 'concept:name', 'time:timestamp'
            (legacy 'trace_id', 'event_type', 'timestamp' are renamed automatically).
        algo: 'dfg' -> Directly-Follows Graph (exported as XML);
              'inductive' -> BPMN via Inductive Miner (exported as BPMN 2.0 XML).
        noise_threshold: 0.0–1.0, higher = simpler model.
        filter_percentile: Pre-filter log variants by frequency percentile (0.0–1.0).
        activity_durations: Optional {activity: avg_seconds} shown in visualisations.

    Returns:
        (model_path, fmt, png_path, html_path)
    """
    if algo not in ('inductive', 'dfg'):
        raise ValueError(f"algo must be 'inductive' or 'dfg', got '{algo}'")

    for old, new in [('trace_id', 'case:concept:name'),
                     ('event_type', 'concept:name'),
                     ('timestamp', 'time:timestamp')]:
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    if 'time:timestamp' in df.columns:
        if dict(df.dtypes).get('time:timestamp', '') != 'timestamp':
            df = df.withColumn('time:timestamp', F.to_timestamp(F.col('time:timestamp')))

    df = df.orderBy(['case:concept:name', 'time:timestamp'])

    pandas_df = df.toPandas()
    pandas_df = dataframe_utils.convert_timestamp_columns_in_df(pandas_df)
    event_log = log_converter.apply(pandas_df)

    if filter_percentile > 0.0:
        from pm4py.algo.filtering.log.variants import variants_filter
        event_log = variants_filter.filter_log_variants_percentage(event_log, filter_percentile)

    if algo == 'dfg':
        return _discover_dfg(event_log, noise_threshold, activity_durations)

    # inductive -> BPMN
    if noise_threshold > 0.0:
        from pm4py.algo.discovery.inductive.variants.imf import IMFParameters
        process_tree = inductive_miner.apply(
            event_log, variant=inductive_miner.Variants.IMf,
            parameters={IMFParameters.NOISE_THRESHOLD: noise_threshold},
        )
    else:
        process_tree = inductive_miner.apply(event_log)

    bpmn_model = pt_converter.apply(process_tree, variant=pt_converter.Variants.TO_BPMN)
    return _export_bpmn_model(bpmn_model, activity_durations)
