"""
Process model discovery — no graphviz required.

Scalability note: pm4py's own DFG discovery (pm4py.algo.discovery.dfg) requires a
materialized pm4py EventLog, i.e. calling events_df.toPandas() on the FULL event log.
That doesn't scale to the log sizes this framework targets. Instead, this module
computes the Directly-Follows Graph with our own distributed Spark aggregation
(_compute_dfg_dict — same Window+lead pattern as directly_follows.py, only ever
collecting the small set of distinct activity pairs to the driver, never raw events)
and feeds the resulting (small) dict to pm4py's Inductive Miner in its DFG-only input
mode (variant=Variants.IMd). Verified against the installed pm4py version
(algo/discovery/inductive/algorithm.py): when given a `DFG` instance, `apply()`
internally routes to `IMD(...).apply(IMDataStructureDFG(InductiveDFG(dfg=obj)))`
regardless of the requested variant, falling back to IMd with a warning if a
log-based variant was requested — i.e. IMd is pm4py's supported, first-class path for
discovering a sound, block-structured process tree from an aggregated DFG alone, with
NO event log required. This is also materially better than pm4py's direct
DFG->Petri-net converter (VERSION_TO_PETRI_NET_INVISIBLES_NO_DUPLICATES), which skips
concurrency/choice-block detection entirely.

Model formats:  DFG -> XML (.xml),  Petri net -> PNML (.pnml),  BPMN (inductive) -> BPMN 2.0 (.bpmn)
PNG:            pm4py/graphviz if available, else matplotlib layered L->R layout with
                orthogonal edge routing
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
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.backends.backend_svg import FigureCanvasSVG
from matplotlib.figure import Figure

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pm4py.objects.dfg.obj import DFG
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.objects.conversion.process_tree import converter as pt_converter
from pm4py.objects.bpmn.exporter import exporter as bpmn_exporter
from pm4py.objects.petri_net.exporter import exporter as pnml_exporter

logger = logging.getLogger(__name__)

# ── Geometry constants (data-coordinate units) ────────────────────────────────
_TASK_W  = 1.8    # task rectangle full width
_TASK_H  = 0.6    # task rectangle full height
_EVENT_R = 0.28   # start/end event circle radius
_GW_SIZE = 0.32   # gateway diamond half-diagonal
_PLACE_R = 0.14   # petri net place circle radius
_SILENT_W = 0.6   # petri net silent/tau transition bar width
_SILENT_H = 0.18  # petri net silent/tau transition bar height
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
    if kind == 'place':
        return _PLACE_R
    if kind == 'silent':
        return _rect_margin(_SILENT_W / 2, _SILENT_H / 2, ux, uy)
    # gateway (xor / and / or)
    return _diamond_margin(_GW_SIZE, ux, uy)


# ── Drawing primitives ────────────────────────────────────────────────────────

def _draw_node(ax, x: float, y: float, kind: str, label: str, marked: bool = False):
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

    elif kind == 'place':
        ax.add_patch(mpatches.Circle((x, y), _PLACE_R,
                                     fc='black' if marked else 'white',
                                     ec='black', lw=1.5, zorder=3))

    elif kind == 'silent':
        ax.add_patch(mpatches.FancyBboxPatch(
            (x - _SILENT_W / 2, y - _SILENT_H / 2), _SILENT_W, _SILENT_H,
            boxstyle="square,pad=0", facecolor='black', edgecolor='black', zorder=3,
        ))

    else:  # gateway
        symbol = {'xor': '×', 'and': '+', 'or': 'O'}.get(kind, '?')
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
        _draw_node(ax, x, y, node_info[nid]['kind'], node_info[nid]['label'],
                   marked=node_info[nid].get('marked', False))

    return fig


# ── Save helpers ──────────────────────────────────────────────────────────────

def _save_png(fig: Figure, path: str):
    FigureCanvasAgg(fig)
    fig.savefig(path, dpi=150, bbox_inches='tight')


def _patch_gviz_durations(gviz, activity_durations: dict = None):
    """Injects activity durations into a graphviz Digraph's DOT body before
    rendering, by patching label="<act>" -> label="<act>\\n(<dur>)". Sorted
    longest-first to prevent short names being matched inside longer ones."""
    if not activity_durations or not hasattr(gviz, 'body'):
        return
    for act, dur in sorted(activity_durations.items(), key=lambda x: -len(x[0])):
        dur_str = _format_duration(dur)
        for i in range(len(gviz.body)):
            gviz.body[i] = gviz.body[i].replace(
                f'label="{act}"',
                f'label="{act}\\n({dur_str})"',
            )


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
        _patch_gviz_durations(gviz, activity_durations)
        bpmn_visualizer.save(gviz, png_path)
        return True
    except Exception as e:
        logger.debug("PM4Py BPMN visualizer unavailable (%s); falling back to matplotlib.", e)
        return False


def _pm4py_png_petri_net(net, im, fm, png_path: str, activity_durations: dict = None) -> bool:
    """Try PM4Py's graphviz Petri net renderer. Returns True on success, False if graphviz absent."""
    try:
        from pm4py.visualization.petri_net import visualizer as pn_visualizer
        gviz = pn_visualizer.apply(net, im, fm)
        _patch_gviz_durations(gviz, activity_durations)
        pn_visualizer.save(gviz, png_path)
        return True
    except Exception as e:
        logger.debug("PM4Py Petri net visualizer unavailable (%s); falling back to matplotlib.", e)
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
            'task':   ('box',     '#2E6DA4', '#1A4A75', 'white'),
            'start':  ('dot',     'white',   'black',   'black'),
            'end':    ('dot',     'black',   'black',   'white'),
            'xor':    ('diamond', 'white',   'black',   'black'),
            'and':    ('diamond', 'white',   'black',   'black'),
            'or':     ('diamond', 'white',   'black',   'black'),
            'place':  ('dot',     'white',   'black',   'black'),
            'silent': ('box',     'black',   'black',   'white'),
        }
        _GW_SYMBOL = {'xor': '×', 'and': '+', 'or': 'O'}

        for nid, info in node_info.items():
            kind  = info['kind']
            label = info['label'] or _GW_SYMBOL.get(kind, '')
            shape, bg, border, fc = _SHAPE.get(kind, ('box', '#aaa', '#666', 'black'))
            if kind == 'place' and info.get('marked'):
                bg = 'black'
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

def _compute_dfg_dict(events_df: SparkDataFrame, end_time: str = None):
    """Computes a pm4py-compatible DFG (dict[(source,target)] -> frequency) plus
    start/end activity frequency dicts, using our own scalable Spark aggregation
    instead of pm4py's log-based discovery. Only the small set of distinct
    activity pairs / activities is ever collected to the driver.
    """
    w = Window.partitionBy("trace_id").orderBy("position")
    pairs = (
        events_df
        .withColumn("target", F.lead("activity").over(w))
        .filter(F.col("target").isNotNull())
    )
    freq_rows = (
        pairs.groupBy("activity", "target")
        .agg(F.count(F.lit(1)).alias("freq"))
        .withColumnRenamed("activity", "source")
        .collect()
    )
    dfg = {(r["source"], r["target"]): int(r["freq"]) for r in freq_rows}

    w2 = Window.partitionBy("trace_id")
    bounded = (
        events_df
        .withColumn("_minp", F.min("position").over(w2))
        .withColumn("_maxp", F.max("position").over(w2))
    )
    start_rows = (
        bounded.filter(F.col("position") == F.col("_minp"))
        .groupBy("activity").agg(F.count(F.lit(1)).alias("freq")).collect()
    )
    end_rows = (
        bounded.filter(F.col("position") == F.col("_maxp"))
        .groupBy("activity").agg(F.count(F.lit(1)).alias("freq")).collect()
    )
    start_activities = {r["activity"]: int(r["freq"]) for r in start_rows}
    end_activities = {r["activity"]: int(r["freq"]) for r in end_rows}

    return dfg, start_activities, end_activities


def _apply_noise_filter(dfg: dict, start_acts: dict, end_acts: dict, noise_threshold: float):
    from pm4py.algo.filtering.dfg import dfg_filtering
    return dfg_filtering.filter_dfg_on_paths_percentage(
        dfg, start_acts, end_acts, 1.0 - noise_threshold
    )


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


def _discover_dfg(dfg: dict, start_acts: dict, end_acts: dict, activity_durations: dict = None):
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


# ── Petri net ─────────────────────────────────────────────────────────────────

def _build_petri_net_graph(net, im, fm, activity_durations: dict = None):
    G = nx.DiGraph()
    node_info: dict = {}

    for p in net.places:
        nid = id(p)
        G.add_node(nid)
        node_info[nid] = {'kind': 'place', 'label': '', 'marked': p in im}

    for t in net.transitions:
        nid = id(t)
        if t.label is None:
            node_info[nid] = {'kind': 'silent', 'label': ''}
        else:
            label = t.label
            if activity_durations and label in activity_durations:
                label = f"{label}\n({_format_duration(activity_durations[label])})"
            node_info[nid] = {'kind': 'task', 'label': label}
        G.add_node(nid)

    for arc in net.arcs:
        src_id = id(arc.source)
        tgt_id = id(arc.target)
        if src_id in node_info and tgt_id in node_info:
            G.add_edge(src_id, tgt_id)

    return G, node_info


def _export_petri_net(net, im, fm, activity_durations: dict = None):
    model_path = _new_tempfile('.pnml')
    png_path   = _new_tempfile('.png')
    html_path  = _new_tempfile('.html')

    # NOTE: signature is (net, initial_marking, output_filename, final_marking=...).
    # Passing final_marking positionally silently corrupts the export.
    pnml_exporter.apply(net, im, model_path, final_marking=fm)

    try:
        G, node_info = _build_petri_net_graph(net, im, fm, activity_durations)
        pos = _layered_layout(G)
        mpl_fig = None

        # PNG: PM4Py/graphviz -> matplotlib fallback
        if not _pm4py_png_petri_net(net, im, fm, png_path, activity_durations):
            mpl_fig = _build_figure(G, pos, node_info, {})
            _save_png(mpl_fig, png_path)

        # HTML: pyvis (interactive) -> matplotlib SVG fallback
        if not _save_html_pyvis(G, node_info, {}, html_path, 'Petri Net Process Model'):
            if mpl_fig is None:
                mpl_fig = _build_figure(G, pos, node_info, {})
            _save_html_static(mpl_fig, html_path, 'Petri Net Process Model')
    except Exception:
        logger.exception("Petri net visualization failed.")

    return model_path, 'pnml', png_path, html_path


# ── Inductive Miner (DFG-only mode) ────────────────────────────────────────────

def _discover_process_tree(dfg: dict, start_acts: dict, end_acts: dict):
    """Discovers a sound, block-structured ProcessTree purely from an aggregated
    DFG - no event log required. See module docstring for the rationale.
    """
    dfg_obj = DFG(graph=dfg, start_activities=start_acts, end_activities=end_acts)
    return inductive_miner.apply(dfg_obj, variant=inductive_miner.Variants.IMd)


# ── Activity durations helper ──────────────────────────────────────────────────

def compute_activity_durations_map(events_df: SparkDataFrame, end_time: str = None) -> dict | None:
    """Per-activity average duration in seconds, as a plain dict (for annotating
    process-model visualizations). Returns None if end_time is not provided.
    """
    if not end_time:
        return None
    try:
        end_raw = F.col("attributes").getItem(end_time)
        end_ts_expr = F.when(end_raw.isNull(), None).when(
            end_raw.rlike('^[0-9]+$'), end_raw.cast('long')
        ).otherwise(F.unix_timestamp(F.to_timestamp(end_raw)))
        dur_pd = (
            events_df
            .withColumn("_end", end_ts_expr)
            .withColumn("_dur", F.col("_end") - F.col("start_timestamp"))
            .filter(F.col("_dur").isNotNull())
            .groupBy("activity")
            .agg(F.avg("_dur").alias("avg_duration"))
            .toPandas()
        )
        return {
            row["activity"]: float(row["avg_duration"])
            for _, row in dur_pd.dropna(subset=["avg_duration"]).iterrows()
        }
    except Exception:
        logger.exception("Failed to compute activity durations; continuing without.")
        return None


# ── Public API ────────────────────────────────────────────────────────────────

def discover_process_model(events_df: SparkDataFrame, algo: str = "bpmn",
                           end_time: str = None, noise_threshold: float = 0.0,
                           activity_durations: dict = None):
    """
    Discovers a process model from an indexed event log's Spark sequence table.

    Args:
        events_df: Sequence table DataFrame (trace_id, activity, position,
                   start_timestamp, attributes).
        algo: 'dfg' -> Directly-Follows Graph (exported as XML);
              'bpmn' -> BPMN via the DFG-based Inductive Miner (BPMN 2.0 XML);
              'petri_net' -> Petri net via the same Inductive Miner (PNML).
        end_time: Optional attributes-map key for event end timestamp, used only
                  to compute per-activity average durations for visualisation
                  annotations (does not affect the DFG itself).
        noise_threshold: 0.0-1.0, higher = simpler model (prunes low-frequency
                         DFG paths before discovery).
        activity_durations: Optional pre-computed {activity: avg_seconds}; if not
                            given and end_time is set, computed automatically.

    Returns:
        (model_path, fmt, png_path, html_path)
    """
    if algo not in ('dfg', 'bpmn', 'petri_net'):
        raise ValueError(f"algo must be 'dfg', 'bpmn' or 'petri_net', got '{algo}'")

    if activity_durations is None:
        activity_durations = compute_activity_durations_map(events_df, end_time)

    dfg, start_acts, end_acts = _compute_dfg_dict(events_df, end_time)

    if noise_threshold > 0.0:
        dfg, start_acts, end_acts = _apply_noise_filter(dfg, start_acts, end_acts, noise_threshold)

    if algo == 'dfg':
        return _discover_dfg(dfg, start_acts, end_acts, activity_durations)

    process_tree = _discover_process_tree(dfg, start_acts, end_acts)

    if algo == 'bpmn':
        bpmn_model = pt_converter.apply(process_tree, variant=pt_converter.Variants.TO_BPMN)
        return _export_bpmn_model(bpmn_model, activity_durations)

    net, im, fm = pt_converter.apply(process_tree, variant=pt_converter.Variants.TO_PETRI_NET)
    return _export_petri_net(net, im, fm, activity_durations)
