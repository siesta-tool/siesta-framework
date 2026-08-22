#!/usr/bin/env python3
"""
succession_trees.py

Filter mined DECLARE 'response' rules above a per-edge support threshold,
chain them into succession trees where the path support is the product of
individual edge supports, prune branches whose cumulative support falls below
a second threshold, and render each tree as a page in a multi-page PDF.

Usage:
    python succession_trees.py [--csv PATH] [--min-support 0.5]
                               [--prune-support 0.1] [--output out.pdf]
                               [--max-trees 0]
"""

import argparse
import sys
import textwrap
from collections import defaultdict
from dataclasses import dataclass, field

import matplotlib
matplotlib.use("Agg")                                   # no display required
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd


# ── Constants ────────────────────────────────────────────────────────────────

LABEL_WRAP = 42          # chars per line for node labels
MAX_FIG_WIDTH = 40       # inches – cap for very bushy trees
MAX_FIG_HEIGHT = 50      # inches


# ── Data model ───────────────────────────────────────────────────────────────

@dataclass
class TreeNode:
    label: str
    edge_support: float   # support on the incoming edge  (0.0 for root)
    cum_support: float    # product of all edge supports from root to here
    children: list = field(default_factory=list)


# ── I/O ──────────────────────────────────────────────────────────────────────

def load_response_rules(csv_path: str, min_support: float) -> pd.DataFrame:
    """
    Read the CSV in chunks, keep only category=='ordered' & template=='response'
    rows above min_support, and return a de-duplicated DataFrame with columns
    [source, target, support].  Duplicate (source, target) pairs are reduced to
    their maximum support.
    """
    chunks = []
    print("  Reading CSV chunks …", end="", flush=True)
    for i, chunk in enumerate(
        pd.read_csv(
            csv_path,
            chunksize=100_000,
            usecols=["category", "template", "source", "target", "support"],
            dtype={
                "category": "string",
                "template": "string",
                "source": "string",
                "target": "string",
                "support": float,
            },
        ),
        start=1,
    ):
        filtered = chunk[
            (chunk["category"] == "ordered")
            & (chunk["template"] == "response")
            & (chunk["support"] >= min_support)
        ]
        if not filtered.empty:
            chunks.append(filtered[["source", "target", "support"]])
        if i % 10 == 0:
            print(f" {i*100_000:,}", end="", flush=True)
    print()

    if not chunks:
        return pd.DataFrame(columns=["source", "target", "support"])

    df = pd.concat(chunks, ignore_index=True)
    # Collapse duplicate (source, target) pairs – keep highest support
    df = (
        df.groupby(["source", "target"], sort=False)["support"]
        .max()
        .reset_index()
    )
    return df


# ── Graph construction ────────────────────────────────────────────────────────

def build_adjacency(df: pd.DataFrame):
    """
    Build an adjacency dict {source: [(target, edge_support), …]} and the set
    of all nodes that appear as targets (used to identify roots).
    """
    adj = defaultdict(list)
    all_targets: set[str] = set()
    for row in df.itertuples(index=False):
        adj[row.source].append((row.target, row.support))
        all_targets.add(row.target)
    return adj, set(adj.keys()), all_targets


# ── Tree construction ─────────────────────────────────────────────────────────

def build_trees(
    adj: dict,
    all_sources: set,
    all_targets: set,
    prune_support: float,
) -> list[TreeNode]:
    """
    Perform DFS from every root node (sources that never appear as a target).

    Pruning rules:
    - A branch is pruned when the cumulative support (product of all edge
      supports from the root) drops below prune_support.
    - A branch is also cut if it would revisit a node already on the current
      path (cycle guard).

    A node may appear in more than one tree; within a single tree its subtree
    is duplicated if it is reachable via multiple paths – this produces proper
    trees rather than DAGs.
    """
    roots = sorted(all_sources - all_targets)

    def dfs(label: str, cum: float, path: frozenset) -> TreeNode:
        node = TreeNode(label=label, edge_support=0.0, cum_support=cum)
        for target, edge_sup in adj.get(label, []):
            new_cum = cum * edge_sup
            if new_cum < prune_support:
                continue
            if target in path:          # cycle guard
                continue
            child = dfs(target, new_cum, path | {target})
            child.edge_support = edge_sup
            node.children.append(child)
        # Sort children by descending cumulative support for a tidy layout
        node.children.sort(key=lambda n: n.cum_support, reverse=True)
        return node

    trees = []
    for root in roots:
        tree = dfs(root, 1.0, frozenset({root}))
        trees.append(tree)
    return trees


# ── Tree metrics helpers ──────────────────────────────────────────────────────

def node_count(node: TreeNode) -> int:
    return 1 + sum(node_count(c) for c in node.children)


def leaf_count(node: TreeNode) -> int:
    if not node.children:
        return 1
    return sum(leaf_count(c) for c in node.children)


def max_depth(node: TreeNode, depth: int = 0) -> int:
    if not node.children:
        return depth
    return max(max_depth(c, depth + 1) for c in node.children)


# ── Layout ────────────────────────────────────────────────────────────────────

def _assign_xy(
    node: TreeNode,
    depth: int,
    x_start: float,
    positions: dict,
    x_step: float,
) -> None:
    """
    Recursive Reingold–Tilford-style positional assignment.
    Each leaf gets one x_step of horizontal space; internal nodes are centred
    over their children.  y = -depth.
    """
    leaves = leaf_count(node)
    x_center = x_start + (leaves * x_step) / 2.0
    positions[id(node)] = (x_center, float(-depth))
    cur_x = x_start
    for child in node.children:
        _assign_xy(child, depth + 1, cur_x, positions, x_step)
        cur_x += leaf_count(child) * x_step


def tree_layout(root: TreeNode, x_step: float = 1.0) -> dict:
    """Return {id(node): (x, y)} for every node in the tree."""
    positions: dict = {}
    _assign_xy(root, depth=0, x_start=0.0, positions=positions, x_step=x_step)
    return positions


# ── Drawing ───────────────────────────────────────────────────────────────────

def _wrap_label(label: str) -> str:
    return "\n".join(textwrap.wrap(label, LABEL_WRAP))


def draw_tree(root: TreeNode, ax: plt.Axes, title: str = "") -> None:
    """Render a single succession tree on *ax*."""
    positions = tree_layout(root, x_step=1.0)

    # Collect all nodes via pre-order traversal
    all_nodes: list[TreeNode] = []

    def collect(n: TreeNode) -> None:
        all_nodes.append(n)
        for c in n.children:
            collect(c)

    collect(root)

    # ── Colour scale (cumulative support) ─────────────────────────────────
    cum_values = [n.cum_support for n in all_nodes]
    vmin, vmax = min(cum_values), max(cum_values)
    if vmax == vmin:
        vmax = vmin + 1e-9
    norm = plt.Normalize(vmin=vmin, vmax=vmax)
    cmap = plt.cm.YlOrRd

    # ── Edges ────────────────────────────────────────────────────────────
    def draw_edges(node: TreeNode) -> None:
        px, py = positions[id(node)]
        for child in node.children:
            cx, cy = positions[id(child)]
            # Arrow from parent to child
            ax.annotate(
                "",
                xy=(cx, cy + 0.08),
                xytext=(px, py - 0.08),
                arrowprops=dict(
                    arrowstyle="-|>",
                    color="#888888",
                    lw=0.7,
                    mutation_scale=8,
                ),
                zorder=1,
            )
            # Edge support label at the midpoint
            mid_x = (px + cx) / 2.0
            mid_y = (py + cy) / 2.0
            ax.text(
                mid_x + 0.06,
                mid_y,
                f"{child.edge_support:.3f}",
                fontsize=5,
                ha="left",
                va="center",
                color="#444444",
                bbox=dict(
                    boxstyle="round,pad=0.1",
                    fc="white",
                    alpha=0.7,
                    lw=0,
                ),
                zorder=2,
            )
            draw_edges(child)

    draw_edges(root)

    # Nodes 
    for node in all_nodes:
        x, y = positions[id(node)]
        color = cmap(norm(node.cum_support))

        # Node circle
        ax.scatter(
            [x], [y],
            s=220,
            c=[color],
            zorder=3,
            edgecolors="#333333",
            linewidths=0.5,
        )

        # Label above the node
        ax.text(
            x, y + 0.12,
            _wrap_label(node.label),
            fontsize=4.5,
            ha="center",
            va="bottom",
            zorder=4,
            linespacing=1.3,
            bbox=dict(
                boxstyle="round,pad=0.15",
                fc="#ffffffcc",
                lw=0,
            ),
        )

        # Cumulative support below the node
        ax.text(
            x, y - 0.12,
            f"cumulative={node.cum_support:.4f}",
            fontsize=4,
            ha="center",
            va="top",
            color="#cc3300",
            zorder=4,
        )

    # ── Colorbar ─────────────────────────────────────────────────────────
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    plt.colorbar(sm, ax=ax, fraction=0.03, pad=0.02, label="Cumulative support")

    ax.set_title(title, fontsize=7, pad=6, loc="left")
    ax.axis("off")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build and visualise succession trees from mined DECLARE "
            "'response' rules."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--csv",
        default="mining_geolife_1776798959.137337.csv",
        metavar="PATH",
        help="Path to the mined rules CSV.",
    )
    parser.add_argument(
        "--min-support",
        type=float,
        default=0.5,
        metavar="FLOAT",
        help="Per-edge support floor: only rules ≥ this value are loaded.",
    )
    parser.add_argument(
        "--prune-support",
        type=float,
        default=0.1,
        metavar="FLOAT",
        help=(
            "Cumulative support floor: a branch is pruned when the product "
            "of edge supports along the path drops below this value."
        ),
    )
    parser.add_argument(
        "--output",
        default="succession_trees.pdf",
        metavar="PATH",
        help="Output PDF file.",
    )
    parser.add_argument(
        "--max-trees",
        type=int,
        default=0,
        metavar="N",
        help="Render only the N largest trees (0 = all).",
    )
    args = parser.parse_args()

    # ── Load ──────────────────────────────────────────────────────────────
    print(f"Loading rules  (min-support={args.min_support}) …")
    df = load_response_rules(args.csv, args.min_support)
    print(f"  {len(df):,} unique (source→target) rules kept.")

    if df.empty:
        print(
            "No rules found. Try lowering --min-support (e.g. --min-support 0.15).",
            file=sys.stderr,
        )
        sys.exit(0)

    # ── Build graph ───────────────────────────────────────────────────────
    adj, all_sources, all_targets = build_adjacency(df)
    n_roots = len(all_sources - all_targets)
    print(
        f"  {len(all_sources):,} unique source nodes  |  "
        f"{n_roots:,} root nodes (never a target)."
    )

    # ── Build trees ───────────────────────────────────────────────────────
    print(f"Building succession trees  (prune-support={args.prune_support}) …")
    trees = build_trees(adj, all_sources, all_targets, args.prune_support)
    # Discard trivial single-node trees (root with no surviving children)
    trees = [t for t in trees if t.children]
    trees.sort(key=node_count, reverse=True)
    print(f"  {len(trees):,} trees with ≥ 2 nodes.")

    if not trees:
        print(
            "No multi-node trees produced.\n"
            "Tips:\n"
            "  • Lower --min-support (fewer but longer chains become possible)\n"
            "  • Lower --prune-support (keep more branches)\n"
            "  • Try --min-support 0.15 --prune-support 0.02",
            file=sys.stderr,
        )
        sys.exit(0)

    if args.max_trees > 0:
        total = len(trees)
        trees = trees[: args.max_trees]
        print(f"  Rendering top {len(trees)} of {total} trees (--max-trees).")

    # ── Render PDF ────────────────────────────────────────────────────────
    print(f"Rendering {len(trees):,} tree(s) → {args.output} …")
    with PdfPages(args.output) as pdf:
        for i, tree in enumerate(trees, start=1):
            n_nodes = node_count(tree)
            n_leaves = leaf_count(tree)
            depth = max_depth(tree)

            # Adaptive figure size capped at sane maxima
            fig_w = min(MAX_FIG_WIDTH, max(6.0, n_leaves * 1.8))
            fig_h = min(MAX_FIG_HEIGHT, max(5.0, (depth + 1) * 2.8))

            fig, ax = plt.subplots(figsize=(fig_w, fig_h))

            short_root = (
                tree.label[:70] + "…"
                if len(tree.label) > 70
                else tree.label
            )
            title = (
                f"Tree {i}/{len(trees)}   "
                f"{n_nodes} nodes · {n_leaves} leaves · depth {depth}\n"
                f"root: {short_root}"
            )
            draw_tree(tree, ax, title=title)
            fig.tight_layout(pad=1.2)
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

            if i % 20 == 0 or i == len(trees):
                print(f"  … {i}/{len(trees)} trees written")

    print(f"\nDone.  Output saved to: {args.output}")


if __name__ == "__main__":
    main()
