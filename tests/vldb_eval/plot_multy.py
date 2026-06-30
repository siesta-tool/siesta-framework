"""
plot_multiperspective.py
Produces fig_multiperspective.pdf / .png

Three-panel figure comparing SIESTA and MATCH_RECOGNIZE latency under
three grouping perspectives (case_id, action, resource) for pattern
lengths 8–15 on BPIC 2017.

MR is shown with:
  - one green diamond at the last completed query (len=8)
  - a gray dotted connecting line rising to the y-axis break
  - a large black × above the break at the first timeout length
    (len=9 for case_id and action; len=8 for resource)

Replace the `siesta` arrays and `mr_pre` values with real measurements
before final publication.
"""

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.lines as mlines

# ── Style (mirrors plot_warmup.py) ───────────────────────────────────────────
plt.rcParams.update({
    "font.family":       "serif",
        "font.serif":         ["STIX", "STIXGeneral", "Liberation Serif",
                               "DejaVu Serif", "Times New Roman"],
                                       "mathtext.fontset":  "stix",
    "font.size":         11,
    "axes.labelsize":    10,
            "axes.titlesize":     10,
            "legend.fontsize":    10,
    "savefig.dpi":       300,
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "axes.grid":         False,
    "lines.linewidth":   1.6,
    "lines.markersize":  5,
    "pdf.fonttype":      42,
    "ps.fonttype":       42,
})

# Colours matching the competitive figure
C_SIESTA = "#b2182b"   # dark crimson
C_MR     = "#1b7837"   # dark green
C_X      = "black"     # timeout × is always black

# ── Data ─────────────────────────────────────────────────────────────────────
lengths = np.arange(8, 16)

def noisy(v0, v1, offs):
    """Linear trend anchored at (len=8, v0) and (len=15, v1) with offsets."""
    return v0 + (v1 - v0) / 7.0 * (lengths - 8) + np.array(offs)

# SIESTA latencies — replace with real JSONL-derived medians for publication
siesta = {
    "case_id":  noisy(8,  14, [ 0.0,  0.4, -0.3,  0.6,  0.8, -0.2,  0.5,  0.0]),
    "action":   noisy(12, 50, [ 0.0,  2.1, -1.5,  3.1, -0.9,  2.6, -1.2,  0.0]),
    "resource": noisy(16, 44, [ 0.0,  1.8, -1.1,  2.4,  0.7, -1.8,  2.9,  0.0]),
}

# MR last-completed-query point (len=8).
# case_id: real minimum from experiment (20 s).
# action / resource: estimates; resource = None because all queries at len=8 timed out.
mr_pre = {
    "case_id":  (8, 20.0),
    "action":   (8, 52.0),
    "resource": None,
}

# Length at which × (first full timeout) is placed
mr_xpos = {
    "case_id": 9,
    "action":  9,
    "resource": 8,   # resource times out from length 8
}

# ── Layout constants ──────────────────────────────────────────────────────────
Y_BOT   = 60.0      # top of lower (data) panel
Y_TOP_L = 660.0     # bottom of upper (timeout) panel — arbitrary dummy range
Y_TOP_H = 760.0
Y_X     = 710.0     # y-value where × is drawn in the upper panel

# ── Figure ───────────────────────────────────────────────────────────────────
fig = plt.figure(figsize=(7.5, 3.1))
gs  = fig.add_gridspec(
    2, 3,
    height_ratios=[0.55, 3.0],
    hspace=0.06,
    left=0.10, right=0.96, top=0.80, bottom=0.14, wspace=0.10,
)

axes_t = [fig.add_subplot(gs[0, j]) for j in range(3)]
axes_b = [fig.add_subplot(gs[1, j]) for j in range(3)]

for j, persp in enumerate(["case_id", "action", "resource"]):
    ax_t, ax_b = axes_t[j], axes_b[j]
    sv  = siesta[persp]
    pre = mr_pre[persp]     # (x, y) or None
    tx  = mr_xpos[persp]    # x-position of ×

    # ── Bottom panel: SIESTA line + MR dot + connecting dotted line ──────────
    ax_b.plot(lengths, sv,
              color=C_SIESTA, marker="s", markersize=4.5, linewidth=1.6,
              zorder=3, markerfacecolor=C_SIESTA, markeredgewidth=0)

    if pre is not None:
        px, py = pre
        ax_b.scatter([px], [py],
                     color=C_MR, marker="D", s=30, zorder=5, linewidths=0)
        # Gray dotted line from the MR dot upward — clipped at Y_BOT
        ax_b.plot([px, tx], [py, Y_BOT],
                  color="gray", linewidth=0.9, linestyle=":",
                  zorder=4, clip_on=True)

    ax_b.set_xlim(7.6, 15.4)
    ax_b.set_ylim(0, Y_BOT)
    ax_b.set_xticks(lengths)
    ax_b.set_xticklabels([str(l) if l % 2 == 0 else "" for l in lengths])
    ax_b.set_xlabel("Pattern Length", fontsize=10)
    ax_b.spines["top"].set_visible(False)
    ax_b.spines["right"].set_visible(False)

    # Perspective name — bottom-right corner
    ax_b.text(0.97, 0.05, persp,
              transform=ax_b.transAxes, ha="right", va="bottom",
              fontsize=9, fontfamily="monospace", color="#111111",
              bbox=dict(boxstyle="round,pad=0.12", fc="white",
                        ec="none", alpha=0.88))

    # ── Top panel: black × above the break ───────────────────────────────────
    ax_t.scatter([tx], [Y_X],
                 color=C_X, marker=r"$\times$", s=220,
                 linewidths=1.0, zorder=5, clip_on=False)

    # Gray dotted line continuing upward from the break to the ×
    if pre is not None:
        ax_t.plot([tx, tx], [Y_TOP_L, Y_X],
                  color="gray", linewidth=0.9, linestyle=":",
                  zorder=4, clip_on=True)

    ax_t.set_xlim(7.6, 15.4)
    ax_t.set_ylim(Y_TOP_L, Y_TOP_H)
    ax_t.set_xticks(lengths)
    ax_t.set_xticklabels([])
    ax_t.tick_params(axis="x", bottom=False)
    ax_t.tick_params(axis="y", left=False, labelleft=False)
    ax_t.spines["bottom"].set_visible(False)
    ax_t.spines["right"].set_visible(False)

    # ── Break indicators (//) — both panels, left-side only ──────────────────
    d  = 0.025
    kw = dict(color="k", linewidth=0.9, clip_on=False, zorder=10)
    ax_b.plot((-d, +d), (1 - d / 2, 1 + d / 2),
              transform=ax_b.transAxes, **kw)
    ax_t.plot((-d, +d), (-d / 2, +d / 2),
              transform=ax_t.transAxes, **kw)

# ── Annotations and axis labels ───────────────────────────────────────────────
# ">700 s" label inside the top-left panel
axes_t[0].text(0.3, 0.50, ">700 s",
               transform=axes_t[0].transAxes,
               va="center", ha="left",
               fontsize=7.5, color="#555555")

axes_b[0].set_ylabel("Latency (s)", fontsize=10)
for ax in axes_b[1:]:
    ax.tick_params(labelleft=False)

# ── Shared legend above all panels ───────────────────────────────────────────
h_s = mlines.Line2D(
    [], [], color=C_SIESTA, marker="s", markersize=4.5,
    markerfacecolor=C_SIESTA, markeredgewidth=0,
    linewidth=1.6, label="SIESTA",
)
h_m = mlines.Line2D(
    [], [], color=C_MR, marker="D", linestyle=":",
    markersize=4.5, markerfacecolor=C_MR, markeredgewidth=0,
    linewidth=1.0,
    label="MATCH_RECOGNIZE  ($\\times$ = timeout)",
)
fig.legend(
    handles=[h_s, h_m],
    loc="upper center",
    bbox_to_anchor=(0.530, 0.9),
    ncol=2, frameon=False,
    handlelength=2.0, handletextpad=0.4, columnspacing=1.5,
)

# ── Save ─────────────────────────────────────────────────────────────────────
fig.savefig("fig_multiperspective.pdf", bbox_inches="tight")
fig.savefig("fig_multiperspective.png", dpi=300, bbox_inches="tight")
plt.close(fig)
print("Saved fig_multiperspective.pdf / .png")