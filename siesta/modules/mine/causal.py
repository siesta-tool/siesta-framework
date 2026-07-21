"""
Ad-hoc causal support for ``Response(a,b)`` DECLARE constraints.

SIESTA's ordered miner (:mod:`siesta.modules.mine.ordered`) discovers
``Response(a,b)`` purely from temporal order.  This module complements those
constraints with a **causal support** score, following:

    Giuliani & Zecchini, "Beyond Temporal Relationships: Causal Support in
    Declarative Process Modeling" (PMAI'25).

Pipeline (per the paper):

1. Group the log by *distinct trace variant* (activity sequence); every trace
   instance of a variant is a row, every event's data feature a column.
2. Run a tailored PC causal-discovery per variant:
     - one node per event/activity; the correlation between two nodes is the
       strongest correlation among their feature pairs (multivariate CI test);
     - the skeleton is thinned with Fisher-z (conditional) independence tests;
     - edges are oriented by the log's temporal prior (position order), which
       replaces PC's collider/Meek orientation rules.
3. Aggregate the per-variant graphs: ``strength(a,b)`` is the ratio of instances
   where a causal link ``a -> b`` was detected to the instances where the pair
   co-occurs with ``a`` before ``b``.  Bidirectional edges are post-processed
   with three strategies -- Plain / Max / Diff.
4. Causal support of a pair is the *widest path* between them: the maximum over
   connecting paths of the minimum edge strength along the path.

The heavy lifting is pure ``numpy``/``pandas`` so it is unit-testable and the
synthetic demo runs without a Spark cluster.  :func:`discover_causal_support` is
a thin Spark wrapper that parallelises the per-variant PC via ``applyInPandas``
and annotates the mined Response constraints.

Design notes / ad-hoc simplifications (documented on purpose):
  * a repeated activity inside a variant uses its *first* occurrence;
  * conditioning at order >= 1 reduces each multivariate node to a scalar proxy
    (its strongest-correlating feature) -- order-0 stays fully multivariate.
"""
from __future__ import annotations

from itertools import combinations
from math import atanh, sqrt
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import logging

logger = logging.getLogger(__name__)

# Tunables (kept as module defaults; overridable through the public entrypoints)
DEFAULT_ALPHA = 0.01          # significance level for Fisher-z CI tests
DEFAULT_MIN_SAMPLES = 20      # min trace instances for a variant to be usable
DEFAULT_MAX_COND = 2          # max conditioning-set size in the PC skeleton

# Output schema of the per-variant UDF and the collected edge table
_EDGE_COLUMNS = ["source", "target", "detected_weight", "cooccur_weight"]


# ---------------------------------------------------------------------------
# Feature parsing
# ---------------------------------------------------------------------------
def _numeric_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Coerce an object-typed attribute frame to numeric columns.

    Each column is parsed with :func:`pandas.to_numeric`; columns that are not
    (mostly) numeric are label-encoded via :func:`pandas.factorize` so that
    categorical features (e.g. ``gender``) still participate in correlations.
    Unparseable / missing cells become ``NaN`` and are dropped pairwise later.
    """
    out = {}
    for col in raw.columns:
        num = pd.to_numeric(raw[col], errors="coerce")
        if num.notna().sum() >= max(2, int(0.5 * len(num))):
            out[col] = num.astype(float)
        else:
            codes, _ = pd.factorize(raw[col])
            codes = codes.astype(float)
            codes[codes < 0] = np.nan  # factorize marks NaN as -1
            out[col] = pd.Series(codes, index=raw.index)
    return pd.DataFrame(out, index=raw.index)


def _node_tables(variant_pdf: pd.DataFrame) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """Split one variant's events into per-activity numeric feature tables.

    Returns ``(node_tables, activity_order)`` where ``node_tables[activity]`` is
    a DataFrame indexed by ``trace_id`` (one row per instance) and
    ``activity_order`` lists activities by their mean first position -- the
    temporal prior used to orient edges.
    """
    pdf = variant_pdf.sort_values(["trace_id", "position"])
    # First occurrence of each (trace_id, activity)
    first = pdf.groupby(["trace_id", "activity"], as_index=False, sort=False).first()

    node_tables: Dict[str, pd.DataFrame] = {}
    for act, grp in first.groupby("activity", sort=False):
        rows = {}
        for _, r in grp.iterrows():
            attrs = r["attributes"] if isinstance(r["attributes"], dict) else {}
            rows[r["trace_id"]] = attrs
        raw = pd.DataFrame.from_dict(rows, orient="index")
        node_tables[act] = _numeric_frame(raw) if not raw.empty else raw

    # Temporal order: activities sorted by their mean first position
    order = (
        first.groupby("activity")["position"].mean().sort_values().index.tolist()
    )
    return node_tables, order


# ---------------------------------------------------------------------------
# Correlation / independence primitives
# ---------------------------------------------------------------------------
def _pearson(x: pd.Series, y: pd.Series) -> float:
    """Pairwise-complete Pearson correlation; 0 for degenerate inputs."""
    joined = pd.concat([x, y], axis=1, join="inner").dropna()
    if len(joined) < 3:
        return 0.0
    a = joined.iloc[:, 0].to_numpy()
    b = joined.iloc[:, 1].to_numpy()
    if np.std(a) == 0 or np.std(b) == 0:
        return 0.0
    r = float(np.corrcoef(a, b)[0, 1])
    return 0.0 if np.isnan(r) else r


def _node_correlation(
    ti: pd.DataFrame, tj: pd.DataFrame
) -> Tuple[float, Optional[str], Optional[str]]:
    """Strongest absolute correlation over feature pairs between two nodes.

    Returns ``(max_abs_r, best_col_i, best_col_j)`` (paper step 2a).
    """
    best_r, bi, bj = 0.0, None, None
    for ci in ti.columns:
        for cj in tj.columns:
            r = _pearson(ti[ci], tj[cj])
            if abs(r) > abs(best_r):
                best_r, bi, bj = r, ci, cj
    return best_r, bi, bj


def _fisher_z_significant(r: float, n: int, alpha: float) -> bool:
    """Fisher-z test of ``H0: rho == 0``. True when the correlation is significant.

    ``z = atanh(r) * sqrt(n - 3)`` is standard-normal under H0; we reject when
    ``|z|`` exceeds the two-sided critical value for ``alpha``.
    """
    if n <= 3:
        return False
    r = max(min(r, 0.9999), -0.9999)  # guard atanh at +-1
    z = abs(atanh(r)) * sqrt(n - 3)
    return z > _z_critical(alpha)


def _z_critical(alpha: float) -> float:
    """Two-sided standard-normal critical value via an inverse-erf approximation.

    Avoids a scipy dependency (scipy is not a declared requirement).
    """
    # Acklam's rational approximation of the normal quantile function.
    p = 1.0 - alpha / 2.0
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = sqrt(-2 * np.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p > phigh:
        q = sqrt(-2 * np.log(1 - p))
        return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
                ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    q = p - 0.5
    r = q * q
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5]) * q / \
           (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)


def _partial_correlation(
    x: pd.Series, y: pd.Series, conditioners: List[pd.Series]
) -> Tuple[float, int]:
    """Partial correlation of ``x`` and ``y`` given ``conditioners``.

    Computed as the correlation of the residuals after linear regression on the
    conditioning variables (with an intercept).  Returns ``(pcorr, n)`` where
    ``n`` is the number of complete rows used.
    """
    cols = [x.rename("x"), y.rename("y")] + [
        c.rename(f"z{i}") for i, c in enumerate(conditioners)
    ]
    data = pd.concat(cols, axis=1, join="inner").dropna()
    n = len(data)
    if n < len(conditioners) + 4:
        return 0.0, n
    if not conditioners:
        return _pearson(data["x"], data["y"]), n

    Z = np.column_stack([np.ones(n)] + [data[f"z{i}"].to_numpy()
                                        for i in range(len(conditioners))])
    def _residual(v: np.ndarray) -> np.ndarray:
        coef, *_ = np.linalg.lstsq(Z, v, rcond=None)
        return v - Z @ coef

    rx = _residual(data["x"].to_numpy())
    ry = _residual(data["y"].to_numpy())
    if np.std(rx) == 0 or np.std(ry) == 0:
        return 0.0, n
    pr = float(np.corrcoef(rx, ry)[0, 1])
    return (0.0 if np.isnan(pr) else pr), n


# ---------------------------------------------------------------------------
# Tailored PC over one variant
# ---------------------------------------------------------------------------
def _pc_variant(
    node_tables: Dict[str, pd.DataFrame],
    activity_order: List[str],
    n_samples: int,
    alpha: float,
    max_cond: int,
) -> List[Tuple[str, str]]:
    """Run the tailored PC on a single variant and return detected directed edges.

    Directed edges are oriented by ``activity_order`` (temporal prior).
    """
    nodes = [a for a in activity_order if a in node_tables and not node_tables[a].empty]
    if len(nodes) < 2:
        return []

    adj = {a: set(nodes) - {a} for a in nodes}

    # --- order 0: fully multivariate max-feature correlation (paper step 2a) ---
    # The node-pair statistic is the MAX over feature-pair correlations, so its
    # null is not N(0,1); a Bonferroni correction (alpha / #feature-pairs) keeps
    # the effective false-positive rate at alpha and stops spurious edges between
    # genuinely independent nodes.
    for i, j in combinations(nodes, 2):
        r, _, _ = _node_correlation(node_tables[i], node_tables[j])
        ncomp = max(1, node_tables[i].shape[1] * node_tables[j].shape[1])
        if not _fisher_z_significant(r, n_samples, alpha / ncomp):
            adj[i].discard(j)
            adj[j].discard(i)

    # --- order >= 1: multivariate conditional-independence tests ---
    # Independence between two nodes given a conditioning set S is tested by the
    # strongest partial correlation over their feature pairs, regressing out the
    # *full* feature sets of every node in S.  Keeping nodes multivariate (rather
    # than collapsing to one scalar proxy) is what lets a node act as a confounder
    # through different features for different neighbours.
    for l in range(1, max_cond + 1):
        for i, j in list(combinations(nodes, 2)):
            if j not in adj[i]:
                continue
            neighbours = (adj[i] | adj[j]) - {i, j}
            if len(neighbours) < l:
                continue
            for cond in combinations(sorted(neighbours), l):
                cond_features = [node_tables[k][c]
                                 for k in cond for c in node_tables[k].columns]
                r, n = _max_partial_correlation(
                    node_tables[i], node_tables[j], cond_features
                )
                # effective dof: n - (#conditioning features) - 3; same Bonferroni
                # correction as order 0 over the feature-pair maximum.
                ncomp = max(1, node_tables[i].shape[1] * node_tables[j].shape[1])
                if not _fisher_z_significant(r, n - len(cond_features), alpha / ncomp):
                    adj[i].discard(j)
                    adj[j].discard(i)
                    break

    # --- orient by temporal prior (paper step 2b) ---
    pos = {a: idx for idx, a in enumerate(activity_order)}
    edges: List[Tuple[str, str]] = []
    for i, j in combinations(nodes, 2):
        if j in adj[i]:
            edges.append((i, j) if pos[i] < pos[j] else (j, i))
    return edges


def _max_partial_correlation(
    ti: pd.DataFrame, tj: pd.DataFrame, cond_features: List[pd.Series]
) -> Tuple[float, int]:
    """Strongest partial correlation over feature pairs of two nodes given a set.

    The multivariate analogue of :func:`_node_correlation`: for every feature
    pair ``(fi, fj)`` we regress out ``cond_features`` and keep the largest
    absolute partial correlation.  Returns ``(max_abs_pcorr, n)``.
    """
    best_r, best_n = 0.0, 0
    for ci in ti.columns:
        for cj in tj.columns:
            pr, n = _partial_correlation(ti[ci], tj[cj], cond_features)
            if abs(pr) > abs(best_r):
                best_r, best_n = pr, n
    return best_r, best_n


# ---------------------------------------------------------------------------
# Per-variant edge weights (used by both the pandas demo and the Spark UDF)
# ---------------------------------------------------------------------------
def variant_edge_weights(
    variant_pdf: pd.DataFrame,
    alpha: float = DEFAULT_ALPHA,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    max_cond: int = DEFAULT_MAX_COND,
) -> pd.DataFrame:
    """Detect causal edges for one trace variant and weight them by instance count.

    Returns rows ``(source, target, detected_weight, cooccur_weight)`` for every
    ordered pair that co-occurs (source before target) in the variant.
    ``*_weight`` are instance counts, so pooling across variants reproduces the
    paper's ``detected / co-occurring`` ratio.  Under-sampled variants (fewer
    than ``min_samples`` instances) are skipped entirely -- excluded from both
    numerator and denominator -- since their CI tests are unreliable.
    """
    n_samples = variant_pdf["trace_id"].nunique()
    if n_samples < min_samples:
        return pd.DataFrame(columns=_EDGE_COLUMNS)

    node_tables, order = _node_tables(variant_pdf)
    nodes = [a for a in order if a in node_tables and not node_tables[a].empty]
    if len(nodes) < 2:
        return pd.DataFrame(columns=_EDGE_COLUMNS)

    detected = set(_pc_variant(node_tables, order, n_samples, alpha, max_cond))

    pos = {a: idx for idx, a in enumerate(order)}
    rows = []
    for a, b in combinations(nodes, 2):
        src, tgt = (a, b) if pos[a] < pos[b] else (b, a)
        rows.append((src, tgt,
                     float(n_samples) if (src, tgt) in detected else 0.0,
                     float(n_samples)))
    return pd.DataFrame(rows, columns=_EDGE_COLUMNS)


# ---------------------------------------------------------------------------
# Aggregation + strategies + widest path (driver-side, pure pandas/numpy)
# ---------------------------------------------------------------------------
def _aggregate_strengths(edge_df: pd.DataFrame) -> Dict[Tuple[str, str], float]:
    """Pool per-variant weights into ``strength(a,b) = det / cooccur``."""
    if edge_df.empty:
        return {}
    agg = edge_df.groupby(["source", "target"], sort=False).agg(
        det=("detected_weight", "sum"), coo=("cooccur_weight", "sum")
    )
    return {
        (s, t): (row.det / row.coo if row.coo > 0 else 0.0)
        for (s, t), row in agg.iterrows()
    }


def _apply_strategy(
    strengths: Dict[Tuple[str, str], float], strategy: str
) -> Dict[Tuple[str, str], float]:
    """Post-process bidirectional edges (paper step 3): plain / max / diff."""
    if strategy == "plain":
        return dict(strengths)

    out: Dict[Tuple[str, str], float] = {}
    seen = set()
    pairs = set(strengths) | {(b, a) for (a, b) in strengths}
    for a, b in pairs:
        if (a, b) in seen or (b, a) in seen:
            continue
        seen.add((a, b))
        fwd = strengths.get((a, b), 0.0)
        rev = strengths.get((b, a), 0.0)
        if strategy == "max":
            if fwd >= rev:
                out[(a, b)] = fwd
                if rev > fwd:  # unreachable, kept for symmetry
                    out[(b, a)] = rev
            if rev >= fwd:
                out[(b, a)] = rev
        elif strategy == "diff":
            if fwd > rev:
                out[(a, b)] = fwd - rev
            elif rev > fwd:
                out[(b, a)] = rev - fwd
            # equal -> neither direction retained
        else:
            raise ValueError(f"Unknown strategy: {strategy}")
    return {k: v for k, v in out.items() if v > 0}


def _widest_path(
    edge_weights: Dict[Tuple[str, str], float], nodes: List[str]
) -> Dict[Tuple[str, str], float]:
    """Max-min widest path between every ordered node pair (paper step 4).

    ``support(a,b) = max over paths min edge strength`` via a modified
    Floyd-Warshall relaxation ``W[i][j] = max(W[i][j], min(W[i][k], W[k][j]))``.
    """
    idx = {a: i for i, a in enumerate(nodes)}
    n = len(nodes)
    W = np.zeros((n, n), dtype=float)
    for (a, b), w in edge_weights.items():
        if a in idx and b in idx:
            W[idx[a], idx[b]] = max(W[idx[a], idx[b]], w)
    np.fill_diagonal(W, 1.0)  # trivial path to self

    for k in range(n):
        # W = max(W, min(W[:,k], W[k,:]))  broadcast
        through_k = np.minimum(W[:, [k]], W[[k], :])
        W = np.maximum(W, through_k)

    return {
        (nodes[i], nodes[j]): float(W[i, j])
        for i in range(n) for j in range(n) if i != j
    }


def causal_support_from_edges(edge_df: pd.DataFrame) -> pd.DataFrame:
    """Turn pooled per-variant edges into a Plain/Max/Diff causal-support table.

    Returns a DataFrame ``[source, target, plain, max, diff]`` covering every
    ordered node pair reachable in at least one strategy graph.
    """
    strengths = _aggregate_strengths(edge_df)
    nodes = sorted({a for a, _ in strengths} | {b for _, b in strengths})
    if not nodes:
        return pd.DataFrame(columns=["source", "target", "plain", "max", "diff"])

    supports = {
        strat: _widest_path(_apply_strategy(strengths, strat), nodes)
        for strat in ("plain", "max", "diff")
    }

    rows = []
    for a in nodes:
        for b in nodes:
            if a == b:
                continue
            p = supports["plain"].get((a, b), 0.0)
            m = supports["max"].get((a, b), 0.0)
            d = supports["diff"].get((a, b), 0.0)
            if p or m or d:
                rows.append((a, b, p, m, d))
    return pd.DataFrame(rows, columns=["source", "target", "plain", "max", "diff"])


def compute_causal_support(
    sequence_pdf: pd.DataFrame,
    alpha: float = DEFAULT_ALPHA,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    max_cond: int = DEFAULT_MAX_COND,
) -> pd.DataFrame:
    """End-to-end causal support on an in-memory sequence log (no Spark).

    ``sequence_pdf`` must have columns ``trace_id, activity, position,
    attributes`` (``attributes`` a ``dict`` per event).  Returns the
    ``[source, target, plain, max, diff]`` table.
    """
    sig = _variant_signatures(sequence_pdf)
    merged = sequence_pdf.merge(sig, on="trace_id", how="inner")

    parts = []
    for _, variant_pdf in merged.groupby("variant_sig", sort=False):
        parts.append(variant_edge_weights(variant_pdf, alpha, min_samples, max_cond))
    edge_df = (
        pd.concat(parts, ignore_index=True)
        if parts else pd.DataFrame(columns=_EDGE_COLUMNS)
    )
    return causal_support_from_edges(edge_df)


def _variant_signatures(sequence_pdf: pd.DataFrame) -> pd.DataFrame:
    """Map each ``trace_id`` to its variant signature (ordered activity string)."""
    ordered = sequence_pdf.sort_values(["trace_id", "position"])
    sig = (
        ordered.groupby("trace_id")["activity"]
        .apply(lambda s: ">".join(s.tolist()))
        .reset_index()
        .rename(columns={"activity": "variant_sig"})
    )
    return sig


# ---------------------------------------------------------------------------
# Spark entrypoint: parallel per-variant PC + Response annotation
# ---------------------------------------------------------------------------
def discover_causal_support(
    sequence_df,
    metadata,
    alpha: float = DEFAULT_ALPHA,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    max_cond: int = DEFAULT_MAX_COND,
    output_path: Optional[str] = None,
):
    """Compute causal support and annotate the mined ``Response(a,b)`` constraints.

    Runs the per-variant tailored PC in parallel with ``applyInPandas``, pools the
    (small) edge table on the driver, computes Plain/Max/Diff widest-path support,
    and joins it onto the distinct ``Response`` pairs read from storage.

    :param sequence_df: Spark ``DataFrame[Event]`` (from ``read_sequence_table``);
                        must retain the ``attributes`` map column.
    :param metadata:    :class:`~siesta.model.StorageModel.MetaData`.
    :param output_path: optional local CSV path; defaults under ``output/``.
    :return: Spark ``DataFrame[source, target, plain, max, diff, is_response]``.
    """
    import csv
    import datetime
    from pathlib import Path

    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType, DoubleType)
    from siesta.core.storageFactory import get_storage_manager

    spark = sequence_df.sparkSession

    # 1. attach the variant signature to every event
    win_cols = ["trace_id", "activity", "position", "attributes"]
    seq = sequence_df.select(*win_cols)
    sig = (
        seq.groupBy("trace_id")
        .agg(F.sort_array(F.collect_list(F.struct("position", "activity"))).alias("seq"))
        .select("trace_id",
                F.concat_ws(">", F.expr("transform(seq, x -> x.activity)")).alias("variant_sig"))
    )
    seq = seq.join(sig, on="trace_id", how="inner")

    # 2. per-variant PC in parallel
    edge_schema = StructType([
        StructField("source", StringType()),
        StructField("target", StringType()),
        StructField("detected_weight", DoubleType()),
        StructField("cooccur_weight", DoubleType()),
    ])

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        return variant_edge_weights(pdf, alpha, min_samples, max_cond)

    edge_sdf = seq.groupBy("variant_sig").applyInPandas(_udf, edge_schema)

    # 3. pool + widest-path on the driver (activity-sized, tiny)
    edge_pdf = edge_sdf.toPandas()
    support_pdf = causal_support_from_edges(edge_pdf)

    # 4. annotate the mined Response(a,b) constraints
    storage = get_storage_manager()
    try:
        response_pairs = (
            storage.read_ordered_constraints(metadata)
            .where(F.col("template") == "response")
            .select("source", "target").distinct().toPandas()
        )
    except Exception as e:  # storage may be unavailable in some contexts
        logger.warning(f"Could not read ordered constraints for annotation: {e}")
        response_pairs = pd.DataFrame(columns=["source", "target"])

    if not response_pairs.empty:
        response_pairs["is_response"] = True
        support_pdf = support_pdf.merge(response_pairs, on=["source", "target"], how="outer")
        support_pdf["is_response"] = support_pdf["is_response"].fillna(False)
        for c in ("plain", "max", "diff"):
            support_pdf[c] = support_pdf[c].fillna(0.0)
    else:
        support_pdf["is_response"] = False

    # 5. write CSV (mirrors the mining output convention)
    if output_path is None:
        Path("output").mkdir(parents=True, exist_ok=True)
        output_path = f"output/{metadata.log_name}_causal_{datetime.datetime.now().timestamp()}.csv"
    support_pdf.sort_values(["source", "target"]).to_csv(output_path, index=False)
    logger.info(f"Wrote causal support for {len(support_pdf)} pairs to {output_path}")

    result_schema = StructType([
        StructField("source", StringType()),
        StructField("target", StringType()),
        StructField("plain", DoubleType()),
        StructField("max", DoubleType()),
        StructField("diff", DoubleType()),
        StructField("is_response", StringType()),
    ])
    if support_pdf.empty:
        return spark.createDataFrame([], schema=result_schema)
    support_pdf = support_pdf.astype({"is_response": str})
    return spark.createDataFrame(support_pdf[["source", "target", "plain", "max", "diff", "is_response"]])


# ---------------------------------------------------------------------------
# Synthetic e-commerce demo (paper's example) -- runs without Spark
# ---------------------------------------------------------------------------
def generate_ecommerce_example(n_traces: int = 2000, seed: int = 7) -> pd.DataFrame:
    """Synthesise a log with the paper's causal structure.

    Ground-truth mechanism (data features carry the causal signal):
      * ``access`` has two latent attributes ``a0``, ``a1``;
      * ``login.age``  := f(a0)          => access -> login
      * ``purchase.price`` := g(a1)      => access -> purchase
      * ``login`` and ``purchase`` features are mutually independent
        (interleaved, NOT causally related);
      * ``feedback.rating`` := h(age, price)  => login -> feedback, purchase -> feedback
      * ``shipping.cost``   := k(price)        => purchase -> shipping
      * ``shipping`` exists only when ``price < 50``; ``feedback`` only when both
        ``login`` and ``purchase`` are present (60%).

    Returns a sequence-shaped DataFrame (``trace_id, activity, position,
    start_timestamp, attributes``) mirroring ``read_sequence_table`` output.
    """
    rng = np.random.default_rng(seed)
    events = []
    for t in range(n_traces):
        tid = f"t{t}"
        a0, a1 = rng.normal(), rng.normal()
        seq: List[Tuple[str, Dict[str, str]]] = [("access", {"a0": f"{a0:.4f}", "a1": f"{a1:.4f}"})]

        has_login = rng.random() < 0.8
        has_purchase = rng.random() < 0.8

        # access explains ~half of login/purchase variance (moderate R^2), so the
        # chain edges login->feedback / purchase->feedback are NOT screened off by
        # access -- they carry independent signal.
        age = 35 + 3 * a0 + rng.normal(scale=3)
        gender = rng.choice(["M", "F"])
        items = int(rng.integers(1, 6))
        price = 45 + 4 * a1 + 2 * items + rng.normal(scale=3)

        login_ev = ("login", {"age": f"{age:.2f}", "gender": gender})
        purchase_ev = ("purchase", {"items": str(items), "price": f"{price:.2f}"})

        present = []
        if has_login:
            present.append(login_ev)
        if has_purchase:
            present.append(purchase_ev)
        rng.shuffle(present)  # login/purchase interleaved in random order
        seq.extend(present)

        if has_login and has_purchase and rng.random() < 0.6:
            rating = age + price + rng.normal(scale=3)
            seq.append(("feedback", {"rating": f"{rating:.3f}"}))
        if has_purchase and price < 50:
            cost = price + rng.normal(scale=3)
            seq.append(("shipping", {"cost": f"{cost:.2f}"}))

        for pos, (act, attrs) in enumerate(seq):
            events.append({"trace_id": tid, "activity": act,
                           "position": pos, "start_timestamp": pos, "attributes": attrs})
    return pd.DataFrame(events)


def _demo() -> pd.DataFrame:
    logging.basicConfig(level=logging.INFO)
    log = generate_ecommerce_example()
    support = compute_causal_support(log)

    print("\nCausal support for detected pairs (Plain / Max / Diff):")
    print("-" * 60)
    print(f"{'source':>10} -> {'target':<10} {'plain':>7} {'max':>7} {'diff':>7}")
    for _, r in support.sort_values("diff", ascending=False).iterrows():
        print(f"{r['source']:>10} -> {r['target']:<10} "
              f"{r['plain']:>7.3f} {r['max']:>7.3f} {r['diff']:>7.3f}")
    print("-" * 60)
    print("Expected: causal pairs (access->login, access->purchase, purchase->shipping,")
    print("login->feedback, purchase->feedback) retain support; the non-causal")
    print("login<->purchase pair collapses to ~0 under Diff.")
    return support


if __name__ == "__main__":
    _demo()
