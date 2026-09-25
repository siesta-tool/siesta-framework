"""
Attribute-predicate pushdown helpers for the adaptive query executor.

These helpers originally lived in
``siesta.modules.query.processors.detection_query`` on the adaptive branch.
Main's ``detection_query`` does not carry them, so they are kept here, scoped
to the adaptive query module, and adapted to main's ``parse_seql`` model:
attribute values are ``StringLiteral`` or ``VarExpr`` and the only literal
operators are ``"="`` and ``"!="`` (main's DSL has no numeric literals or
ordering comparisons).  Anything not expressible as a pure column predicate
falls through to the CEP engine, so correctness is always preserved.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from functools import reduce

from pyspark.sql import functions as F

from siesta.modules.query.parse_seql import (
    RespondedPair,
    StringLiteral,
    VarExpr,
)

logger = logging.getLogger(__name__)


def _literal_attr_predicate(side_map: str, name: str, op: str, value):
    """
    Map a single literal attribute constraint to a Spark ``Column`` predicate
    over a MapType column, or return ``None`` when the ``(op, value)`` pair is
    not expressible as a pure column predicate (leaving it to the CEP engine).

    ``side_map`` is the MapType column name (``"source_attributes"`` or
    ``"target_attributes"``); a pair row stores each endpoint's attributes
    there.  A missing key yields a NULL accessor, so an equality comparison
    against a missing attribute evaluates to NULL and the row is dropped — the
    correct "constraint not satisfied" behaviour.  For ``!=`` we additionally
    admit NULL so that ``attr != "x"`` matches events that simply lack the
    attribute.

    Main's ``parse_seql`` models every literal attribute value as a
    ``StringLiteral`` with ``op`` in ``{"=", "!="}``; any other shape falls
    through to the CEP engine.
    """
    accessor = F.col(side_map)[name]

    if isinstance(value, StringLiteral):
        if op == "=":
            return accessor == value.value
        if op == "!=":
            return accessor.isNull() | (accessor != value.value)

    # VarExpr ($-bindings) are handled by build_pair_attr_predicate as
    # cross-side equalities; anything else is left to the CEP engine.
    return None


def build_pair_attr_predicate(rp: RespondedPair):
    """
    Build a Spark column predicate that a pair row of ``rp`` must satisfy to
    honour the attribute constraints declared on its source and target
    activities, and report whether *every* constraint was expressible.

    Returns
    -------
    (predicate, all_pushable) : (Column | None, bool)
        predicate     — conjunction of all expressible attribute predicates,
                        applied to the ``source_attributes`` /
                        ``target_attributes`` MapType columns of the pair
                        table.  ``None`` when there is nothing to push down.
        all_pushable  — ``True`` iff every constraint on both endpoints was
                        turned into a column predicate.  When ``True`` the
                        downstream CEP validation is redundant for this pair:
                        the surviving rows already satisfy the full constraint
                        set.

    Supported, pushable constraint forms
    -------------------------------------
      * literal equality / inequality:  ``A[attr="x"]``
      * cross-side binding equality:     ``A[attr=$1] B[attr=$1]``  ->
        ``source_attributes[attr] == target_attributes[attr]``

    Not pushable (forces CEP):
      * a binding used on only one side (no cross-side equality to form)
      * a binding with an arithmetic offset (``$1+5``) — value arithmetic
        across events is left to the CEP engine
    """
    preds = []
    all_pushable = True

    # -- literal / inequality constraints on each side ----------------------
    for side_map, node in (("source_attributes", rp.source),
                           ("target_attributes", rp.target)):
        for c in node.constraints:
            if isinstance(c.value, VarExpr):
                continue  # bindings handled below
            p = _literal_attr_predicate(side_map, c.name, c.op, c.value)
            if p is None:
                all_pushable = False
            else:
                preds.append(p)

    # -- cross-side binding equalities ($N appearing on both endpoints) -----
    # Collect every binding site: var_id -> [(side_map, attr_name, varexpr), ...]
    var_sites = defaultdict(list)
    for side_map, node in (("source_attributes", rp.source),
                           ("target_attributes", rp.target)):
        for c in node.constraints:
            if isinstance(c.value, VarExpr):
                var_sites[c.value.var_id].append((side_map, c.name, c.value))

    for var_id, sites in var_sites.items():
        sides = {s[0] for s in sites}
        no_offset = all(v.op is None for _, _, v in sites)
        if (len(sites) == 2 and no_offset
                and sides == {"source_attributes", "target_attributes"}):
            # e.g. A[x=$1] B[y=$1]  ->  source_attributes[x] == target_attributes[y]
            (m0, n0, _), (m1, n1, _) = sites
            preds.append(F.col(m0)[n0] == F.col(m1)[n1])
        else:
            # single-use binding or arithmetic offset — leave to CEP
            all_pushable = False

    if not preds:
        return None, all_pushable
    return reduce(lambda a, b: a & b, preds), all_pushable
