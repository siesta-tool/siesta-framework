"""
Attribute-predicate pushdown helpers shared by the query engines.

Two builders live here:

* ``build_pair_attr_predicate`` — compiles the attribute constraints of a
  single ``RespondedPair`` into a Spark column predicate over the pair table's
  ``source_attributes`` / ``target_attributes`` MapType columns (used by the
  adaptive executor).

* ``build_event_keep_predicate`` — a pattern-wide row filter for the eager
  detection path.  It drops only pair rows whose *both* endpoint events can
  never take part in a match, so the CEP result is unchanged (see its
  docstring for why a per-pair filter is not safe there).

Both follow main's ``parse_seql`` model: attribute values are
``StringLiteral`` or ``VarExpr`` and the only literal operators are ``"="``
and ``"!="``.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from functools import reduce

from pyspark.sql import functions as F

from siesta.modules.query.parse_seql import (
    Quantifier,
    RespondedPair,
    StringLiteral,
    VarExpr,
    _linearise,
    parse_pattern,
)

logger = logging.getLogger(__name__)

# Keys the CEP adapter / OpenCEP put on every event dict next to the event's
# own attributes.  A constraint on one of these reads that base field inside
# CEP when the event has no such attribute, while the Spark map lookup yields
# NULL — so such constraints are never pushed down.
_RESERVED_EVENT_KEYS = frozenset(
    {"name", "position", "timestamp", "ts", "InternalIndexAttributeName"}
)


def _literal_attr_predicate(side_map: str, name: str, op: str, value):
    """
    Map a single literal attribute constraint to a Spark ``Column`` predicate
    over a MapType column, or return ``None`` when the ``(op, value)`` pair is
    not expressible as a pure column predicate (leaving it to the CEP engine).

    ``side_map`` is the MapType column name (``"source_attributes"`` or
    ``"target_attributes"``); a pair row stores each endpoint's attributes
    there.  A missing key yields a NULL accessor, so an equality comparison
    against a missing attribute evaluates to NULL and the row is dropped — the
    same outcome as CEP, where ``event.get(attr) == literal`` is ``False``.
    For ``!=`` we additionally admit NULL, matching CEP's
    ``None != literal`` → ``True``.
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


def _label_keep_conditions(pattern: str) -> dict:
    """
    For every label that can be filtered, return the list of its occurrences'
    literal constraints: ``{label: [[(attr, op, literal), ...], ...]}``.

    A label is filterable only if *every* occurrence of it, across all OR
    branches, is a positive, quantifier-ONE activity carrying at least one
    ``StringLiteral`` constraint on a non-reserved attribute.  Anything else
    keeps all of that label's events:

      * negated occurrences — CEP applies no attribute conditions to negated
        events, so any event of that label can block a match;
      * ``*`` / ``+`` occurrences — kept conservatively (Kleene closure);
      * an occurrence without literal constraints — any event can match it;
      * a constraint on a reserved event key (see ``_RESERVED_EVENT_KEYS``).

    ``$N`` bindings are ignored (treated as satisfiable), which only makes the
    filter keep more rows, never fewer.
    """
    occurrences = defaultdict(list)
    unfilterable = set()

    for branch in _linearise(parse_pattern(pattern)):
        for ba in branch:
            label = ba.activity.label
            literals = [
                (c.name, c.op, c.value.value)
                for c in ba.activity.constraints
                if isinstance(c.value, StringLiteral)
            ]
            if (ba.negated
                    or ba.quantifier != Quantifier.ONE
                    or not literals
                    or any(name in _RESERVED_EVENT_KEYS for name, _, _ in literals)):
                unfilterable.add(label)
            else:
                occurrences[label].append(literals)

    return {label: occs for label, occs in occurrences.items()
            if label not in unfilterable}


def build_event_keep_predicate(pattern: str):
    """
    Build a pair-row filter for the eager detection path, or ``None`` when the
    pattern leaves nothing to filter.

    Why not filter per responded pair?  The pairs index follows
    skip-till-next-match, so a valid attribute-satisfying match ``A -> B``
    often has no ``(A, B)`` row of its own: CEP finds it by rebuilding the
    trace's events from *all* rows (including the ``(A, A)`` / ``(B, B)``
    self-pairs).  Dropping ``(A, B)`` rows by the pair's attribute predicate
    — before pruning or before CEP — can therefore lose matches.

    This filter is safe instead because it works per *event*: an event is
    "keepable" iff it satisfies the literal constraints of at least one
    occurrence of its label in the pattern (labels that are not filterable,
    see ``_label_keep_conditions``, are always keepable).  A row is kept iff
    its source **or** target event is keepable.  Hence every keepable event
    still reaches CEP, and the only events removed are ones that fail the
    unary conditions of every pattern element they could bind to — they can
    never be part of a match, so CEP's output is unchanged.  Apply it only to
    the rows handed to CEP; trace pruning must keep using the unfiltered rows.
    """
    per_label = _label_keep_conditions(pattern)
    if not per_label:
        return None

    filtered_labels = list(per_label)

    def side_keep(label_col: str, attr_col: str):
        clauses = []
        for label, occs in per_label.items():
            occ_preds = []
            for literals in occs:
                preds = [_literal_attr_predicate(attr_col, name, op, StringLiteral(value))
                         for name, op, value in literals]
                occ_preds.append(reduce(lambda a, b: a & b, preds))
            label_ok = F.coalesce(reduce(lambda a, b: a | b, occ_preds), F.lit(False))
            clauses.append((F.col(label_col) == label) & label_ok)
        clauses.append(~F.col(label_col).isin(filtered_labels))
        return reduce(lambda a, b: a | b, clauses)

    return (side_keep("source", "source_attributes")
            | side_keep("target", "target_attributes"))
