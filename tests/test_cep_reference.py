"""
Differential test: ``find_occurrences_dsl`` against a brute-force reference.

The reference enumerates every match of a DSL pattern over a short event list
directly from the parse tree, with no OpenCEP involved, and applies the same
first-match selection as the adapter (earliest start, then earliest end, then
lexicographically smallest index tuple).

Semantics encoded by the reference
----------------------------------
* sequence elements match strictly increasing, disjoint events;
* ``x+`` binds any non-empty increasing subset of matching events,
  ``x*`` additionally the empty one;
* ``(a || b)`` binds one event of either alternative;
* literal constraints: ``attr="v"`` fails on a missing attribute,
  ``attr!="v"`` passes on it;
* ``$1`` compares against the event bound by the first element (on the first
  element itself it compares the event with itself); on a Kleene element it
  must hold for every event of the closure;
* a negation forbids events of its label(s) that satisfy its constraints,
  strictly between the positive elements around it (from the trace start /
  to the trace end when the pattern has no positive element on that side);
  a negation whose neighbouring positive elements on one side are all empty
  ``*`` closures bounds nothing and is ignored.

Generated patterns keep ``$1`` pointing at a leading, unquantified activity,
because the star expansion renumbers ``$N`` when it omits an element before
the referenced one.
"""
import itertools
import random

import pytest

from siesta.modules.query.CEP_adapter import find_occurrences_dsl
from siesta.modules.query.parse_seql import (
    ActivityNode, NegatedNode, OrNode, Quantifier, SeqNode, StringLiteral, VarExpr,
    parse_pattern,
)

LABELS = "ABCD"


# -- reference matcher ---------------------------------------------------------

def _satisfies(node: ActivityNode, event: dict, first_event):
    if event["name"] != node.label:
        return False
    for c in node.constraints:
        if isinstance(c.value, StringLiteral):
            expected = c.value.value
        elif isinstance(c.value, VarExpr) and c.value.var_id == 1 and c.value.op is None:
            if first_event is None:
                return False
            expected = first_event.get(c.name)
        else:
            raise AssertionError(f"reference does not support constraint {c}")
        value = event.get(c.name)
        if c.op == "=" and value != expected:
            return False
        if c.op == "!=" and value == expected:
            return False
    return True


def _alternatives(atom):
    """ActivityNodes an atom (activity or OR of activities) can bind."""
    if isinstance(atom, ActivityNode):
        return [atom]
    if isinstance(atom, OrNode):
        return [b.elements[0].atom for b in atom.branches]
    raise AssertionError(f"reference does not support atom {atom}")


def reference_first_match(pattern: str, events: list):
    ast = parse_pattern(pattern)
    elements = ast.elements if isinstance(ast, SeqNode) else [ast]
    n = len(events)
    positive = [not isinstance(e.atom, NegatedNode) for e in elements]
    matches = set()

    def bind(k, start, assign):
        if k == len(elements):
            if _negations_hold(assign):
                matches.add(tuple(sorted(i for group in assign if group for i in group)))
            return
        element = elements[k]
        if not positive[k]:
            bind(k + 1, start, assign + [None])
            return
        first = events[assign[0][0]] if assign and assign[0] else None
        candidates = [i for i in range(start, n)
                      if any(_satisfies(a, events[i], first if k else events[i])
                             for a in _alternatives(element.atom))]
        if element.quantifier == Quantifier.ONE:
            for i in candidates:
                bind(k + 1, i + 1, assign + [[i]])
            return
        if element.quantifier == Quantifier.STAR:
            bind(k + 1, start, assign + [[]])
        for size in range(1, len(candidates) + 1):
            for subset in itertools.combinations(candidates, size):
                bind(k + 1, subset[-1] + 1, assign + [list(subset)])

    def _negations_hold(assign):
        first = events[assign[0][0]] if assign and assign[0] else None
        for k, element in enumerate(elements):
            if positive[k]:
                continue
            left = [j for j in range(k) if positive[j]]
            right = [j for j in range(k + 1, len(elements)) if positive[j]]
            left_bound = [j for j in left if assign[j]]
            right_bound = [j for j in right if assign[j]]
            if (left and not left_bound) or (right and not right_bound):
                continue  # anchor omitted by an empty '*': nothing to bound
            lo = max(assign[left_bound[-1]]) if left_bound else -1
            hi = min(assign[right_bound[0]]) if right_bound else n
            forbidden = _alternatives(element.atom.inner)
            if any(any(_satisfies(f, events[i], first) for f in forbidden)
                   for i in range(lo + 1, hi)):
                return False
        return True

    bind(0, 0, [])
    if not matches:
        return []
    return list(min(matches, key=lambda m: (m[0], m[-1], m)))


# -- random patterns and traces --------------------------------------------------

def _literal(rng):
    return f'r{"=" if rng.random() < 0.7 else "!="}"{rng.choice("xy")}"'


def _activity(rng, allow_binding):
    constraints = []
    if rng.random() < 0.4:
        constraints.append(_literal(rng))
    if allow_binding and rng.random() < 0.25:
        constraints.append(f'r{"=" if rng.random() < 0.8 else "!="}$1')
    label = rng.choice(LABELS)
    return label + (f"[{', '.join(constraints)}]" if constraints else "")


def random_pattern(rng):
    n_pos = rng.randint(1, 3)
    parts = []
    for k in range(n_pos):
        first = k == 0
        if not first and rng.random() < 0.15:
            atom = f"({_activity(rng, True)} || {_activity(rng, True)})"
            quant = ""
        else:
            atom = _activity(rng, allow_binding=not first)
            quant = "" if first else rng.choice(["", "", "", "+", "*"])
        parts.append(atom + quant)
    # negations: anywhere except that the pattern must not start with one when
    # a binding to $1 would then refer to nothing
    out = []
    for k, part in enumerate(parts):
        if k > 0 and rng.random() < 0.3:
            inner = (_activity(rng, True) if rng.random() < 0.8
                     else f"({_activity(rng, True)} || {_activity(rng, True)})")
            out.append("!" + inner)
        out.append(part)
    if rng.random() < 0.15:
        out.append("!" + _activity(rng, True))
    return " ".join(out)


def random_trace(rng):
    events = []
    for _ in range(rng.randint(1, 7)):
        e = {"name": rng.choice(LABELS)}
        if rng.random() < 0.8:
            e["r"] = rng.choice("xy")
        events.append(e)
    return events


@pytest.mark.parametrize("seed", range(8))
def test_find_occurrences_matches_reference(seed):
    rng = random.Random(seed)
    mismatches = []
    for _ in range(60):
        pattern = random_pattern(rng)
        for _ in range(12):
            events = random_trace(rng)
            expected = reference_first_match(pattern, events)
            got = find_occurrences_dsl([e["name"] for e in events], pattern, events=events)
            if got != expected:
                mismatches.append((pattern, [(e["name"], e.get("r")) for e in events], got, expected))
    assert not mismatches, f"{len(mismatches)} mismatches, e.g.: " + "\n".join(map(str, mismatches[:5]))
