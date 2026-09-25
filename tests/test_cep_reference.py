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
* ``$N`` is the N-th positive activity of the pattern, counted left to right
  with OR alternatives included and negated activities excluded.  A binding
  constraint must hold between every event of the constrained element and
  every event bound to activity N (so on Kleene closures it holds for every
  pair), never comparing an event with itself.  When activity N bound no event - an empty ``*`` or an OR
  alternative that did not match - or N is out of range, it imposes nothing;
* a negation forbids events of its label(s) that satisfy its constraints,
  strictly between the positive elements around it (from the trace start /
  to the trace end when the pattern has no positive element on that side);
  a negation whose neighbouring positive elements on one side are all empty
  ``*`` closures bounds nothing and is ignored.
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

def _ordinals(node, out):
    """id(ActivityNode) -> $N number (positive activities, left to right)."""
    if isinstance(node, ActivityNode):
        out[id(node)] = len(out) + 1
    elif isinstance(node, SeqNode):
        for element in node.elements:
            _ordinals(element.atom, out)
    elif isinstance(node, OrNode):
        for branch in node.branches:
            _ordinals(branch, out)
    return out


def _alternatives(atom):
    """ActivityNodes an atom (activity or OR of activities) can bind."""
    if isinstance(atom, ActivityNode):
        return [atom]
    if isinstance(atom, OrNode):
        return [b.elements[0].atom for b in atom.branches]
    raise AssertionError(f"reference does not support atom {atom}")


def _literals_hold(node: ActivityNode, event: dict) -> bool:
    if event["name"] != node.label:
        return False
    for c in node.constraints:
        if isinstance(c.value, StringLiteral):
            value = event.get(c.name)
            if c.op == "=" and value != c.value.value:
                return False
            if c.op == "!=" and value == c.value.value:
                return False
        elif not (isinstance(c.value, VarExpr) and c.value.op is None):
            raise AssertionError(f"reference does not support constraint {c}")
    return True


def _bindings_hold(node: ActivityNode, own_events: list, bound: dict) -> bool:
    for c in node.constraints:
        if not isinstance(c.value, VarExpr):
            continue
        refs = bound.get(c.value.var_id)
        if not refs:
            continue  # referenced activity bound nothing (or out of range)
        for ref in refs:
            for own in own_events:
                if own is ref:
                    continue  # a binding never compares an event with itself
                equal = own.get(c.name) == ref.get(c.name)
                if equal != (c.op == "="):
                    return False
    return True


def reference_first_match(pattern: str, events: list):
    ast = parse_pattern(pattern)
    ordinal = _ordinals(ast, {})
    elements = ast.elements if isinstance(ast, SeqNode) else [ast]
    n = len(events)
    positive = [not isinstance(e.atom, NegatedNode) for e in elements]
    matches = set()

    def complete(assign):
        # assign[k]: list of (event index, ActivityNode) for positive elements
        bound = {}
        for group in assign:
            for i, node in group or []:
                bound.setdefault(ordinal[id(node)], []).append(events[i])
        for group in assign:
            by_node = {}
            for i, node in group or []:
                by_node.setdefault(id(node), (node, []))[1].append(events[i])
            for node, own in by_node.values():
                if not _bindings_hold(node, own, bound):
                    return
        if not _negations_hold(assign, bound):
            return
        match = tuple(sorted(i for group in assign if group for i, _ in group))
        if match:  # a match must contain at least one event
            matches.add(match)

    def bind(k, start, assign):
        if k == len(elements):
            complete(assign)
            return
        element = elements[k]
        if not positive[k]:
            bind(k + 1, start, assign + [None])
            return
        options = [(i, a) for i in range(start, n) for a in _alternatives(element.atom)
                   if _literals_hold(a, events[i])]
        if element.quantifier == Quantifier.ONE:
            for i, a in options:
                bind(k + 1, i + 1, assign + [[(i, a)]])
            return
        (node,) = _alternatives(element.atom)  # generated closures are single activities
        if element.quantifier == Quantifier.STAR:
            bind(k + 1, start, assign + [[]])
        candidates = [i for i, _ in options]
        for size in range(1, len(candidates) + 1):
            for subset in itertools.combinations(candidates, size):
                bind(k + 1, subset[-1] + 1, assign + [[(i, node) for i in subset]])

    def _negations_hold(assign, bound):
        for k, element in enumerate(elements):
            if positive[k]:
                continue
            left = [j for j in range(k) if positive[j]]
            right = [j for j in range(k + 1, len(elements)) if positive[j]]
            left_bound = [j for j in left if assign[j]]
            right_bound = [j for j in right if assign[j]]
            if (left and not left_bound) or (right and not right_bound):
                continue  # anchor omitted by an empty '*': nothing to bound
            lo = max(i for i, _ in assign[left_bound[-1]]) if left_bound else -1
            hi = min(i for i, _ in assign[right_bound[0]]) if right_bound else n
            forbidden = _alternatives(element.atom.inner)
            for i in range(lo + 1, hi):
                if any(_literals_hold(f, events[i]) and _bindings_hold(f, [events[i]], bound)
                       for f in forbidden):
                    return False
        return True

    bind(0, 0, [])
    if not matches:
        return []
    return list(min(matches, key=lambda m: (m[0], m[-1], m)))


# -- random patterns and traces --------------------------------------------------

def _constraints(rng, n_positive):
    cs = []
    if rng.random() < 0.4:
        cs.append(f'r{"=" if rng.random() < 0.7 else "!="}"{rng.choice("xy")}"')
    if rng.random() < 0.3:
        k = rng.randint(1, n_positive) if rng.random() < 0.95 else n_positive + 2
        cs.append(f'r{"=" if rng.random() < 0.8 else "!="}${k}')
    return f"[{', '.join(cs)}]" if cs else ""


def random_pattern(rng):
    # element specs: ("pos", [labels], quantifier) or ("neg", [labels])
    specs = []
    for k in range(rng.randint(1, 3)):
        if rng.random() < 0.25 and (k > 0 or rng.random() < 0.3):
            specs.append(("neg", [rng.choice(LABELS) for _ in range(1 if rng.random() < 0.8 else 2)]))
        if rng.random() < 0.15:
            specs.append(("pos", [rng.choice(LABELS), rng.choice(LABELS)], ""))
        else:
            specs.append(("pos", [rng.choice(LABELS)], rng.choice(["", "", "", "+", "*"])))
    if rng.random() < 0.15:
        specs.append(("neg", [rng.choice(LABELS)]))
    n_positive = sum(len(s[1]) for s in specs if s[0] == "pos")

    def alts(labels):
        rendered = [lab + _constraints(rng, n_positive) for lab in labels]
        return rendered[0] if len(rendered) == 1 else f"({' || '.join(rendered)})"

    return " ".join(("!" + alts(s[1])) if s[0] == "neg" else alts(s[1]) + s[2] for s in specs)


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
