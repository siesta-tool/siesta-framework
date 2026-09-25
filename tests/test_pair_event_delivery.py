"""
The pairs index must deliver every event CEP needs.

Detection rebuilds each trace's events from the fetched pair rows (responded
pairs plus ``extract_info_pairs``) and runs CEP on them.  The pairs index is
built with skip-till-next-match, so these tests check, without Spark, that the
fetched rows are enough: CEP over the rebuilt events must equal the brute-force
reference over the full trace.  Patterns that can match a single event are
answered from the activity index instead, so they are excluded here.
"""
import random
from collections import defaultdict

import pytest

from siesta.modules.index.computations import _calculate_pairs_stnm, _parse_lookback
from siesta.modules.query.CEP_adapter import find_occurrences_dsl
from siesta.modules.query.parse_seql import (
    Quantifier, can_match_single_event, extract_info_pairs, extract_responded_pairs,
)
from test_cep_reference import random_pattern, random_trace, reference_first_match

LOOKBACK = _parse_lookback("30d")


def detect_from_pairs(pattern, events):
    """Mirror of detection_query._events_from_pairs + _first_match for one trace."""
    trace = [(e["name"], i * 60, i, {k: v for k, v in e.items() if k != "name"})
             for i, e in enumerate(events)]
    rows, _ = _calculate_pairs_stnm(("t", trace), None, LOOKBACK)
    responded = set(extract_responded_pairs(pattern))
    wanted = {(rp.source.label, rp.target.label) for rp in responded}
    wanted |= {(p[0], p[1]) for p in extract_info_pairs(pattern)}
    tagged = [r for r in rows if (r[0], r[1]) in wanted]

    required = defaultdict(set)
    for rp in responded:
        if Quantifier.STAR not in (rp.source_quantifier, rp.target_quantifier):
            required[rp.branch_id].add((rp.source.label, rp.target.label))
    present = {(r[0], r[1]) for r in tagged}
    if required:
        if not any(req <= present for req in required.values()):
            return []
    elif not tagged:
        return []

    seen = {}
    for r in tagged:
        for name, pos, attrs in ((r[0], r[5], r[7]), (r[1], r[6], r[8])):
            seen.setdefault(pos, {"name": name, "position": pos, **(attrs or {})})
    rebuilt = [seen[k] for k in sorted(seen)]
    idxs = find_occurrences_dsl([e["name"] for e in rebuilt], pattern, events=rebuilt)
    return [rebuilt[i]["position"] for i in idxs]


# -- the counterexamples that motivated the info-pair rules ---------------------

@pytest.mark.parametrize("pattern, trace, expected", [
    # a later, STNM-skipped A is needed because the first one is blocked
    ("A !B C", ["A", "B", "A", "C"], [2, 3]),
    ("A !C B", ["A", "C", "A", "B"], [2, 3]),
    # a single forbidden A must reach CEP although C* is empty
    ("D C* !A D", ["D", "A", "D"], []),
    # a D whose bound value matches, skipped by STNM
    ('D C[r="y", r=$1]', [("D", "x"), ("D", "y"), ("C", "y")], [1, 2]),
    # same (source, target) with different quantifiers in one branch
    ("D !D A* (A || C)", ["D", "B", "D", "A", "A"], [2, 3]),
])
def test_counterexamples(pattern, trace, expected):
    events = [({"name": t} if isinstance(t, str) else {"name": t[0], "r": t[1]}) for t in trace]
    assert detect_from_pairs(pattern, events) == expected


def test_info_pairs_rules():
    assert extract_info_pairs("A B") == set()
    # negation: every positive label gets its self-pair
    assert {(a, b) for a, b, *_ in extract_info_pairs("A !B C")} >= {("A", "A"), ("C", "C"), ("B", "B"), ("A", "B")}
    # anchors: every positive before the negation up to the first non-optional one
    anchors = {p for p in extract_info_pairs("D C* !A D") if len(p) == 2}
    assert anchors == {("C", "A"), ("D", "A")}
    anchors = {p for p in extract_info_pairs("!X C* D") if len(p) == 2}
    assert anchors == {("X", "C"), ("X", "D")}


@pytest.mark.parametrize("pattern, expected", [
    ("A B", False), ("A+ B", False), ("B[r!=\"x\"]", True), ("A+", True),
    ("D !C D*", True), ("A !B", True), ("(A B || C)", True), ("(A || B C) D", False),
])
def test_can_match_single_event(pattern, expected):
    assert can_match_single_event(pattern) is expected


def test_responded_pairs_keep_quantifier_variants():
    pairs = extract_responded_pairs("D !D A* (A || C)")
    assert len(set(pairs)) == len(pairs)


@pytest.mark.parametrize("seed", range(6))
def test_pairs_deliver_every_needed_event(seed):
    rng = random.Random(1000 + seed)
    mismatches = []
    for _ in range(50):
        pattern = random_pattern(rng)
        if can_match_single_event(pattern):
            continue
        for _ in range(15):
            events = random_trace(rng)
            expected = reference_first_match(pattern, events)
            got = detect_from_pairs(pattern, events)
            if got != expected:
                mismatches.append((pattern, [(e["name"], e.get("r")) for e in events], got, expected))
    assert not mismatches, f"{len(mismatches)} mismatches, e.g.: " + "\n".join(map(str, mismatches[:5]))
