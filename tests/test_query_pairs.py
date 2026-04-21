"""
Pure-Python unit tests for parse_seql pair-extraction helpers.

No Spark, no MinIO, no external fixtures - just import and call.

Tested functions
----------------
- extract_responded_pairs(pattern) -> List[RespondedPair]
- extract_info_pairs(pattern)      -> set of (source_label, target_label, branch_id)
"""
import pytest
from siesta.modules.query.parse_seql import (
    Quantifier,
    extract_responded_pairs,
    extract_info_pairs,
)


# ══════════════════════════════════════════════════════════════════════
# extract_responded_pairs
# ══════════════════════════════════════════════════════════════════════

class TestExtractRespondedPairs:

    # ── simple sequences ──────────────────────────────────────────────

    def test_two_event_sequence(self):
        pairs = extract_responded_pairs("A B")
        assert len(pairs) == 1
        p = pairs[0]
        assert p.source.label == "A"
        assert p.target.label == "B"
        assert p.source_quantifier == Quantifier.ONE
        assert p.target_quantifier == Quantifier.ONE
        assert p.forbidden_between == ()
        assert p.branch_id == 0

    def test_three_event_sequence_gives_all_forward_pairs(self):
        pairs = extract_responded_pairs("A B C")
        # Expects (A,B), (A,C), (B,C) - all in branch 0
        assert len(pairs) == 3
        keys = [(p.source.label, p.target.label) for p in pairs]
        assert ("A", "B") in keys
        assert ("A", "C") in keys
        assert ("B", "C") in keys
        assert all(p.branch_id == 0 for p in pairs)

    def test_no_backward_pairs(self):
        """Pairs must be forward-only (source comes before target)."""
        pairs = extract_responded_pairs("A B C")
        keys = [(p.source.label, p.target.label) for p in pairs]
        assert ("B", "A") not in keys
        assert ("C", "A") not in keys
        assert ("C", "B") not in keys

    # ── OR branches ───────────────────────────────────────────────────

    def test_or_produces_two_branches(self):
        pairs = extract_responded_pairs("(A||B) C")
        branch_ids = {p.branch_id for p in pairs}
        assert branch_ids == {0, 1}

    def test_or_branch_zero_has_correct_pair(self):
        pairs = extract_responded_pairs("(A||B) C")
        b0 = [p for p in pairs if p.branch_id == 0]
        assert len(b0) == 1
        assert b0[0].source.label == "A"
        assert b0[0].target.label == "C"

    def test_or_branch_one_has_correct_pair(self):
        pairs = extract_responded_pairs("(A||B) C")
        b1 = [p for p in pairs if p.branch_id == 1]
        assert len(b1) == 1
        assert b1[0].source.label == "B"
        assert b1[0].target.label == "C"

    def test_or_in_sequence_each_branch_has_three_pairs(self):
        # A (B||C) D → branch 0: ABD → (A,B),(A,D),(B,D)
        #              branch 1: ACD → (A,C),(A,D),(C,D)
        pairs = extract_responded_pairs("A (B||C) D")
        b0 = [(p.source.label, p.target.label) for p in pairs if p.branch_id == 0]
        b1 = [(p.source.label, p.target.label) for p in pairs if p.branch_id == 1]
        assert set(b0) == {("A", "B"), ("A", "D"), ("B", "D")}
        assert set(b1) == {("A", "C"), ("A", "D"), ("C", "D")}

    # ── quantifiers ───────────────────────────────────────────────────

    def test_star_source_quantifier(self):
        pairs = extract_responded_pairs("A* B")
        assert len(pairs) == 1
        assert pairs[0].source.label == "A"
        assert pairs[0].target.label == "B"
        assert pairs[0].source_quantifier == Quantifier.STAR

    def test_plus_source_quantifier(self):
        pairs = extract_responded_pairs("A+ B")
        assert len(pairs) == 1
        assert pairs[0].source_quantifier == Quantifier.PLUS

    def test_plus_target_quantifier(self):
        pairs = extract_responded_pairs("A B+")
        assert len(pairs) == 1
        assert pairs[0].target_quantifier == Quantifier.PLUS

    def test_star_and_plus_together(self):
        # A* B+ C → pairs: (A*,B+,?), (A*,C), (B+,C)
        pairs = extract_responded_pairs("A* B+ C")
        keys = [(p.source.label, p.target.label) for p in pairs]
        assert ("A", "B") in keys
        assert ("A", "C") in keys
        assert ("B", "C") in keys
        ab = next(p for p in pairs if p.source.label == "A" and p.target.label == "B")
        assert ab.source_quantifier == Quantifier.STAR
        assert ab.target_quantifier == Quantifier.PLUS

    # ── negation ──────────────────────────────────────────────────────

    def test_negation_pair_endpoints(self):
        """Negated activity is not a pair endpoint; only A and C are."""
        pairs = extract_responded_pairs("A !B C")
        assert len(pairs) == 1
        p = pairs[0]
        assert p.source.label == "A"
        assert p.target.label == "C"

    def test_negation_forbidden_between_populated(self):
        pairs = extract_responded_pairs("A !B C")
        p = pairs[0]
        assert len(p.forbidden_between) == 1
        assert p.forbidden_between[0].label == "B"

    def test_negation_outside_pair_no_forbidden(self):
        """Negated activity after all positive activities is not forbidden-between."""
        pairs = extract_responded_pairs("A B !C")
        ab = next(p for p in pairs if p.source.label == "A" and p.target.label == "B")
        assert ab.forbidden_between == ()

    def test_multiple_negated_forbidden(self):
        pairs = extract_responded_pairs("A !B !C D")
        ad = next(p for p in pairs if p.source.label == "A" and p.target.label == "D")
        forbidden_labels = {a.label for a in ad.forbidden_between}
        assert forbidden_labels == {"B", "C"}

    # ── attributes ────────────────────────────────────────────────────

    def test_attribute_constraint_preserved_in_source(self):
        pairs = extract_responded_pairs('A[resource="r1"] B')
        assert len(pairs) == 1
        assert len(pairs[0].source.constraints) == 1
        assert pairs[0].source.constraints[0].name == "resource"

    def test_attribute_constraint_not_affects_pair_count(self):
        p_plain = extract_responded_pairs("A B")
        p_attr = extract_responded_pairs('A[resource="r1"] B')
        assert len(p_plain) == len(p_attr)


# ══════════════════════════════════════════════════════════════════════
# extract_info_pairs
# ══════════════════════════════════════════════════════════════════════

class TestExtractInfoPairs:

    # ── no info pairs ─────────────────────────────────────────────────

    def test_plain_sequence_empty(self):
        info = extract_info_pairs("A B")
        assert info == set()

    def test_plain_three_sequence_empty(self):
        info = extract_info_pairs("A B C")
        assert info == set()

    # ── attribute constraints generate self-pairs ──────────────────────

    def test_literal_attr_generates_self_pair(self):
        info = extract_info_pairs('A[resource="r1"] B')
        assert ("A", "A", 0) in info

    def test_literal_attr_only_constrained_activity(self):
        info = extract_info_pairs('A[resource="r1"] B')
        # B has no constraints → no (B,B) pair
        assert ("B", "B", 0) not in info

    def test_two_constrained_activities(self):
        info = extract_info_pairs('A[resource="r1"] B[role="analyst"]')
        assert ("A", "A", 0) in info
        assert ("B", "B", 0) in info

    # ── quantifiers generate self-pairs ───────────────────────────────

    def test_star_generates_self_pair(self):
        info = extract_info_pairs("A* B")
        assert ("A", "A", 0) in info

    def test_plus_generates_self_pair(self):
        info = extract_info_pairs("A+ B")
        assert ("A", "A", 0) in info

    def test_one_quantifier_no_self_pair(self):
        """ONE activities without constraints must not produce self-pairs."""
        info = extract_info_pairs("A B")
        assert len([x for x in info if x[0] == "A"]) == 0

    # ── negation generates self-pairs ─────────────────────────────────

    def test_negated_activity_generates_self_pair(self):
        info = extract_info_pairs("A !B C")
        assert ("B", "B", 0) in info

    def test_negated_activity_not_positive_self_pair(self):
        info = extract_info_pairs("A !B C")
        # A and C are plain positive, no self-pairs
        assert ("A", "A", 0) not in info
        assert ("C", "C", 0) not in info

    # ── OR branches give branch-scoped self-pairs ─────────────────────

    def test_or_attr_branch_zero_self_pair(self):
        info = extract_info_pairs('(A[resource="r1"]||B) C')
        assert ("A", "A", 0) in info

    def test_or_no_attr_branch_one_no_self_pair(self):
        info = extract_info_pairs('(A[resource="r1"]||B) C')
        assert ("B", "B", 1) not in info

    # ── combined ──────────────────────────────────────────────────────

    def test_attr_and_negation_combined(self):
        info = extract_info_pairs('A[resource="r1"] !B C')
        assert ("A", "A", 0) in info
        assert ("B", "B", 0) in info

    def test_plus_and_attr_same_activity(self):
        info = extract_info_pairs('A[resource="r1"]+ B')
        # A is PLUS with a constraint → still just one (A,A,0) self-pair
        assert ("A", "A", 0) in info
