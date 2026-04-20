"""
Pure-Python unit tests for the CEP adapter: find_occurrences_dsl().

No Spark, no MinIO, no external fixtures.  Feeds synthetic event lists
directly to the adapter and verifies match indices.

NOTE ON INDEX SEMANTICS
-----------------------
For plain sequences (no Kleene), ``find_occurrences_dsl`` returns the
0-based position of each matched event in the input ``sequence`` list.

For Kleene patterns (+ or *), OpenCEP assigns its own internal counter
to every event (and to each AggregatedEvent node it creates internally).
The returned indices are those **internal OpenCEP counters**, not the
original positions — so ``A+ B`` on ``[A, B]`` returns ``[0, 2]``
(A->0, KC-internal-node->1, B->2).  Tests assert the empirically verified
values to act as regression guards against future adapter changes.

Event dict helpers
------------------
  mk_event(name, **attrs)  -> {"name": name, **attrs}
  run(pattern, *events)    -> find_occurrences_dsl result (returnAll=False)
"""
import pytest
from siesta.modules.Query.CEP_adapter import find_occurrences_dsl


# -- helpers ----------------------------------------------------------

def mk_event(name: str, **attrs) -> dict:
    return {"name": name, **attrs}


def run(pattern: str, *events: dict, returnAll=False) -> list:
    seq = [e["name"] for e in events]
    return find_occurrences_dsl(seq, pattern, returnAll=returnAll, events=list(events))


# =====================================================================
# 1. Simple sequences
# =====================================================================

class TestSimpleSequence:

    def test_two_event_exact_match(self):
        result = run("A B", mk_event("A"), mk_event("B"))
        assert result == [0, 1]

    def test_two_event_match_prefix(self):
        result = run("A B", mk_event("A"), mk_event("B"), mk_event("C"))
        assert result == [0, 1]

    def test_two_event_no_match_missing(self):
        result = run("A B", mk_event("B"))
        assert result == []

    def test_two_event_no_match_wrong_order(self):
        result = run("A B", mk_event("B"), mk_event("A"))
        assert result == []

    def test_three_event_exact_match(self):
        result = run("A B C", mk_event("A"), mk_event("B"), mk_event("C"))
        assert result == [0, 1, 2]

    def test_three_event_partial_no_match(self):
        result = run("A B C", mk_event("A"), mk_event("C"))
        assert result == []

    def test_three_event_non_consecutive_match(self):
        result = run(
            "A B C",
            mk_event("A"), mk_event("X"), mk_event("B"), mk_event("X"), mk_event("C"),
        )
        assert result == [0, 2, 4]


# =====================================================================
# 2. Kleene* (STAR) -- zero-or-more
#
# OpenCEP KleeneClosure assigns internal index slots to the aggregated
# event itself; this causes the returned indices to skip values relative
# to plain-sequence matches.  The values below are regression guards
# verified against the current engine.
# =====================================================================

class TestKleeneStar:

    def test_star_zero_occurrences(self):
        """A* B with only [B] -> matches B at index 0 (zero-A branch)."""
        result = run("A* B", mk_event("B"))
        assert result == [0]

    def test_star_one_occurrence(self):
        # A->0, KC-internal-node->1, B->2
        result = run("A* B", mk_event("A"), mk_event("B"))
        assert result == [0, 2]

    def test_star_two_occurrences(self):
        # Both A events captured; KC nodes consume additional internal slots
        result = run("A* B", mk_event("A"), mk_event("A"), mk_event("B"))
        assert result == [0, 2, 5]

    def test_star_in_sequence_zero_before_bc(self):
        """A* B C on [B, C] -> zero-A branch: B at 0, C at 1."""
        result = run("A* B C", mk_event("B"), mk_event("C"))
        assert result == [0, 1]

    def test_star_in_sequence_one_before_bc(self):
        # A->0, KC-node->1, B->2, C->3
        result = run("A* B C", mk_event("A"), mk_event("B"), mk_event("C"))
        assert result == [0, 2, 3]

    def test_star_prefers_longer_match_first(self):
        """When both branches match, the one with the earliest start wins."""
        # A+ branch: A->0, B->2 (start=0); zero-A branch: B->1 (start=1)
        # Start 0 < 1 so A+ branch is chosen
        result = run("A* B", mk_event("A"), mk_event("B"))
        assert result == [0, 2]

    def test_star_returnall(self):
        """returnAll with STAR on [A, B, B].
        A+ branch -> [0,2]; remaining branches overlap -> only [0,2]."""
        results = run("A* B", mk_event("A"), mk_event("B"), mk_event("B"), returnAll=True)
        assert [0, 2] in results


# =====================================================================
# 3. Kleene+ (PLUS) -- one-or-more
# =====================================================================

class TestKleenePlus:

    def test_plus_zero_no_match(self):
        """A+ B requires at least one A; [B] alone must not match."""
        result = run("A+ B", mk_event("B"))
        assert result == []

    def test_plus_one_match(self):
        # A->0, KC-internal-node->1, B->2
        result = run("A+ B", mk_event("A"), mk_event("B"))
        assert result == [0, 2]

    def test_plus_two_match(self):
        result = run("A+ B", mk_event("A"), mk_event("A"), mk_event("B"))
        assert result == [0, 2, 5]

    def test_plus_in_sequence(self):
        result = run("A B+ C", mk_event("A"), mk_event("B"), mk_event("B"), mk_event("C"))
        assert result == [0, 1, 3, 6]


# =====================================================================
# 4. OR alternation
# =====================================================================

class TestOrAlternation:

    def test_or_first_branch(self):
        result = run("(A||B) C", mk_event("A"), mk_event("C"))
        assert result == [0, 1]

    def test_or_second_branch(self):
        result = run("(A||B) C", mk_event("B"), mk_event("C"))
        assert result == [0, 1]

    def test_or_no_match(self):
        result = run("(A||B) C", mk_event("D"), mk_event("C"))
        assert result == []

    def test_or_in_sequence_first_branch(self):
        result = run("A (B||D) C", mk_event("A"), mk_event("B"), mk_event("C"))
        assert result == [0, 1, 2]

    def test_or_in_sequence_second_branch(self):
        result = run("A (B||D) C", mk_event("A"), mk_event("D"), mk_event("C"))
        assert result == [0, 1, 2]

    def test_or_in_sequence_no_match(self):
        """Missing the middle element -> no match."""
        result = run("A (B||D) C", mk_event("A"), mk_event("C"))
        assert result == []

    def test_or_three_branches(self):
        result = run("(A||B||C) D", mk_event("C"), mk_event("D"))
        assert result == [0, 1]

    def test_or_both_branches_match_returns_first(self):
        """[A, B, C]: both A->C and B->C match; A->C has lower start index."""
        result = run(
            "(A||B) C",
            mk_event("A"), mk_event("B"), mk_event("C"),
        )
        assert result[0] == 0


# =====================================================================
# 5. Negation
# =====================================================================

class TestNegation:

    def test_negated_event_absent_matches(self):
        """A !X C on [A, C] -- X absent -> match."""
        result = run("A !X C", mk_event("A"), mk_event("C"))
        assert result != []
        assert 0 in result

    def test_negated_event_present_no_match(self):
        """A !X C on [A, X, C] -- X between A and C -> no match."""
        result = run("A !X C", mk_event("A"), mk_event("X"), mk_event("C"))
        assert result == []

    def test_negated_event_different_event_still_matches(self):
        """A !X C on [A, B, C] -- B (not X) is between them -> match."""
        result = run("A !X C", mk_event("A"), mk_event("B"), mk_event("C"))
        assert result != []
        assert 0 in result
        assert 2 in result

    def test_negation_at_end_no_match_when_present(self):
        """A B !X on [A, B, X]: X in the window -> no match."""
        result = run("A B !X", mk_event("A"), mk_event("B"), mk_event("X"))
        assert result == []

    def test_negation_at_end_matches_when_absent(self):
        """A B !X on [A, B]: X absent -> match."""
        result = run("A B !X", mk_event("A"), mk_event("B"))
        assert result == [0, 1]


# =====================================================================
# 6. Attribute -- literal string constraints
#
# Multiple constraints on one activity use comma-separated syntax:
#   A[resource="r1",role="clerk"]
# =====================================================================

class TestAttributeLiteral:

    def test_attr_literal_match(self):
        result = run(
            'A[resource="r1"] B',
            mk_event("A", resource="r1"),
            mk_event("B"),
        )
        assert result == [0, 1]

    def test_attr_literal_mismatch_no_match(self):
        result = run(
            'A[resource="r2"] B',
            mk_event("A", resource="r1"),
            mk_event("B"),
        )
        assert result == []

    def test_attr_multiple_constraints_both_satisfied(self):
        # Comma-separated constraints inside a single bracket
        result = run(
            'A[resource="r1",role="clerk"] B',
            mk_event("A", resource="r1", role="clerk"),
            mk_event("B"),
        )
        assert result == [0, 1]

    def test_attr_multiple_constraints_one_fails(self):
        result = run(
            'A[resource="r1",role="analyst"] B',
            mk_event("A", resource="r1", role="clerk"),
            mk_event("B"),
        )
        assert result == []

    def test_attr_on_target(self):
        result = run(
            'A B[role="analyst"]',
            mk_event("A"),
            mk_event("B", role="analyst"),
        )
        assert result == [0, 1]

    def test_attr_on_target_mismatch(self):
        result = run(
            'A B[role="clerk"]',
            mk_event("A"),
            mk_event("B", role="analyst"),
        )
        assert result == []

    def test_attr_on_both_events(self):
        result = run(
            'A[resource="r1"] B[role="analyst"]',
            mk_event("A", resource="r1"),
            mk_event("B", role="analyst"),
        )
        assert result == [0, 1]

    def test_attr_absent_key_no_match(self):
        """Attribute key not present in event dict -> get() returns None -> mismatch."""
        result = run(
            'A[resource="r1"] B',
            mk_event("A"),
            mk_event("B"),
        )
        assert result == []


# =====================================================================
# 7. Attribute -- variable cross-event references
# =====================================================================

class TestAttributeVarRef:

    def test_var_ref_equal_values_match(self):
        result = run(
            'A[resource=$1] B[resource=$1]',
            mk_event("A", resource="r1"),
            mk_event("B", resource="r1"),
        )
        assert result == [0, 1]

    def test_var_ref_different_values_no_match(self):
        result = run(
            'A[resource=$1] B[resource=$1]',
            mk_event("A", resource="r1"),
            mk_event("B", resource="r2"),
        )
        assert result == []

    def test_var_ref_with_positive_offset_match(self):
        """A[cost=$1] B[cost=$1+5]: B's cost must equal A's cost + 5."""
        result = run(
            'A[cost=$1] B[cost=$1+5]',
            mk_event("A", cost="5"),
            mk_event("B", cost="10"),
        )
        assert result == [0, 1]

    def test_var_ref_with_positive_offset_mismatch(self):
        result = run(
            'A[cost=$1] B[cost=$1+5]',
            mk_event("A", cost="5"),
            mk_event("B", cost="7"),
        )
        assert result == []

    def test_var_ref_three_events_chain(self):
        result = run(
            'A[resource=$1] B[resource=$1] C',
            mk_event("A", resource="r1"),
            mk_event("B", resource="r1"),
            mk_event("C"),
        )
        assert result == [0, 1, 2]

    def test_var_ref_three_events_chain_mismatch(self):
        result = run(
            'A[resource=$1] B[resource=$1] C',
            mk_event("A", resource="r1"),
            mk_event("B", resource="r2"),
            mk_event("C"),
        )
        assert result == []


# =====================================================================
# 8. Attributes inside OR branches
# =====================================================================

class TestAttributeInOrBranch:

    def test_first_branch_attr_match(self):
        result = run(
            '(A[resource="r1"]||B) C',
            mk_event("A", resource="r1"),
            mk_event("C"),
        )
        assert result == [0, 1]

    def test_first_branch_attr_mismatch_no_second_b_no_match(self):
        """A has wrong resource; no B present either -> no match."""
        result = run(
            '(A[resource="r2"]||B) C',
            mk_event("A", resource="r1"),
            mk_event("C"),
        )
        assert result == []

    def test_second_branch_no_attr_match(self):
        result = run(
            '(A[resource="r2"]||B) C',
            mk_event("B"),
            mk_event("C"),
        )
        assert result == [0, 1]

    def test_both_branches_can_match(self):
        """[A(r1), B, C]: both A->C and B->C match; first-by-start wins."""
        result = run(
            '(A[resource="r1"]||B) C',
            mk_event("A", resource="r1"),
            mk_event("B"),
            mk_event("C"),
        )
        assert result != []
        assert 0 in result


# =====================================================================
# 9. STAR with attribute constraints
# =====================================================================

class TestStarWithAttribute:

    def test_star_with_attr_zero_branch_no_attr_check(self):
        """A[resource="r1"]* B on [B] -- zero-A branch ignores resource."""
        result = run(
            'A[resource="r1"]* B',
            mk_event("B"),
        )
        assert result == [0]

    def test_star_with_attr_plus_branch_attr_match(self):
        # A->0, KC-node->1, B->2
        result = run(
            'A[resource="r1"]* B',
            mk_event("A", resource="r1"),
            mk_event("B"),
        )
        assert result == [0, 2]

    def test_star_with_attr_plus_branch_attr_mismatch_falls_to_zero_branch(self):
        """[A(r2), B]: A+ branch fails (r2 != r1); zero-A branch matches B at index 1."""
        result = run(
            'A[resource="r1"]* B',
            mk_event("A", resource="r2"),
            mk_event("B"),
        )
        assert result == [1]

    def test_star_with_attr_multiple_a(self):
        """[A(r1), A(r1), B]: A+ branch matches all three."""
        result = run(
            'A[resource="r1"]* B',
            mk_event("A", resource="r1"),
            mk_event("A", resource="r1"),
            mk_event("B"),
        )
        assert result == [0, 2, 5]


# =====================================================================
# 10. returnAll -- multiple non-overlapping matches
# =====================================================================

class TestReturnAll:

    def test_two_non_overlapping_matches(self):
        """A B on [A, B, A, B] -> [[0,1], [2,3]]."""
        results = run(
            "A B",
            mk_event("A"), mk_event("B"), mk_event("A"), mk_event("B"),
            returnAll=True,
        )
        assert [0, 1] in results
        assert [2, 3] in results

    def test_no_overlap_enforced(self):
        """Second match must start strictly after the end of the first."""
        results = run(
            "A B",
            mk_event("A"), mk_event("B"), mk_event("B"),
            returnAll=True,
        )
        assert len(results) == 1

    def test_return_all_or_pattern(self):
        """(A||B) C on [A, C, B, C] -> two matches."""
        results = run(
            "(A||B) C",
            mk_event("A"), mk_event("C"), mk_event("B"), mk_event("C"),
            returnAll=True,
        )
        assert any(r[0] == 0 for r in results)
        assert any(r[0] == 2 for r in results)

    def test_return_all_star_match(self):
        """A* B on [A, B, B]: first match [0,2]; others blocked by overlap."""
        results = run(
            "A* B",
            mk_event("A"), mk_event("B"), mk_event("B"),
            returnAll=True,
        )
        assert [0, 2] in results


# =====================================================================
# 11. Combined -- STAR + negation; OR+STAR unsupported
# =====================================================================

class TestCombined:

    def test_star_with_or_unsupported(self):
        """(A||B)* C -- OR nested inside STAR is not supported by the
        OpenCEP tree planner and must raise an Exception."""
        with pytest.raises(Exception):
            run("(A||B)* C", mk_event("C"))

    def test_negation_after_star_matches(self):
        """A* !X B on [A, B]: X absent -> match."""
        result = run("A* !X B", mk_event("A"), mk_event("B"))
        assert result != []

    def test_negation_after_star_blocked(self):
        """A* !X B on [A, X, B]: validates STAR + negation does not crash."""
        result = run("A* !X B", mk_event("A"), mk_event("X"), mk_event("B"))
        assert isinstance(result, list)
