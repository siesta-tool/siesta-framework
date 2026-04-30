"""
Integration tests for process_detection_query_local().

Requires MinIO at localhost:9000.  The ``query_preprocessed`` fixture
(defined in conftest.py) preprocesses datasets/test.csv once per session
before these tests run.

Dataset (test.csv)
------------------
  trace_1 : A(resource=r1,role=clerk)    -> B(resource=r2,role=analyst)
                                         -> C(resource=r3,role=system)
  trace_2 : A(resource=r1,role=clerk)    -> C(resource=r3,role=system)
  trace_3 : B(resource=r2,role=analyst)  -> C(resource=r3,role=system)
                                         -> D(resource=r4,role=manager)
  trace_4 : A(resource=r1,role=clerk)    -> D(resource=r4,role=manager)

Helper
------
  _query(pattern, metadata) -> str
      Runs process_detection_query_local and returns the result string.

  _total(result_str) -> int
      Parses the "Total: N" prefix from the result string.

  _trace_ids_in_result(result_str) -> set[str]
      Returns every "trace_N" token found anywhere in the result string.
"""
import re
import socket
import pytest

from siesta.modules.query.query_types.query_processors_detection import (
    process_detection_query_local,
)


# ── helpers ──────────────────────────────────────────────────────────

def _query(pattern: str, metadata) -> str:
    config = {"query": {"alt_pattern": pattern}}
    result = process_detection_query_local(config, metadata)
    assert result is not None, "process_detection_query_local returned None"
    return result


def _total(result: str) -> int:
    m = re.match(r"Total:\s*(\d+)", result)
    assert m is not None, f"Could not parse 'Total:' from: {result!r}"
    return int(m.group(1))


def _trace_ids(result: str) -> set:
    return set(re.findall(r"trace_\d+", result))


# ── skip guard ───────────────────────────────────────────────────────

def _minio_reachable(host="localhost", port=9000, timeout=2):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, ConnectionRefusedError):
        return False


pytestmark = pytest.mark.skipif(
    not _minio_reachable(),
    reason="MinIO is not running at localhost:9000",
)


# ══════════════════════════════════════════════════════════════════════
# Basic sequence patterns
# ══════════════════════════════════════════════════════════════════════

class TestSimpleSequences:

    def test_ab_matches_only_trace1(self, query_preprocessed):
        """A B: only trace_1 has A immediately followed by B."""
        md = query_preprocessed["metadata"]
        result = _query("A B", md)
        assert _total(result) == 1
        assert "trace_1" in _trace_ids(result)

    def test_ac_matches_trace1_and_trace2(self, query_preprocessed):
        """A C: trace_1 (A->B->C) and trace_2 (A->C) both have A before C."""
        md = query_preprocessed["metadata"]
        result = _query("A C", md)
        assert _total(result) == 2
        assert {"trace_1", "trace_2"} == _trace_ids(result)

    def test_ad_matches_only_trace4(self, query_preprocessed):
        """A D: only trace_4 has both A and D (A before D)."""
        md = query_preprocessed["metadata"]
        result = _query("A D", md)
        assert _total(result) == 1
        assert "trace_4" in _trace_ids(result)

    def test_bd_matches_only_trace3(self, query_preprocessed):
        """B D: only trace_3 has B before D."""
        md = query_preprocessed["metadata"]
        result = _query("B D", md)
        assert _total(result) == 1
        assert "trace_3" in _trace_ids(result)

    def test_xe_no_match(self, query_preprocessed):
        """X E: neither X nor E appear -> no match."""
        md = query_preprocessed["metadata"]
        result = _query("X E", md)
        assert _total(result) == 0


# ══════════════════════════════════════════════════════════════════════
# OR branches
# ══════════════════════════════════════════════════════════════════════

class TestOrBranches:

    def test_or_ab_c_three_matches(self, query_preprocessed):
        """(A||B) C: trace_1 (A->C), trace_2 (A->C), trace_3 (B->C)."""
        md = query_preprocessed["metadata"]
        result = _query("(A||B) C", md)
        assert _total(result) == 3
        assert {"trace_1", "trace_2", "trace_3"} == _trace_ids(result)

    def test_or_ac_d_two_matches(self, query_preprocessed):
        """(A||C) D: trace_3 (C->D) and trace_4 (A->D)."""
        md = query_preprocessed["metadata"]
        result = _query("(A||C) D", md)
        assert _total(result) == 2
        assert {"trace_3", "trace_4"} == _trace_ids(result)

    def test_or_no_match(self, query_preprocessed):
        """(X||Y) C: neither X nor Y in dataset -> no match."""
        md = query_preprocessed["metadata"]
        result = _query("(X||Y) C", md)
        assert _total(result) == 0


# ══════════════════════════════════════════════════════════════════════
# Kleene+ (PLUS)
# ══════════════════════════════════════════════════════════════════════

class TestKleenePlus:

    def test_plus_bc_matches_trace1_and_trace3(self, query_preprocessed):
        """B+ C: one B before C -> trace_1 (B at pos 1, C at pos 2) and
        trace_3 (B at pos 0, C at pos 1)."""
        md = query_preprocessed["metadata"]
        result = _query("B+ C", md)
        assert _total(result) == 2
        assert {"trace_1", "trace_3"} == _trace_ids(result)

    def test_plus_requires_at_least_one(self, query_preprocessed):
        """A+ C: at least one A before C.
        trace_1 and trace_2 have A before C; trace_3 and trace_4 do not.
        Same expected result as plain 'A C'."""
        md = query_preprocessed["metadata"]
        result = _query("A+ C", md)
        assert _total(result) == 2
        assert {"trace_1", "trace_2"} == _trace_ids(result)


# ══════════════════════════════════════════════════════════════════════
# Kleene* (STAR)
# ══════════════════════════════════════════════════════════════════════

class TestKleeneStar:

    def test_star_ab_matches_trace1(self, query_preprocessed):
        """A* B: the pairs index has (A,B) only for trace_1; the zero-A
        branch requires a B-self-pair which doesn't exist for other traces.
        Effective result: just trace_1 (via the A+ branch)."""
        md = query_preprocessed["metadata"]
        result = _query("A* B", md)
        assert _total(result) >= 1
        assert "trace_1" in _trace_ids(result)

    def test_star_ac_contains_trace1_and_trace2(self, query_preprocessed):
        """A* C: the A+ branch fetches traces with (A,C) pairs.
        trace_1 and trace_2 qualify; trace_3 has C but no A in its pairs."""
        md = query_preprocessed["metadata"]
        result = _query("A* C", md)
        assert _total(result) >= 2
        ids = _trace_ids(result)
        assert "trace_1" in ids
        assert "trace_2" in ids


# ══════════════════════════════════════════════════════════════════════
# Attribute: literal string constraints
# ══════════════════════════════════════════════════════════════════════

class TestAttributeLiteral:

    def test_attr_ab_resource_r1_matches_trace1(self, query_preprocessed):
        """A[resource="r1"] B: A in trace_1 has resource=r1."""
        md = query_preprocessed["metadata"]
        result = _query('A[resource="r1"] B', md)
        assert _total(result) == 1
        assert "trace_1" in _trace_ids(result)

    def test_attr_ac_resource_r1_matches_trace1_and_trace2(self, query_preprocessed):
        """A[resource="r1"] C: both trace_1 and trace_2 have A(r1) before C."""
        md = query_preprocessed["metadata"]
        result = _query('A[resource="r1"] C', md)
        assert _total(result) == 2
        assert {"trace_1", "trace_2"} == _trace_ids(result)

    def test_attr_ac_resource_r2_no_match(self, query_preprocessed):
        """A[resource="r2"] C: no A event has resource=r2 (A always has r1)."""
        md = query_preprocessed["metadata"]
        result = _query('A[resource="r2"] C', md)
        assert _total(result) == 0

    def test_attr_ab_role_clerk_matches_trace1(self, query_preprocessed):
        """A[role="clerk"] B: role=clerk is correct for A."""
        md = query_preprocessed["metadata"]
        result = _query('A[role="clerk"] B', md)
        assert _total(result) == 1
        assert "trace_1" in _trace_ids(result)

    def test_attr_wrong_role_no_match(self, query_preprocessed):
        """A[role="manager"] B: no A has role=manager."""
        md = query_preprocessed["metadata"]
        result = _query('A[role="manager"] B', md)
        assert _total(result) == 0


# ══════════════════════════════════════════════════════════════════════
# Attribute: literal constraints inside OR branches
# ══════════════════════════════════════════════════════════════════════

class TestAttributeInOrBranch:

    def test_or_with_attrs_three_traces(self, query_preprocessed):
        """(A[resource="r1"]||B[resource="r2"]) C:
        Branch 0: A(r1) before C  -> trace_1, trace_2
        Branch 1: B(r2) before C  -> trace_1, trace_3
        Union: trace_1, trace_2, trace_3."""
        md = query_preprocessed["metadata"]
        result = _query('(A[resource="r1"]||B[resource="r2"]) C', md)
        assert _total(result) == 3
        assert {"trace_1", "trace_2", "trace_3"} == _trace_ids(result)

    def test_or_with_matching_and_failing_attrs(self, query_preprocessed):
        """(A[resource="r2"]||B[resource="r2"]) C:
        A never has r2; B has r2 -> only trace_1 (B->C) and trace_3 (B->C)."""
        md = query_preprocessed["metadata"]
        result = _query('(A[resource="r2"]||B[resource="r2"]) C', md)
        assert _total(result) == 2
        assert {"trace_1", "trace_3"} == _trace_ids(result)

    def test_or_all_attr_fail_no_match(self, query_preprocessed):
        """(A[resource="r9"]||B[resource="r9"]) C: no event has resource=r9."""
        md = query_preprocessed["metadata"]
        result = _query('(A[resource="r9"]||B[resource="r9"]) C', md)
        assert _total(result) == 0
