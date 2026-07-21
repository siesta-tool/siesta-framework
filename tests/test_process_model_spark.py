"""
End-to-end smoke test of discover_process_model against the query_preprocessed
fixture's small log, for all three algos. Verifies our own scalable Spark DFG
aggregation (_compute_dfg_dict) feeds correctly into pm4py's IMd-based Inductive
Miner and produces real output files - graphviz/pyvis paths may legitimately
fall back to matplotlib/SVG if those aren't installed, so this checks file
existence and non-zero size, not exact rendering.
"""
import os

import pytest

from siesta.modules.analyser.process_model import discover_process_model, _compute_dfg_dict


class TestComputeDfgDict:

    def test_matches_known_pairs_and_start_end_activities(self, query_preprocessed):
        storage = query_preprocessed["storage"]
        metadata = storage.read_metadata_table(query_preprocessed["metadata"])
        events_df = storage.read_sequence_table(metadata)

        dfg, start_acts, end_acts = _compute_dfg_dict(events_df)

        assert set(dfg.keys()) == {("A", "B"), ("B", "C"), ("A", "C"), ("C", "D"), ("A", "D")}
        assert dfg[("B", "C")] == 2  # trace_1 and trace_3
        assert dfg[("A", "B")] == 1  # trace_1 only

        # Start activities: A (trace_1, trace_2, trace_4), B (trace_3).
        assert start_acts.get("A") == 3
        assert start_acts.get("B") == 1
        # End activities: C (trace_1, trace_2), D (trace_3, trace_4).
        assert end_acts.get("C") == 2
        assert end_acts.get("D") == 2


class TestDiscoverProcessModelEndToEnd:

    @pytest.mark.parametrize("algo,ext", [("dfg", "xml"), ("bpmn", "bpmn"), ("petri_net", "pnml")])
    def test_produces_model_and_visualization_files(self, query_preprocessed, algo, ext):
        storage = query_preprocessed["storage"]
        metadata = storage.read_metadata_table(query_preprocessed["metadata"])
        events_df = storage.read_sequence_table(metadata)

        model_path, fmt, png_path, html_path = discover_process_model(events_df, algo=algo)

        assert fmt == ext
        for path in (model_path, png_path, html_path):
            assert os.path.exists(path)
            assert os.path.getsize(path) > 0
