"""
Pure-Python tests for the pm4py DFG-only process model pipeline.

No Spark, no MinIO - constructs a pm4py DFG by hand (bypassing
process_model._compute_dfg_dict) and asserts that discovering a process tree
purely from the DFG (variant=Variants.IMd), then converting to a Petri net and to
BPMN, produces valid, non-empty structures. This isolates pm4py-API-correctness
testing from Spark/S3 infrastructure, so it also catches pm4py-version-upgrade
breakage independently of everything else.
"""
from pm4py.objects.dfg.obj import DFG
from pm4py.objects.conversion.process_tree import converter as pt_converter

from siesta.modules.analyser.process_model import _discover_process_tree


DFG_GRAPH = {("A", "B"): 5, ("B", "C"): 3, ("A", "C"): 2}
START_ACTS = {"A": 7}
END_ACTS = {"C": 5}


def _leaf_labels(node) -> set[str]:
    if not node.children:
        return {node.label} if node.label else set()
    labels: set[str] = set()
    for child in node.children:
        labels |= _leaf_labels(child)
    return labels


def test_discover_process_tree_from_dfg_only():
    tree = _discover_process_tree(DFG_GRAPH, START_ACTS, END_ACTS)
    assert tree is not None
    assert _leaf_labels(tree) == {"A", "B", "C"}


def test_process_tree_to_petri_net():
    tree = _discover_process_tree(DFG_GRAPH, START_ACTS, END_ACTS)
    net, im, fm = pt_converter.apply(tree, variant=pt_converter.Variants.TO_PETRI_NET)

    assert len(net.places) > 0
    assert len(net.transitions) > 0
    assert len(net.arcs) > 0
    assert len(im) > 0
    assert len(fm) > 0

    labels = {t.label for t in net.transitions if t.label is not None}
    assert labels == {"A", "B", "C"}

    # Markings must reference actual places of this net (not just look right when printed).
    assert all(place in net.places for place in im)
    assert all(place in net.places for place in fm)


def test_process_tree_to_bpmn():
    tree = _discover_process_tree(DFG_GRAPH, START_ACTS, END_ACTS)
    bpmn_model = pt_converter.apply(tree, variant=pt_converter.Variants.TO_BPMN)

    nodes = list(bpmn_model.get_nodes())
    flows = list(bpmn_model.get_flows())
    assert len(nodes) > 0
    assert len(flows) > 0

    task_names = {
        node.get_name() for node in nodes
        if type(node).__name__ == "Task"
    }
    assert task_names == {"A", "B", "C"}


def test_dfg_object_accepts_our_aggregated_shape():
    """Sanity check that our own dict shape (as produced by _compute_dfg_dict) is
    exactly what pm4py's DFG constructor expects - dict[(source, target)] -> count.
    """
    dfg_obj = DFG(graph=DFG_GRAPH, start_activities=START_ACTS, end_activities=END_ACTS)
    assert dict(dfg_obj.graph) == DFG_GRAPH
    assert dict(dfg_obj.start_activities) == START_ACTS
    assert dict(dfg_obj.end_activities) == END_ACTS
