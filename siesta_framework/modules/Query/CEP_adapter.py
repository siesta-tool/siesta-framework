import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List


OPENCEP_ROOT = Path(__file__).resolve().parent / "OpenCEP"
if str(OPENCEP_ROOT) not in sys.path:
    sys.path.append(str(OPENCEP_ROOT))

from CEP import CEP
from base.DataFormatter import DataFormatter, EventTypeClassifier
from base.Event import AggregatedEvent, Event
from base.Pattern import Pattern
from base.PatternStructure import (
    KleeneClosureOperator,
    NegationOperator,
    OrOperator,
    PatternStructure,
    PrimitiveEventStructure,
    SeqOperator,
)
from condition.CompositeCondition import AndCondition
from condition.Condition import BinaryCondition, SimpleCondition, TrueCondition, Variable
from condition.KCCondition import KCValueCondition
from stream.Stream import InputStream, OutputStream
from transformation.PatternPreprocessingParameters import PatternPreprocessingParameters
from transformation.PatternTransformationRules import PatternTransformationRules

from siesta_framework.modules.Query.parse_seql import Quantifier

from siesta_framework.modules.Query.parse_seql import (
    ActivityNode,
    ElementNode,
    NegatedNode,
    OrNode,
    parse_pattern,
    Quantifier,
    SeqNode,
)

# from CEP_adapter_helpers import (
#     _extract_split_matches,
#     _check_gap_constraints
# )

def _get_accepted_labels(atom) -> set:
    """
    Collect all activity labels that *atom* can directly match.
    Used to decide which indices belong to which Kleene group.
    """
    if isinstance(atom, ActivityNode):
        return {atom.label}
    if isinstance(atom, OrNode):
        labels = set()
        for branch in atom.branches:
            labels |= _get_accepted_labels(branch)
        return labels
    if isinstance(atom, SeqNode):
        # For a grouped sequence used as a Kleene body, e.g. (a b)+,
        # every positive label inside is a candidate.
        labels = set()
        for elem in atom.elements:
            if not isinstance(elem.atom, NegatedNode):
                labels |= _get_accepted_labels(elem.atom)
        return labels
    if isinstance(atom, ElementNode):
        return _get_accepted_labels(atom.atom)
    if isinstance(atom, NegatedNode):
        return set()
    return set()


def _min_events_for_element(elem: ElementNode) -> int:
    """
    Minimum number of events *elem* must consume in any valid match.
    ONE and PLUS require at least 1; STAR and OPT allow 0.
    Used to reserve slots for downstream elements when Kleene is greedy.
    """
    return 1 if elem.quantifier in (Quantifier.ONE, Quantifier.PLUS) else 0


def _extract_split_matches_dsl(
    ast,
    sequence: List[str],
    idxs: List[int],
) -> List[List[int]]:
    """
    Assign matched event indices back to their originating pattern groups.

    Parameters
    ----------
    ast      : DSL AST root (from ``parse_pattern``).
    sequence : full label sequence of the trace.
    idxs     : sorted event indices belonging to this match (from OpenCEP).

    Returns
    -------
    List[List[int]]
        One sublist per top-level positive element, parallel to
        ``_DSLPatternBuilder.top_level_parts``:

        =========  =================================
        ONE        always one index  → ``[idx]``
        OPT        zero or one index → ``[]`` / ``[idx]``
        PLUS/STAR  zero or more      → ``[idx, idx, ...]``
        =========  =================================

    Algorithm
    ---------
    Walk the positive elements left-to-right, assigning indices greedily.
    For Kleene elements (PLUS/STAR) a *cap* prevents over-consumption:
    we always leave enough indices to satisfy the minimum requirements of
    every element that comes after the current one.  This handles patterns
    like ``a+ a b`` correctly without backtracking.

    We trust OpenCEP's guarantee that *idxs* is a valid match, so for ONE
    elements we consume unconditionally rather than re-checking labels.
    """
    # ── Normalise to a list of positive top-level elements ───────────────
    if isinstance(ast, SeqNode):
        positive_elements = [
            elem for elem in ast.elements
            if not isinstance(elem.atom, NegatedNode)
        ]
    else:
        # Single root (ActivityNode, OrNode, …) — one group owns all indices
        return [list(idxs)]

    if not positive_elements:
        return []

    matched_labels = [sequence[i] for i in idxs]
    groups: List[List[int]] = []
    cursor = 0  # next unassigned position in idxs

    for pos, elem in enumerate(positive_elements):
        accepted = _get_accepted_labels(elem.atom)

        # How many indices must be kept for all elements that follow?
        reserved = sum(
            _min_events_for_element(future)
            for future in positive_elements[pos + 1:]
        )
        # The maximum number of indices this element is allowed to consume
        cap = len(idxs) - cursor - reserved

        if elem.quantifier == Quantifier.ONE:
            # Exactly one — consume unconditionally (OpenCEP already validated)
            groups.append([idxs[cursor]] if cursor < len(idxs) else [])
            cursor += 1

        elif elem.quantifier == Quantifier.OPT:
            # Zero or one — only consume if the label matches and budget allows
            if (
                cap > 0
                and cursor < len(idxs)
                and matched_labels[cursor] in accepted
            ):
                groups.append([idxs[cursor]])
                cursor += 1
            else:
                groups.append([])

        else:
            # PLUS or STAR — greedy within the cap
            group: List[int] = []
            limit = cursor + max(0, cap)
            while (
                cursor < limit
                and cursor < len(idxs)
                and matched_labels[cursor] in accepted
            ):
                group.append(idxs[cursor])
                cursor += 1
            groups.append(group)

    return groups


# ═══════════════════════════════════════════════════════════════════════
# DSL → OpenCEP pattern builder
# ═══════════════════════════════════════════════════════════════════════

class _DSLPatternBuilder:
    """
    Converts a responded_pairs_dsl AST into an OpenCEP PatternStructure.

    After calling ``build_root``, ``top_level_parts`` holds the
    non-negated top-level structures in left-to-right order; these are
    used by ``_attach_native_conditions`` to wire up constraints.
    """

    def __init__(self):
        self._counter = 0
        self.top_level_parts: list = []

    # ── helpers ──────────────────────────────────────────────────────────

    def _next_name(self) -> str:
        name = f"e{self._counter}"
        self._counter += 1
        return name

    def _apply_quantifier(self, structure, quantifier: Quantifier):
        """Wrap *structure* in the appropriate Kleene closure (or not)."""
        if quantifier == Quantifier.ONE:
            return structure
        if quantifier == Quantifier.PLUS:
            return KleeneClosureOperator(structure, min_size=1)
        if quantifier == Quantifier.STAR:
            return KleeneClosureOperator(structure, min_size=0)
        if quantifier == Quantifier.OPT:
            # OpenCEP: min=0, max=1 ≈ optional
            return KleeneClosureOperator(structure, min_size=0, max_size=1)
        raise ValueError(f"Unknown quantifier: {quantifier!r}")

    # ── recursive build ──────────────────────────────────────────────────

    def build_element(self, elem: ElementNode):
        """Build one ElementNode, applying its quantifier to the inner atom."""
        inner = self.build(elem.atom)
        return self._apply_quantifier(inner, elem.quantifier)

    def build(self, node):
        """Convert any DSL AST node to an OpenCEP PatternStructure."""

        if isinstance(node, ActivityNode):
            return PrimitiveEventStructure(node.label, self._next_name())

        if isinstance(node, NegatedNode):
            # The inner node may be an ActivityNode or an OrNode whose
            # alternatives are all forbidden (union, not branch).
            return NegationOperator(self.build(node.inner))

        if isinstance(node, SeqNode):
            parts = [self.build_element(elem) for elem in node.elements]
            # SeqOperator requires at least two args; unwrap singletons.
            return SeqOperator(*parts) if len(parts) > 1 else parts[0]

        if isinstance(node, OrNode):
            # OrOperator is binary — chain left-to-right for >2 branches.
            branches = [self.build(branch) for branch in node.branches]
            result = branches[0]
            for branch in branches[1:]:
                result = OrOperator(result, branch)
            return result

        if isinstance(node, ElementNode):
            # Reached when an ElementNode appears outside a SeqNode context.
            return self.build_element(node)

        raise TypeError(f"Unsupported DSL node type: {type(node).__name__}")

    # ── root entry-point ─────────────────────────────────────────────────

    def build_root(self, node):
        """
        Build the root pattern and populate ``top_level_parts``.

        ``top_level_parts`` mirrors what the original ``_PatternBuilder``
        produced: the non-negated, top-level structures of a SeqNode,
        used later to attach native OpenCEP conditions.
        """
        self.top_level_parts = []

        if isinstance(node, SeqNode):
            parts = []
            for elem in node.elements:
                built = self.build_element(elem)
                parts.append(built)
                if not isinstance(elem.atom, NegatedNode):
                    self.top_level_parts.append(built)
            return SeqOperator(*parts) if len(parts) > 1 else parts[0]

        # Single-element root (ActivityNode, OrNode, …)
        built = self.build(node)
        if not isinstance(node, NegatedNode):
            self.top_level_parts = [built]
        return built


# ── DSL-aware _has_alt ───────────────────────────────────────────────────

def _dsl_has_alt(node) -> bool:
    """True iff the DSL AST contains any OR branch anywhere."""
    if isinstance(node, OrNode):
        return True
    if isinstance(node, SeqNode):
        return any(_dsl_has_alt(elem) for elem in node.elements)
    if isinstance(node, ElementNode):
        return _dsl_has_alt(node.atom)
    if isinstance(node, NegatedNode):
        return _dsl_has_alt(node.inner)
    return False


# ── public translation helper ────────────────────────────────────────────

def translate_dsl_pattern_to_opencep(pattern_str: str):
    """
    Parse *pattern_str* with the DSL parser and produce an OpenCEP Pattern.

    Returns
    -------
    (ast, pattern, top_level_parts)
        Mirrors the return value of ``translate_query_to_pattern`` so the
        rest of the pipeline is identical.
    """
    ast = parse_pattern(pattern_str)
    builder = _DSLPatternBuilder()
    pattern = Pattern(
        builder.build_root(ast),
        TrueCondition(),
        timedelta(days=3650),
    )
    return ast, pattern, builder.top_level_parts


# ═══════════════════════════════════════════════════════════════════════
# Main entry-point using the DSL parser
# ═══════════════════════════════════════════════════════════════════════

def find_occurrences_dsl(
    sequence: List[str],
    pattern_str: str,
    returnAll: bool = False,
    returnSplit: bool = False,
    constraints=None,
    events=None,
):
    """
    Drop-in replacement for ``find_occurrences_opencep`` that accepts
    patterns written in the responded_pairs_dsl grammar instead of
    the parse_seql grammar.

    Parameters
    ----------
    sequence    : list of activity label strings, one per event in order.
    pattern_str : DSL pattern, e.g. ``'a b+ !c d'`` or ``'a (b||c) d'``.
    returnAll   : if True, return every non-overlapping match.
    returnSplit : if True, each result is ``(idxs, split_matches)``.
    constraints : list of TimeConstraint / EventValueConstraint / … objects.
    events      : list of raw event dicts (must align with *sequence*).
    """
    ast, pattern, top_level_parts = translate_dsl_pattern_to_opencep(pattern_str)

    constraint_events = _normalize_events(sequence, events)
    engine_events     = _build_engine_events(constraint_events)

    residual_constraints = _attach_native_conditions(
        pattern,
        top_level_parts,
        constraints or [],
    )

    preprocessing_params = (
        _build_preprocessing_params_for_dsl(ast)
        if _dsl_has_alt(ast) else None
    )

    output_items = run_opencep_pattern(pattern, engine_events, preprocessing_params)

    candidate_matches = []
    for match in output_items:
        idxs = sorted(
            event.payload[Event.INDEX_ATTRIBUTE_NAME]
            for event in _flatten_match_events(match.events)
        )
        split_matches = _extract_split_matches_dsl(ast, sequence, idxs)
        if not _check_post_filters(
            residual_constraints, idxs, constraint_events, split_matches
        ):
            continue
        candidate_matches.append(idxs)

    filtered_matches = _dedupe_and_sort(candidate_matches)

    if not filtered_matches:
        return _empty_result(returnSplit) if not returnAll else []

    if not returnAll:
        idxs = filtered_matches[0]
        return _pack_result(
            idxs, _extract_split_matches_dsl(ast, sequence, idxs), returnSplit
        )

    results, last_end = [], -1
    for idxs in filtered_matches:
        if idxs[0] <= last_end:
            continue
        results.append(
            _pack_result(
                idxs, _extract_split_matches_dsl(ast, sequence, idxs), returnSplit
            )
        )
        last_end = idxs[-1]
    return results


def _build_preprocessing_params_for_dsl(ast):
    return PatternPreprocessingParameters([
        PatternTransformationRules.TOPMOST_OR_PATTERN,
        PatternTransformationRules.INNER_OR_PATTERN,
    ])


class _OpenCEPEventTypeClassifier(EventTypeClassifier):
    def get_event_type(self, event_payload):
        return event_payload["name"]


class _OpenCEPDataFormatter(DataFormatter):
    def __init__(self):
        super().__init__(_OpenCEPEventTypeClassifier())

    def parse_event(self, raw_data):
        return dict(raw_data)

    def get_event_timestamp(self, event_payload):
        return event_payload["ts"]


class _MemoryInputStream(InputStream):
    def __init__(self, items):
        super().__init__()
        for item in items:
            self._stream.put(item)
        self.close()

    def add_item(self, item):
        self._stream.put(item)


class _MemoryOutputStream(OutputStream):
    def __init__(self):
        super().__init__()
        self.items = []

    def add_item(self, item):
        self.items.append(item)
        self._stream.put(item)


class _PatternBuilder:
    def __init__(self):
        self._counter = 0
        self.top_level_parts = []

    def _next_name(self):
        name = f"e{self._counter}"
        self._counter += 1
        return name

    def build(self, node):
        if isinstance(node, Lit):
            return PrimitiveEventStructure(node.v, self._next_name())
        if isinstance(node, Concat):
            return SeqOperator(*(self.build(part) for part in node.parts))
        if isinstance(node, Alt):
            return OrOperator(self.build(node.left), self.build(node.right))
        if isinstance(node, Plus):
            return KleeneClosureOperator(self.build(node.node), min_size=1)
        if isinstance(node, Not):
            return NegationOperator(self.build(node.node))
        if isinstance(node, Star):
            raise NotImplementedError("OpenCEP adapter does not support '*' expressions")
        raise TypeError(f"Unsupported AST node: {type(node).__name__}")

    def build_root(self, node):
        if isinstance(node, Concat):
            args = []
            self.top_level_parts = []
            for part in node.parts:
                built = self.build(part)
                args.append(built)
                if not isinstance(part, Not):
                    self.top_level_parts.append(built)
            return SeqOperator(*args)
        built = self.build(node)
        self.top_level_parts = [] if isinstance(node, Not) else [built]
        return built


def _has_alt(node):
    if isinstance(node, Alt):
        return True
    if isinstance(node, Concat):
        return any(_has_alt(part) for part in node.parts)
    if isinstance(node, (Plus, Star, Not)):
        return _has_alt(node.node)
    return False

    
def _normalize_events(sequence: List[str], events=None):
    base_ts = datetime(2024, 1, 1)
    normalized = []

    if events is None:
        events = [{"name": name} for name in sequence]
    if len(sequence) != len(events):
        raise ValueError("sequence and events must have the same length")

    for i, (name, event) in enumerate(zip(sequence, events)):
        current = dict(event)
        current.setdefault("name", name)
        if current["name"] != name:
            raise ValueError("sequence and events names do not match")
        normalized.append(current)
    return normalized


def _coerce_engine_timestamp(timestamp, fallback_index: int):
    if timestamp is None:
        return datetime(2024, 1, 1) + timedelta(seconds=fallback_index)
    if isinstance(timestamp, datetime):
        return timestamp
    if isinstance(timestamp, (int, float)):
        return datetime(2024, 1, 1) + timedelta(seconds=float(timestamp))
    raise TypeError(f"Unsupported timestamp type for OpenCEP adapter: {type(timestamp).__name__}")


def _build_engine_events(events):
    engine_events = []
    for i, event in enumerate(events):
        current = dict(event)
        current["ts"] = _coerce_engine_timestamp(current.get("ts"), i)
        engine_events.append(current)
    return engine_events


def _flatten_match_events(events):
    flattened = []
    for event in events:
        if isinstance(event, AggregatedEvent):
            flattened.extend(_flatten_match_events(event.primitive_events))
        else:
            flattened.append(event)
    return flattened


def _pack_result(idxs: List[int], split_matches: List[List[int]], return_split: bool):
    return (idxs, split_matches) if return_split else idxs


def _empty_result(return_split: bool):
    return ([], []) if return_split else []


def _dedupe_and_sort(matches):
    unique = {tuple(match) for match in matches}
    return [list(match) for match in sorted(unique, key=lambda item: (item[0], item[-1], item))]


@dataclass(frozen=True)
class TimeConstraint:
    start_index: int
    end_index: int
    min_delta: timedelta | int | float | None = None
    max_delta: timedelta | int | float | None = None

    def _to_timedelta(self, value):
        if value is None or isinstance(value, timedelta):
            return value
        return timedelta(seconds=float(value))

    def check(self, idxs, events):
        if self.start_index >= len(idxs) or self.end_index >= len(idxs):
            return False
        start_ts = events[idxs[self.start_index]]["ts"]
        end_ts = events[idxs[self.end_index]]["ts"]
        delta = end_ts - start_ts
        min_delta = self._to_timedelta(self.min_delta)
        max_delta = self._to_timedelta(self.max_delta)
        if min_delta is not None and delta < min_delta:
            return False
        if max_delta is not None and delta > max_delta:
            return False
        return True

    def to_opencep_condition(self, top_level_parts):
        return _build_time_condition(self, top_level_parts)


@dataclass(frozen=True)
class EventValueConstraint:
    index: int
    attr: str
    relation_op: callable
    value: object

    def check(self, idxs, events):
        if self.index >= len(idxs):
            return False
        return self.relation_op(events[idxs[self.index]][self.attr], self.value)

    def to_opencep_condition(self, top_level_parts):
        if self.index >= len(top_level_parts):
            return None
        event_name = _primitive_name_for_part(top_level_parts[self.index])
        if event_name is None:
            return None
        return SimpleCondition(
            Variable(event_name, lambda x, attr=self.attr: x[attr]),
            self.value,
            relation_op=self.relation_op,
        )


@dataclass(frozen=True)
class EventRelationConstraint:
    terms: tuple[tuple[int, str], ...]
    relation_op: callable

    def check(self, idxs, events):
        values = []
        for index, attr in self.terms:
            if index >= len(idxs):
                return False
            values.append(events[idxs[index]][attr])
        return self.relation_op(*values)

    def to_opencep_condition(self, top_level_parts):
        variables = []
        for index, attr in self.terms:
            if index >= len(top_level_parts):
                return None
            event_name = _primitive_name_for_part(top_level_parts[index])
            if event_name is None:
                return None
            variables.append(Variable(event_name, lambda x, attr=attr: x[attr]))
        return SimpleCondition(*variables, relation_op=self.relation_op)


@dataclass(frozen=True)
class KleeneValueConstraint:
    index: int
    attr: str
    relation_op: callable
    value: object

    def check(self, idxs, events, split_matches=None):
        if split_matches is None or self.index >= len(split_matches):
            return False
        group = split_matches[self.index]
        if not group:
            return False
        return all(self.relation_op(events[i][self.attr], self.value) for i in group)

    def to_opencep_condition(self, top_level_parts):
        if self.index >= len(top_level_parts):
            return None
        part = top_level_parts[self.index]
        if not isinstance(part, KleeneClosureOperator) or not isinstance(part.arg, PrimitiveEventStructure):
            return None
        return KCValueCondition(
            names={part.arg.name},
            getattr_func=lambda x, attr=self.attr: x[attr],
            relation_op=self.relation_op,
            value=self.value,
        )


def _primitive_name_for_part(part: PatternStructure):
    if isinstance(part, PrimitiveEventStructure):
        return part.name
    if isinstance(part, KleeneClosureOperator) and isinstance(part.arg, PrimitiveEventStructure):
        return part.arg.name
    return None


def _build_gap_condition(gap_constraint, top_level_parts):
    if gap_constraint.start_index >= len(top_level_parts) or gap_constraint.end_index >= len(top_level_parts):
        return None

    start_part = top_level_parts[gap_constraint.start_index]
    end_part = top_level_parts[gap_constraint.end_index]

    start_name = _primitive_name_for_part(start_part)
    end_name = _primitive_name_for_part(end_part)
    if start_name is None or end_name is None:
        return None

    if gap_constraint.start_anchor != "first" or gap_constraint.end_anchor != "first":
        return None

    return BinaryCondition(
        Variable(start_name, lambda x: x[Event.INDEX_ATTRIBUTE_NAME]),
        Variable(end_name, lambda x: x[Event.INDEX_ATTRIBUTE_NAME]),
        relation_op=lambda x, y, gc=gap_constraint: (
            y - x - 1 >= gc.min_gap and
            (gc.max_gap is None or y - x - 1 <= gc.max_gap)
        ),
    )


def _build_time_condition(time_constraint: TimeConstraint, top_level_parts):
    if time_constraint.start_index >= len(top_level_parts) or time_constraint.end_index >= len(top_level_parts):
        return None

    start_name = _primitive_name_for_part(top_level_parts[time_constraint.start_index])
    end_name = _primitive_name_for_part(top_level_parts[time_constraint.end_index])
    if start_name is None or end_name is None:
        return None

    min_delta = time_constraint._to_timedelta(time_constraint.min_delta)
    max_delta = time_constraint._to_timedelta(time_constraint.max_delta)

    return BinaryCondition(
        Variable(start_name, lambda x: x["ts"]),
        Variable(end_name, lambda x: x["ts"]),
        relation_op=lambda start_ts, end_ts, min_d=min_delta, max_d=max_delta: (
            (min_d is None or end_ts - start_ts >= min_d) and
            (max_d is None or end_ts - start_ts <= max_d)
        ),
    )


def _split_constraints(constraints, strict_native: bool = False):
    native = []
    residual = []
    if not constraints:
        return native, residual
    for constraint in constraints:
        if (
            isinstance(constraint, TimeConstraint)
            or hasattr(constraint, "to_opencep_condition")
            or (
                hasattr(constraint, "start_index")
                and hasattr(constraint, "end_index")
                and hasattr(constraint, "min_gap")
            )
        ):
            native.append(constraint)
        else:
            if strict_native:
                raise TypeError(
                    f"Constraint {type(constraint).__name__} is not translatable to native OpenCEP conditions"
                )
            residual.append(constraint)
    return native, residual


def _merge_legacy_constraints(constraints, gap_constraints, event_constraints):
    merged = list(constraints or [])
    if gap_constraints:
        merged.extend(gap_constraints)
    if event_constraints:
        merged.extend(event_constraints)
    return merged


def _attach_native_conditions(pattern, top_level_parts, constraints):
    conditions = []
    native_constraints, residual_constraints = _split_constraints(constraints)
    for constraint in native_constraints:
        if hasattr(constraint, "start_index") and hasattr(constraint, "end_index") and hasattr(constraint, "min_gap"):
            condition = _build_gap_condition(constraint, top_level_parts)
        elif isinstance(constraint, TimeConstraint):
            condition = _build_time_condition(constraint, top_level_parts)
        else:
            condition = constraint.to_opencep_condition(top_level_parts)
        if condition is not None:
            conditions.append(condition)
        else:
            residual_constraints.append(constraint)

    for condition in conditions:
        pattern.condition.add_atomic_condition(condition)

    return residual_constraints


def _check_post_filters(residual_constraints, idxs, constraint_events, split_matches):
    if not residual_constraints:
        return True
    for constraint in residual_constraints:
        if hasattr(constraint, "start_index") and hasattr(constraint, "end_index") and hasattr(constraint, "min_gap"):
            if not _check_gap_constraints(split_matches, [constraint]):
                return False
            continue
        try:
            result = constraint.check(idxs, constraint_events, split_matches)
        except TypeError:
            result = constraint.check(idxs, constraint_events)
        if not result:
            return False
    return True


def parse_query(expr: str):
    return Parser(tokenize(expr)).parse()


def translate_query_to_pattern(expr: str):
    ast = parse_query(expr)
    builder = _PatternBuilder()
    pattern = Pattern(
        builder.build_root(ast),
        TrueCondition(),
        timedelta(days=3650),
    )
    return ast, pattern, builder.top_level_parts


def _build_preprocessing_params(ast):
    if not _has_alt(ast):
        return None
    return PatternPreprocessingParameters([
        PatternTransformationRules.TOPMOST_OR_PATTERN,
        PatternTransformationRules.INNER_OR_PATTERN,
    ])


def run_opencep_pattern(pattern: Pattern, events, pattern_preprocessing_params=None):
    Event.counter = 0
    output = _MemoryOutputStream()
    CEP([pattern], pattern_preprocessing_params=pattern_preprocessing_params).run(
        _MemoryInputStream(events),
        output,
        _OpenCEPDataFormatter(),
    )
    return output.items


def find_occurrences_opencep(
    sequence: List[str],
    expr: str,
    returnAll: bool = False,
    returnSplit: bool = False,
    constraints=None,
    gap_constraints=None,
    event_constraints=None,
    events=None,
):
    """
    Adapter from the test_main-style matcher input into an OpenCEP run.

    Supported expression subset:
    - concatenation, e.g. "a b c"
    - alternation, e.g. "a (b|c)"
    - plus, e.g. "a+ b"
    - negation inside sequence gaps, e.g. "a !d b"

    Preferred constraint API:
    - pass a single list via constraints=[...]
    - supported native constraints include GapConstraint, TimeConstraint,
      EventValueConstraint, EventRelationConstraint, and KleeneValueConstraint

    Unsupported:
    - star ("*"), because it does not map cleanly to OpenCEP's Kleene closure semantics
    - exact behavioral equivalence with the regex matcher for all nested expressions
    """
    ast, pattern, top_level_parts = translate_query_to_pattern(expr)
    constraint_events = _normalize_events(sequence, events)
    engine_events = _build_engine_events(constraint_events)
    residual_constraints = _attach_native_conditions(
        pattern,
        top_level_parts,
        _merge_legacy_constraints(constraints, gap_constraints, event_constraints),
    )
    preprocessing_params = _build_preprocessing_params(ast)
    output_items = run_opencep_pattern(pattern, engine_events, preprocessing_params)

    candidate_matches = []
    for match in output_items:
        idxs = sorted(
            event.payload[Event.INDEX_ATTRIBUTE_NAME]
            for event in _flatten_match_events(match.events)
        )
        split_matches = _extract_split_matches(ast, sequence, idxs)
        if not _check_post_filters(residual_constraints, idxs, constraint_events, split_matches):
            continue
        candidate_matches.append(idxs)

    filtered_matches = _dedupe_and_sort(candidate_matches)
    if not filtered_matches:
        return _empty_result(returnSplit) if not returnAll else []

    if not returnAll:
        idxs = filtered_matches[0]
        return _pack_result(idxs, _extract_split_matches(ast, sequence, idxs), returnSplit)

    results = []
    last_end = -1
    for idxs in filtered_matches:
        if idxs[0] <= last_end:
            continue
        results.append(_pack_result(idxs, _extract_split_matches(ast, sequence, idxs), returnSplit))
        last_end = idxs[-1]
    return results


if __name__ == "__main__":
    sample_events = [
        {"name": "a", "user": "alice"},
        {"name": "b", "user": "bob"},
        {"name": "c", "user": "alice"},
    ]
    sample_sequence = [event["name"] for event in sample_events]
    print(find_occurrences_opencep(sample_sequence, "a b c", events=sample_events))
    
