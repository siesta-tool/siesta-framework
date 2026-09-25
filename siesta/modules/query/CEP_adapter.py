import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import product
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

from siesta.modules.query.parse_seql import (
    ActivityNode, AttrConstraint, ElementNode, NegatedNode,
    OrNode, Quantifier, SeqNode, StringLiteral, VarExpr, parse_pattern,
)


# Position of an event in the list handed to OpenCEP.  OpenCEP's own
# Event.INDEX_ATTRIBUTE_NAME is a global serial that Kleene-closure
# AggregatedEvents also consume, so it drifts away from list positions.
_EVENT_INDEX_KEY = "SiestaEventIndex"


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


class _DSLPatternBuilder:
    """
    Converts a parse_seql DSL AST into an OpenCEP PatternStructure.
 
    Mirrors _PatternBuilder but operates on parse_seql AST node types
    (ActivityNode, SeqNode, OrNode, NegatedNode, ElementNode) rather than
    the simpler main.py types (Lit, Concat, Alt, Not, Plus, Star).
 
    After calling build_root(), `positive_event_names` holds the ordered
    list of (ActivityNode, assigned_opencep_name) for every non-negated
    activity, which is used to build attribute conditions.
    """
 
    def __init__(self):
        self._counter = 0
        self.top_level_parts: List[PatternStructure] = []
        # (ActivityNode, opencep_name) for each positive (non-negated) event,
        # in left-to-right traversal order - used for $N VarExpr resolution.
        self.positive_event_names: List[tuple] = []
        # (ActivityNode, opencep_name) for each event inside a negation; their
        # attribute constraints restrict which events are forbidden.
        self.negated_event_names: List[tuple] = []
 
    def _next_name(self) -> str:
        name = f"e{self._counter}"
        self._counter += 1
        return name
 
    def _build_element(self, elem: ElementNode) -> PatternStructure:
        """Wrap the atom's PatternStructure in a Kleene closure when needed."""
        built = self._build_node(elem.atom, inside_negation=False)
        q = elem.quantifier
        if q == Quantifier.PLUS:
            return KleeneClosureOperator(built, min_size=1)
        if q == Quantifier.STAR:
            raise NotImplementedError(
                "OpenCEP DSL adapter does not support the '*' (STAR) quantifier"
            )
        if q == Quantifier.OPT:
            raise NotImplementedError(
                "OpenCEP DSL adapter does not support the '?' (OPT) quantifier"
            )
        return built  # Quantifier.ONE - pass through unchanged
 
    def _build_node(self, node, inside_negation: bool) -> PatternStructure:
        if isinstance(node, ActivityNode):
            name = self._next_name()
            struct = PrimitiveEventStructure(node.label, name)
            if inside_negation:
                self.negated_event_names.append((node, name))
            else:
                self.positive_event_names.append((node, name))
            return struct
 
        if isinstance(node, SeqNode):
            return SeqOperator(*(self._build_element(e) for e in node.elements))
 
        if isinstance(node, OrNode):
            # OrOperator is binary; chain multiple branches left-to-right.
            branches = [self._build_node(b, inside_negation) for b in node.branches]
            result = branches[0]
            for b in branches[1:]:
                result = OrOperator(result, b)
            return result
 
        if isinstance(node, NegatedNode):
            # Activities inside a negation are forbidden markers; they must not
            # be recorded in positive_event_names or used for conditions.
            return NegationOperator(self._build_node(node.inner, inside_negation=True))
 
        raise TypeError(f"Unsupported parse_seql AST node: {type(node).__name__}")
 
    def build_root(self, ast) -> PatternStructure:
        """
        Build the root PatternStructure and populate top_level_parts.
 
        top_level_parts mirrors the logic of _PatternBuilder.build_root:
        it contains only the non-negated, top-level elements of a SeqNode
        so that gap/time constraints can reference them by index.
        """
        if isinstance(ast, SeqNode):
            args: List[PatternStructure] = []
            self.top_level_parts = []
            for elem in ast.elements:
                built = self._build_element(elem)
                args.append(built)
                if not isinstance(elem.atom, NegatedNode):
                    self.top_level_parts.append(built)
            return SeqOperator(*args)
 
        built = self._build_node(ast, inside_negation=False)
        self.top_level_parts = [] if isinstance(ast, NegatedNode) else [built]
        return built


def _has_or_dsl(node) -> bool:
    """Return True if the parse_seql AST contains any alternation (OrNode)."""
    if isinstance(node, OrNode):
        return True
    if isinstance(node, SeqNode):
        return any(_has_or_dsl(e.atom) for e in node.elements)
    if isinstance(node, NegatedNode):
        return _has_or_dsl(node.inner)
    return False


def _split_negated_or(node):
    """
    Rewrite ``!(b || c)`` as ``!b !c`` inside sequences.

    OpenCEP cannot negate an OR, and forbidding every alternative between the
    same neighbours is exactly the DSL's (unioned) negation semantics.  Only
    ORs of single, unquantified activities are rewritten.
    """
    if isinstance(node, SeqNode):
        elements = []
        for element in node.elements:
            atom = element.atom
            if isinstance(atom, NegatedNode):
                inner = atom.inner
                if (isinstance(inner, OrNode) and all(
                        len(b.elements) == 1
                        and b.elements[0].quantifier == Quantifier.ONE
                        and isinstance(b.elements[0].atom, ActivityNode)
                        for b in inner.branches)):
                    elements.extend(
                        ElementNode(atom=NegatedNode(inner=b.elements[0].atom),
                                    quantifier=element.quantifier)
                        for b in inner.branches
                    )
                    continue
                elements.append(element)
            else:
                elements.append(ElementNode(atom=_split_negated_or(atom), quantifier=element.quantifier))
        return SeqNode(elements)
    if isinstance(node, OrNode):
        return OrNode([_split_negated_or(b) for b in node.branches])
    return node


def _drop_unanchored_negations(original_elements, combination):
    """
    Build one star-expanded element list, dropping negations whose anchor was
    omitted.

    A negation constrains the gap between the positive elements around it.
    When every positive element on one of its sides was a ``*`` that this
    alternative omits, the negation has nothing to bound on that side and
    would otherwise turn into a leading/trailing negation over the rest of
    the trace (``D !E D*`` must not mean "no E after D" when ``D*`` is empty).
    Negations written at the start or end of the pattern keep their meaning.
    """
    is_positive = [not isinstance(e.atom, NegatedNode) for e in original_elements]
    kept = []
    for k, element in enumerate(combination):
        if element is None:
            continue
        if not is_positive[k]:
            left = [i for i in range(k) if is_positive[i]]
            right = [i for i in range(k + 1, len(original_elements)) if is_positive[i]]
            if ((left and all(combination[i] is None for i in left))
                    or (right and all(combination[i] is None for i in right))):
                continue
        kept.append(element)
    return kept


def _expand_star_dsl(node):
    """
    Expand every STAR quantifier into two star-free alternatives:
    one branch replaces ``x*`` with ``x+`` and the other omits ``x``.
    """
    if isinstance(node, SeqNode):
        per_element = []
        for element in node.elements:
            atom_alternatives = _expand_star_dsl(element.atom)
            if element.quantifier == Quantifier.STAR:
                alternatives = [
                    ElementNode(atom=atom, quantifier=Quantifier.PLUS)
                    for atom in atom_alternatives
                ]
                alternatives.append(None)
            else:
                alternatives = [
                    ElementNode(atom=atom, quantifier=element.quantifier)
                    for atom in atom_alternatives
                ]
            per_element.append(alternatives)

        expanded = []
        for combination in product(*per_element):
            elements = _drop_unanchored_negations(node.elements, combination)
            if elements:
                expanded.append(SeqNode(elements))
        return expanded or [node]

    if isinstance(node, OrNode):
        per_branch = [_expand_star_dsl(branch) for branch in node.branches]
        return [OrNode(list(branches)) for branches in product(*per_branch)]

    if isinstance(node, NegatedNode):
        return [NegatedNode(inner) for inner in _expand_star_dsl(node.inner)]

    return [node]


def _build_dsl_attr_conditions(positive_event_names: list, negated_event_names: list = ()) -> List:
    """
    Convert DSL attribute constraints into native OpenCEP conditions.
 
    StringLiteral constraints  ->  SimpleCondition (unary, per-event check).
    VarExpr constraints        ->  BinaryCondition (cross-event equality /
                                  arithmetic), where $N refers to the Nth
                                  positive event in left-to-right pattern order
                                  (1-based).
 
    Constraints on negated events (``negated_event_names``) are attached the
    same way, so only forbidden events that satisfy them block a match.  A
    Kleene-closure variable evaluates to a list of events; a condition on it
    must hold for every event in the closure.

    Constraints whose VarExpr index is out of range are silently skipped;
    callers should add a fallback post-filter if strict validation is needed.
    """
    conditions: List = []
    n = len(positive_event_names)
 
    for activity_node, own_name in list(positive_event_names) + list(negated_event_names):
        for constraint in activity_node.constraints:
            attr = constraint.name
            val  = constraint.value
            neq  = (getattr(constraint, "op", "=") == "!=")

            if isinstance(val, StringLiteral):
                # Unary condition: event[attr] (==|!=) literal
                literal = val.value
                if neq:
                    lit_rel = lambda v, lit=literal: v != lit
                else:
                    lit_rel = lambda v, lit=literal: v == lit
                conditions.append(
                    SimpleCondition(
                        Variable(own_name, lambda x, a=attr: x.get(a)),
                        relation_op=lit_rel,
                    )
                )
 
                def _numeric_eq(ov, rv, offset: int | float, op: str) -> bool:
                    """Compare two attribute values (possibly strings) with an arithmetic offset.

                    Attribute values stored in the index are always strings.  This helper
                    converts both sides to float before the arithmetic so that
                    ``A[cost=$1] B[cost=$1+5]`` works when cost is stored as "10" / "5".
                    Returns False if either value cannot be converted to a number.
                    """
                    try:
                        rv_f = float(rv)
                        ov_f = float(ov)
                    except (TypeError, ValueError):
                        return False
                    if op == "+":
                        return ov_f == rv_f + offset
                    return ov_f == rv_f - offset


            elif isinstance(val, VarExpr):
                # Cross-event condition: this event's attr relates to the attr
                # of the $ref_id-th positive event (1-based).
                ref_id = val.var_id
                if ref_id < 1 or ref_id > n:
                    continue  # out-of-range reference - skip
                _, ref_name = positive_event_names[ref_id - 1]
                op     = val.op      # '+' | '-' | None
                offset = val.offset or 0

                if op == "+":
                    def rel(rv, ov, off=offset, neq=neq):
                        try:
                            eq = float(ov) == float(rv) + off
                        except (TypeError, ValueError):
                            return False
                        return (not eq) if neq else eq
                elif op == "-":
                    def rel(rv, ov, off=offset, neq=neq):
                        try:
                            eq = float(ov) == float(rv) - off
                        except (TypeError, ValueError):
                            return False
                        return (not eq) if neq else eq
                else:
                    rel = (lambda rv, ov: ov != rv) if neq else (lambda rv, ov: ov == rv)

                conditions.append(
                    BinaryCondition(
                        Variable(ref_name, lambda x, a=attr: _attr_values(x, a)),
                        Variable(own_name, lambda x, a=attr: _attr_values(x, a)),
                        relation_op=lambda rv, ov, rel=rel: all(
                            rel(r, o) for r in rv for o in ov
                        ),
                    )
                )
 
    return conditions

def _attr_values(payload, attr):
    """Attribute values of one event, or of every event of a Kleene closure."""
    if isinstance(payload, list):
        return [p.get(attr) for p in payload]
    return [payload.get(attr)]


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
        current[_EVENT_INDEX_KEY] = i
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
    # OpenCEP can bind one event to two pattern positions (e.g. ``D`` and a
    # following ``D+`` closure); such matches are not valid occurrences.
    unique = {tuple(match) for match in matches if len(set(match)) == len(match)}
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
        Variable(start_name, lambda x: x[_EVENT_INDEX_KEY]),
        Variable(end_name, lambda x: x[_EVENT_INDEX_KEY]),
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


def find_occurrences_dsl(
    sequence: List[str],
    pattern_str: str,
    returnAll: bool = False,
    returnSplit: bool = False,
    constraints=None,
    events=None,
):
    """
    Adapter from the parse_seql DSL pattern language into an OpenCEP run.
 
    Accepted DSL features
    ---------------------
    - Sequence:      'A B C'
    - Alternation:   'A (B||C)'
    - Plus:          'A+ B'
    - Negation:      'A !B C'   (B must not appear between A and C)
    - Literal attrs: 'A[resource="alice"] B'
    - Var refs:      'A[amount=$1] B[amount=$1+100]'
                     ($N references the Nth positive event, 1-based)
 
    Not supported (raises NotImplementedError, matching find_occurrences_opencep):
    - Optional ('?') quantifier
 
    Parameters
    ----------
    sequence     : ordered list of event-type names for the trace
    pattern_str  : parse_seql DSL pattern string
    returnAll    : if True, return all non-overlapping matches; else first only
    returnSplit  : if True, return (idxs, split_matches) tuples
                   (split_matches are [] for DSL paths - no main.py AST)
    constraints  : optional list of TimeConstraint / GapConstraint objects
    events       : optional list of event dicts (must align with sequence)
    """
    # 1. Parse the DSL pattern and expand every STAR into two star-free queries:
    #    one with PLUS semantics and one with the STAR branch omitted.
    ast = parse_pattern(pattern_str)
    expanded_asts = [_split_negated_or(a) for a in _expand_star_dsl(ast)]

    # 2. Prepare the event stream once and reuse it across the expanded queries.
    constraint_events = _normalize_events(sequence, events)
    engine_events = _build_engine_events(constraint_events)

    # 3. Run each expanded query independently and merge the matches.
    #    split_matches are passed as [] because _extract_split_matches requires
    #    a main.py AST (not available here).  Callers should not rely on
    #    returnSplit when using find_occurrences_dsl.
    candidate_matches = []
    for expanded_ast in expanded_asts:
        builder = _DSLPatternBuilder()
        pattern_structure = builder.build_root(expanded_ast)

        pattern = Pattern(
            pattern_structure,
            TrueCondition(),
            timedelta(days=3650),
        )

        for cond in _build_dsl_attr_conditions(builder.positive_event_names,
                                               builder.negated_event_names):
            pattern.condition.add_atomic_condition(cond)

        residual_constraints = _attach_native_conditions(
            pattern,
            builder.top_level_parts,
            constraints or [],
        )

        preprocessing_params = (
            PatternPreprocessingParameters([
                PatternTransformationRules.TOPMOST_OR_PATTERN,
                PatternTransformationRules.INNER_OR_PATTERN,
            ])
            if _has_or_dsl(expanded_ast) else None
        )

        output_items = run_opencep_pattern(pattern, engine_events, preprocessing_params)
        for match in output_items:
            idxs = sorted(
                event.payload[_EVENT_INDEX_KEY]
                for event in _flatten_match_events(match.events)
            )
            if not _check_post_filters(residual_constraints, idxs, constraint_events, []):
                continue
            candidate_matches.append(idxs)
 
    filtered_matches = _dedupe_and_sort(candidate_matches)
    if not filtered_matches:
        return _empty_result(returnSplit) if not returnAll else []
 
    if not returnAll:
        idxs = filtered_matches[0]
        return _pack_result(idxs, [], returnSplit)
 
    results = []
    last_end = -1
    for idxs in filtered_matches:
        if idxs[0] <= last_end:
            continue
        results.append(_pack_result(idxs, [], returnSplit))
        last_end = idxs[-1]
    return results
 