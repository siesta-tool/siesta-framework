import re
import regex
import sys
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Union
from bisect import bisect_right


# ----------------------------
# AST nodes
# ----------------------------
@dataclass(frozen=True)
class Lit:
    v: str

@dataclass(frozen=True)
class Concat:
    parts: List["Node"]

@dataclass(frozen=True)
class Alt:
    left: "Node"
    right: "Node"

@dataclass(frozen=True)
class Star:
    node: "Node"

@dataclass(frozen=True)
class Plus:
    node: "Node"

@dataclass(frozen=True)
class Not:
    node: "Node"

Node = Union[Lit, Concat, Alt, Star, Plus, Not]


@dataclass(frozen=True)
class GapConstraint:
    """Positional gap between two matched query indices.
    start_index/end_index refer to top-level positive pattern parts.
    Gap = number of sequence tokens between the two matched events (exclusive).
    """
    start_index: int
    end_index: int
    min_gap: int = 0
    max_gap: Optional[int] = None
    start_anchor: str = "first"
    end_anchor: str = "first"


@dataclass(frozen=True)
class EventConstraint:
    """External constraint evaluated after a candidate match is found.
    check: callable(indices, events) -> bool
        indices: list of matched token positions (e.g. [0, 2, 5])
        events:  the full event list passed to find_occurrences
        Return True if the constraint is satisfied.
    """
    check: Callable[[List[int], Any], bool]


def _has_negation(node: Node) -> bool:
    """Return True if the AST contains any Not nodes."""
    if isinstance(node, Not):
        return True
    if isinstance(node, Lit):
        return False
    if isinstance(node, Alt):
        return _has_negation(node.left) or _has_negation(node.right)
    if isinstance(node, Concat):
        return any(_has_negation(p) for p in node.parts)
    if isinstance(node, (Star, Plus)):
        return _has_negation(node.node)
    return False


def _validate_gap_constraint(gc: GapConstraint) -> None:
    if gc.start_index < 0 or gc.end_index < 0:
        raise ValueError("GapConstraint indices must be non-negative")
    if gc.start_index >= gc.end_index:
        raise ValueError("GapConstraint start_index must be less than end_index")
    if gc.min_gap < 0:
        raise ValueError("GapConstraint min_gap must be non-negative")
    if gc.max_gap is not None and gc.max_gap < gc.min_gap:
        raise ValueError("GapConstraint max_gap must be >= min_gap")
    if gc.start_anchor not in {"first", "last"}:
        raise ValueError("GapConstraint start_anchor must be 'first' or 'last'")
    if gc.end_anchor not in {"first", "last"}:
        raise ValueError("GapConstraint end_anchor must be 'first' or 'last'")


def _build_adjacent_gap_specs(gap_constraints):
    gap_specs = {}
    if not gap_constraints:
        return gap_specs
    for gc in gap_constraints:
        _validate_gap_constraint(gc)
        if gc.end_index == gc.start_index + 1 and gc.start_anchor == "last" and gc.end_anchor == "first":
            gap_specs[gc.start_index] = (gc.min_gap, gc.max_gap)
    return gap_specs


def _anchor_event(group: List[int], anchor: str) -> Optional[int]:
    if not group:
        return None
    return group[0] if anchor == "first" else group[-1]


def _check_gap_constraints(split_matches: List[List[int]], gap_constraints) -> bool:
    if not gap_constraints:
        return True
    for gc in gap_constraints:
        _validate_gap_constraint(gc)
        if gc.end_index >= len(split_matches):
            return False
        start_event = _anchor_event(split_matches[gc.start_index], gc.start_anchor)
        end_event = _anchor_event(split_matches[gc.end_index], gc.end_anchor)
        if start_event is None or end_event is None or start_event >= end_event:
            return False
        gap = end_event - start_event - 1
        if gap < gc.min_gap:
            return False
        if gc.max_gap is not None and gap > gc.max_gap:
            return False
    return True


def _enumerate_node_matches(node: Node, sequence: List[str], pos: int, end: int):
    if isinstance(node, Lit):
        for i in range(pos, end + 1):
            if sequence[i] == node.v:
                yield i + 1, [i]
        return

    if isinstance(node, Alt):
        yield from _enumerate_node_matches(node.left, sequence, pos, end)
        yield from _enumerate_node_matches(node.right, sequence, pos, end)
        return

    if isinstance(node, Concat):
        def gap_has_forbidden(start: int, stop: int, forbidden: List[Node]) -> bool:
            if not forbidden or start >= stop:
                return False
            gap_end = stop - 1
            for fnode in forbidden:
                for s in range(start, stop):
                    for _, _ in _enumerate_node_matches(fnode, sequence, s, gap_end):
                        return True
            return False

        def walk(parts: List[Node], part_i: int, cur_pos: int, acc: List[int], pending_forbidden: List[Node]):
            if part_i == len(parts):
                if pending_forbidden:
                    return
                yield cur_pos, acc
                return
            part = parts[part_i]
            if isinstance(part, Not):
                yield from walk(parts, part_i + 1, cur_pos, acc, pending_forbidden + [part.node])
                return

            for next_pos, idxs in _enumerate_node_matches(part, sequence, cur_pos, end):
                next_pending = pending_forbidden
                if pending_forbidden and idxs:
                    first_idx = idxs[0]
                    if gap_has_forbidden(cur_pos, first_idx, pending_forbidden):
                        continue
                    next_pending = []
                yield from walk(parts, part_i + 1, next_pos, acc + idxs, next_pending)

        yield from walk(node.parts, 0, pos, [], [])
        return

    if isinstance(node, Plus):
        yield from _enumerate_repetition_matches(node.node, sequence, pos, end, min_reps=1)
        return

    if isinstance(node, Star):
        yield pos, []
        yield from _enumerate_repetition_matches(node.node, sequence, pos, end, min_reps=1)
        return

    if isinstance(node, Not):
        raise ValueError("Negation can only be used inside concatenation gaps")

    raise TypeError(f"Unknown node: {node}")


def _enumerate_repetition_matches(node: Node, sequence: List[str], pos: int, end: int, min_reps: int):
    # Collect all repetition paths, then emit greedy-first (more matched events first).
    all_matches = []

    def dfs(cur_pos: int, acc: List[int], reps: int):
        if reps >= min_reps:
            all_matches.append((cur_pos, acc))
        for next_pos, idxs in _enumerate_node_matches(node, sequence, cur_pos, end):
            # Guard against zero-progress loops for pathological expressions.
            if next_pos == cur_pos:
                continue
            dfs(next_pos, acc + idxs, reps + 1)

    dfs(pos, [], 0)
    all_matches.sort(key=lambda m: (-len(m[1]), m[1]))
    for m in all_matches:
        yield m


def _enumerate_top_level_matches(ast: Node, sequence: List[str], pos: int, end: int):
    if isinstance(ast, Concat):
        def gap_has_forbidden(start: int, stop: int, forbidden: List[Node]) -> bool:
            if not forbidden or start >= stop:
                return False
            gap_end = stop - 1
            for fnode in forbidden:
                for s in range(start, stop):
                    for _, _ in _enumerate_node_matches(fnode, sequence, s, gap_end):
                        return True
            return False

        def walk(parts: List[Node], part_i: int, cur_pos: int, acc: List[int], split_acc: List[List[int]], pending_forbidden: List[Node]):
            if part_i == len(parts):
                if pending_forbidden:
                    return
                yield cur_pos, acc, split_acc
                return
            part = parts[part_i]
            if isinstance(part, Not):
                yield from walk(parts, part_i + 1, cur_pos, acc, split_acc, pending_forbidden + [part.node])
                return

            for next_pos, idxs in _enumerate_node_matches(part, sequence, cur_pos, end):
                next_pending = pending_forbidden
                if pending_forbidden and idxs:
                    first_idx = idxs[0]
                    if gap_has_forbidden(cur_pos, first_idx, pending_forbidden):
                        continue
                    next_pending = []
                yield from walk(parts, part_i + 1, next_pos, acc + idxs, split_acc + [idxs], next_pending)

        yield from walk(ast.parts, 0, pos, [], [], [])
        return

    for next_pos, idxs in _enumerate_node_matches(ast, sequence, pos, end):
        yield next_pos, idxs, [idxs]


def _extract_split_matches(ast: Node, sequence: List[str], idxs: List[int]) -> List[List[int]]:
    if not idxs:
        return []
    start_tok, end_tok = idxs[0], idxs[-1]
    for _, candidate_idxs, split_matches in _enumerate_top_level_matches(ast, sequence, start_tok, end_tok):
        if candidate_idxs == idxs:
            return split_matches
    return []


# ----------------------------
# Tokenizer
# literals: any run of non-operator, non-whitespace chars
# operators: ( ) * + | !
# supports "||" as "|"
# ----------------------------
_token_re = re.compile(r"\s*(\|\||[()*+|!]|[^()*+|!\s]+)")

def tokenize(expr: str) -> List[str]:
    toks = _token_re.findall(expr)
    # normalize "||" to "|"
    toks = ["|" if t == "||" else t for t in toks]
    return toks


# ----------------------------
# Recursive descent parser with precedence:
# 1) postfix: * +
# 2) concat
# 3) alternation |
# ----------------------------
class Parser:
    def __init__(self, tokens: List[str]):
        self.toks = tokens
        self.i = 0

    def peek(self) -> Optional[str]:
        return self.toks[self.i] if self.i < len(self.toks) else None

    def pop(self) -> str:
        t = self.peek()
        if t is None:
            raise ValueError("Unexpected end of expression")
        self.i += 1
        return t

    def parse(self) -> Node:
        node = self.parse_alt()
        if self.peek() is not None:
            raise ValueError(f"Unexpected token: {self.peek()}")
        return node

    def parse_alt(self) -> Node:
        node = self.parse_concat()
        while self.peek() == "|":
            self.pop()
            right = self.parse_concat()
            node = Alt(node, right)
        return node

    def parse_concat(self) -> Node:
        parts = []
        # concat continues until ')' or '|' or end
        while True:
            t = self.peek()
            if t is None or t in (")", "|"):
                break
            parts.append(self.parse_postfix())
        if not parts:
            # Empty concat not allowed in this minimal version
            raise ValueError("Empty expression segment")
        if isinstance(parts[0], Not):
            raise ValueError("Negation cannot start an expression segment")
        if isinstance(parts[-1], Not):
            raise ValueError("Negation must be followed by a positive token")
        if len(parts) == 1:
            return parts[0]
        return Concat(parts)

    def parse_postfix(self) -> Node:
        node = self.parse_atom()
        while True:
            t = self.peek()
            if t == "*":
                if isinstance(node, Not):
                    raise ValueError("Postfix operators cannot be applied to negation")
                self.pop()
                node = Star(node)
            elif t == "+":
                if isinstance(node, Not):
                    raise ValueError("Postfix operators cannot be applied to negation")
                self.pop()
                node = Plus(node)
            else:
                break
        return node

    def parse_atom(self) -> Node:
        t = self.pop()
        if t == "!":
            node = self.parse_atom()
            return Not(node)
        if t == "(":
            node = self.parse_alt()
            if self.pop() != ")":
                raise ValueError("Missing closing ')'")
            return node
        if t in (")", "|", "*", "+", "!"):
            raise ValueError(f"Unexpected token: {t}")
        return Lit(t)


# ----------------------------
# Compiler: AST -> Python regex over encoded token stream
# Subsequence semantics:
#   concat(A,B) = A + SKIP*? + B
# where SKIP matches one whole token: [^SEP]*SEP
#
# Repetition semantics (subsequence-aware & greedy):
#   X+ = X (SKIP*? X)*
#   X* = (X (SKIP*? X)*)?
# ----------------------------
def compile_to_re(ast: Node, sep: str = "\x1f", gap_constraints=None) -> re.Pattern:
    sep_esc = re.escape(sep)
    skip_one_token = rf"[^{sep_esc}]*{sep_esc}"   # one whole token (content + sep)
    skip_many = rf"(?:{skip_one_token})*?"        # non-greedy skipping (skip-till-next-match)

    gap_specs = _build_adjacent_gap_specs(gap_constraints)

    def skip_many_with_forbidden(forbidden: List[Node], min_gap: int = 0, max_gap: Optional[int] = None) -> str:
        if not forbidden and min_gap == 0 and max_gap is None:
            return skip_many
        hi = "" if max_gap is None else str(max_gap)
        if not forbidden:
            return rf"(?:{skip_one_token}){{{min_gap},{hi}}}?"
        forbidden_patterns = []
        for fnode in forbidden:
            forbidden_patterns.append(c(fnode))
        forbidden_alt = "|".join(f"(?:{p})" for p in dict.fromkeys(forbidden_patterns))
        # At each skipped token boundary, forbid any forbidden pattern from starting there.
        skip_one_safe = rf"(?:(?!(?:{forbidden_alt}))[^{sep_esc}]*{sep_esc})"
        if min_gap == 0 and max_gap is None:
            return rf"(?:{skip_one_safe})*?"
        return rf"(?:{skip_one_safe}){{{min_gap},{hi}}}?"

    def c(node: Node) -> str:
        if isinstance(node, Lit):
            # literal token must end at sep boundary
            return re.escape(node.v) + sep_esc

        if isinstance(node, Alt):
            return rf"(?:{c(node.left)}|{c(node.right)})"

        if isinstance(node, Concat):
            out = c(node.parts[0])
            pending_forbidden = []
            for nxt in node.parts[1:]:
                if isinstance(nxt, Not):
                    pending_forbidden.append(nxt.node)
                    continue
                out = out + skip_many_with_forbidden(pending_forbidden) + c(nxt)
                pending_forbidden = []
            if pending_forbidden:
                raise ValueError("Negation must be followed by a positive token")
            return rf"(?:{out})"

        if isinstance(node, Plus):
            base = c(node.node)
            # X (SKIP X)*  (the * is greedy by default)
            return rf"(?:{base}(?:{skip_many}{base})*)"

        if isinstance(node, Star):
            base = c(node.node)
            # optional of Plus form
            return rf"(?:{base}(?:{skip_many}{base})*)?"

        if isinstance(node, Not):
            raise ValueError("Negation can only be used inside concatenation gaps")

        raise TypeError(f"Unknown node: {node}")

    if isinstance(ast, Concat) and gap_specs:
        out = c(ast.parts[0])
        pending_forbidden = []
        pos_idx = 0
        for nxt in ast.parts[1:]:
            if isinstance(nxt, Not):
                pending_forbidden.append(nxt.node)
                continue
            gs = gap_specs.get(pos_idx, (0, None))
            out += skip_many_with_forbidden(pending_forbidden, gs[0], gs[1]) + c(nxt)
            pending_forbidden = []
            pos_idx += 1
        if pending_forbidden:
            raise ValueError("Negation must be followed by a positive token")
        pattern = rf"(?:{out})"
    else:
        pattern = c(ast)
    return re.compile(pattern)


# ----------------------------
# Compiler (capturing): AST -> regex with capturing groups on literals
# Uses the 'regex' module for .captures()/.spans() support.
# Does NOT support negation nodes.
# ----------------------------
def compile_to_re_capturing(ast: Node, sep: str = "\x1f", gap_constraints=None):
    sep_esc = regex.escape(sep)
    skip_one_token = rf"[^{sep_esc}]*{sep_esc}"
    skip_many = rf"(?:{skip_one_token})*?"

    gap_specs = _build_adjacent_gap_specs(gap_constraints)

    group_counter = [1]  # next capturing group number
    literal_groups = []  # group numbers that capture literal tokens

    def c(node: Node) -> str:
        if isinstance(node, Lit):
            gn = group_counter[0]
            group_counter[0] += 1
            literal_groups.append(gn)
            return "(" + regex.escape(node.v) + sep_esc + ")"

        if isinstance(node, Alt):
            return rf"(?:{c(node.left)}|{c(node.right)})"

        if isinstance(node, Concat):
            out = c(node.parts[0])
            for nxt in node.parts[1:]:
                out = out + skip_many + c(nxt)
            return rf"(?:{out})"

        if isinstance(node, Plus):
            # Compile inner node twice: first iteration + subsequent iterations
            # Each gets its own group numbers so .captures() tracks all of them
            base1 = c(node.node)
            base2 = c(node.node)
            return rf"(?:{base1}(?:{skip_many}{base2})*)"

        if isinstance(node, Star):
            base1 = c(node.node)
            base2 = c(node.node)
            return rf"(?:{base1}(?:{skip_many}{base2})*)?"

        if isinstance(node, Not):
            raise ValueError("compile_to_re_capturing does not support negation")

        raise TypeError(f"Unknown node: {node}")

    if isinstance(ast, Concat) and gap_specs:
        out = c(ast.parts[0])
        for i, nxt in enumerate(ast.parts[1:]):
            gs = gap_specs.get(i, (0, None))
            min_g, max_g = gs
            if min_g == 0 and max_g is None:
                skip = skip_many
            else:
                hi = "" if max_g is None else str(max_g)
                skip = rf"(?:{skip_one_token}){{{min_g},{hi}}}?"
            out += skip + c(nxt)
        pattern = rf"(?:{out})"
    else:
        pattern = c(ast)
    rx = regex.compile(pattern)
    return rx, literal_groups


# ----------------------------
# Matching API
# returnAll=False -> first match event indices (e.g. [0,1,3]) or []
# returnAll=True  -> list of event-index lists for all non-overlapping matches
# ----------------------------
def find_occurrences(
    sequence: List[str],
    expr: str,
    returnAll: bool = False,
    returnSplit: bool = False,
    sep: str = "\x1f",
    gap_constraints=None,
    event_constraints=None,
    events=None,
):
    # Encode
    if any(sep in s for s in sequence):
        raise ValueError("Separator appears in literals; choose a different sep.")

    encoded = sep.join(sequence) + sep  # token stream, each token ends with sep
    # Precompute sep positions for fast span->token-index mapping
    sep_positions = [i for i, ch in enumerate(encoded) if ch == sep]

    # Parse
    toks = tokenize(expr)
    ast = Parser(toks).parse()

    def _check_constraints(idxs: List[int], split_matches: List[List[int]]) -> bool:
        if not _check_gap_constraints(split_matches, gap_constraints):
            return False
        if not event_constraints:
            return True
        for ec in event_constraints:
            if not ec.check(idxs, events):
                return False
        return True

    def _pack_result(idxs: List[int], split_matches: List[List[int]]):
        return (idxs, split_matches) if returnSplit else idxs

    def _empty_result():
        return ([], []) if returnSplit else []

    def _mask_token(enc: str, token_idx: int) -> str:
        """Replace the content of token at token_idx with \\x00 so no literal
        can match it, but separators stay intact.  The regex skip pattern
        [^SEP]*SEP still passes over the masked slot."""
        start = sep_positions[token_idx - 1] + 1 if token_idx > 0 else 0
        end = sep_positions[token_idx]          # position of the \x1f separator
        return enc[:start] + '\x00' * (end - start) + enc[end:]

    def _mask_tokens(enc: str, token_indices: set) -> str:
        """Apply masks to all tokens in the set."""
        for ti in token_indices:
            enc = _mask_token(enc, ti)
        return enc

    def _backtrack_search(rx_obj, extract_fn, pos_start, return_all):
        """Right-to-left backtracking retry loop for event constraints.

        When a candidate [i0, i1, ..., iN] fails constraints:
        1. Mask iN (rightmost) and retry from same search pos.
        2. If no match found → remove iN-level masks, mask i(N-1) instead.
        3. Continue leftward.  When i0 (anchor) has no options → advance pos.

        Uses per-level mask sets so unwinding is clean.
        ``cascade_to`` tracks where to try next when a depth is exhausted.
        """
        results = []
        pos = pos_start
        # mask_levels[k] = set of token indices we've masked while
        #                   trying alternatives for position k in idxs
        mask_levels = {}  # depth_k -> set of masked token indices
        # When the regex finds no match due to masks, cascade_to tells us
        # which depth to try masking next (working leftward from the last
        # exhausted depth).
        cascade_to = None  # int or None

        def _all_masked():
            s = set()
            for v in mask_levels.values():
                s |= v
            return s

        def _clear_from_depth(depth):
            """Remove mask entries at depth >= given depth."""
            for d in list(mask_levels):
                if d >= depth:
                    del mask_levels[d]

        def _advance_anchor():
            """Clear all masks and advance pos past the current anchor."""
            nonlocal pos
            mask_levels.clear()
            next_tok = bisect_right(sep_positions, pos - 1)
            if next_tok < len(sep_positions):
                pos = sep_positions[next_tok] + 1
            else:
                pos = len(encoded)

        while pos < len(encoded):
            masked = _all_masked()
            enc = _mask_tokens(encoded, masked) if masked else encoded
            m = rx_obj.search(enc, pos)

            if not m:
                if not mask_levels:
                    _advance_anchor()
                    continue

                # Masks are too restrictive — the deepest level is exhausted.
                # Cascade: clear it and mark that we should try one level up.
                deepest = max(mask_levels)
                del mask_levels[deepest]
                cascade_to = deepest - 1
                if cascade_to < 1:
                    _advance_anchor()
                    cascade_to = None
                # else: loop retries with fewer masks; cascade_to ensures
                # the next constraint failure masks at the right depth.
                continue

            idxs = extract_fn(m)
            if not idxs:
                break
            split_matches = _extract_split_matches(ast, sequence, idxs)

            if _check_constraints(idxs, split_matches):
                if not return_all:
                    return _pack_result(idxs, split_matches)
                results.append(_pack_result(idxs, split_matches))
                pos = sep_positions[idxs[-1]] + 1 if idxs[-1] < len(sep_positions) else len(encoded)
                mask_levels.clear()
                cascade_to = None
                continue

            # Constraint failed — decide which depth to mask at.
            if cascade_to is not None:
                # We're cascading leftward from an exhausted deeper level.
                target_depth = cascade_to
                cascade_to = None
            else:
                # Fresh failure — start from the rightmost position.
                target_depth = len(idxs) - 1

            # Try to place a mask at target_depth, or cascade further left.
            placed = False
            for k in range(target_depth, 0, -1):
                tok = idxs[k]
                level_masks = mask_levels.get(k, set())
                if tok not in level_masks:
                    _clear_from_depth(k + 1)
                    mask_levels.setdefault(k, set()).add(tok)
                    placed = True
                    break
            if not placed:
                _advance_anchor()
                cascade_to = None

        return results if return_all else _empty_result()

    if _has_negation(ast):
        # Fallback: legacy regex + AST backtracking for negation patterns
        rx = compile_to_re(ast, sep=sep, gap_constraints=gap_constraints)

        def span_to_token_indices(start_char: int, end_char: int) -> tuple[int, int]:
            start_tok = bisect_right(sep_positions, start_char - 1)
            end_tok = bisect_right(sep_positions, end_char - 1) - 1
            return start_tok, end_tok

        def extract_event_indices(start_tok: int, end_tok: int) -> List[int]:
            for _, idxs in _enumerate_node_matches(ast, sequence, start_tok, end_tok):
                if idxs and idxs[0] == start_tok and idxs[-1] == end_tok:
                    if _check_constraints(idxs, _extract_split_matches(ast, sequence, idxs)):
                        return idxs
            return []

        if not returnAll:
            pos = 0
            while True:
                m = rx.search(encoded, pos)
                if not m:
                    return _empty_result()
                s_tok, e_tok = span_to_token_indices(m.start(), m.end())
                idxs = extract_event_indices(s_tok, e_tok)
                if idxs:
                    return _pack_result(idxs, _extract_split_matches(ast, sequence, idxs))
                pos = sep_positions[s_tok] + 1 if s_tok < len(sep_positions) else len(encoded)

        out = []
        pos = 0
        while True:
            m = rx.search(encoded, pos)
            if not m:
                break
            s_tok, e_tok = span_to_token_indices(m.start(), m.end())
            idxs = extract_event_indices(s_tok, e_tok)
            if idxs:
                out.append(_pack_result(idxs, _extract_split_matches(ast, sequence, idxs)))
                pos = sep_positions[e_tok] + 1
            else:
                pos = sep_positions[s_tok] + 1 if s_tok < len(sep_positions) else len(encoded)
        return out

    # Fast path: capturing groups via 'regex' module — no AST backtracking
    rx, literal_groups = compile_to_re_capturing(ast, sep=sep, gap_constraints=gap_constraints)

    def extract_indices_from_match(m) -> List[int]:
        indices = []
        for gn in literal_groups:
            for start, _end in m.spans(gn):
                tok_idx = bisect_right(sep_positions, start - 1)
                indices.append(tok_idx)
        indices.sort()
        return indices

    return _backtrack_search(rx, extract_indices_from_match, 0, returnAll)


def _load_test_functions():
    sys.modules.setdefault("main", sys.modules[__name__])
    import test_main

    return [
        (name, fn)
        for name, fn in vars(test_main).items()
        if name.startswith("test_") and callable(fn)
    ]


def run_tests():
    tests = sorted(_load_test_functions(), key=lambda item: item[0])
    failures = []

    for name, fn in tests:
        try:
            fn()
        except Exception as exc:
            failures.append((name, exc))

    passed = len(tests) - len(failures)
    print(f"{passed}/{len(tests)} tests passed")
    for name, exc in failures:
        print(f"FAIL {name}: {exc}")

    return not failures


if __name__ == "__main__":
    ok = run_tests()
    raise SystemExit(0 if ok else 1)

#     print("\n=== Old path (regex + AST backtracking) vs New path (capturing groups) ===")
#     print(f"  {'N':>6}  {'new (s)':>10}  {'old (s)':>10}  {'speedup':>8}  {'match?':>6}")
#     for N in [20, 50, 100, 200, 500, 1000, 2000]:
#         seq = ["a"] * N + ["b"]
#         expected = list(range(N + 1))

#         # --- new path first (always fast) ---
#         t0 = time.perf_counter()
#         new = _find_new_path(seq, "a+ b")
#         dt_new = time.perf_counter() - t0

#         # --- old path with timeout ---
#         old, dt_old, timed_out, err = _run_with_timeout(
#             lambda s=seq: _find_old_path(s, "a+ b"), TIMEOUT
#         )

#         if timed_out:
#             speedup_str = "  >timeout"
#             same_str = "skip"
#             dt_old_str = f">{TIMEOUT:.1f}"
#         elif err is not None:
#             speedup_str = "  crashed"
#             same_str = type(err).__name__
#             dt_old_str = f"{dt_old:.4f}"
#         else:
#             speedup = dt_old / dt_new if dt_new > 0 else float("inf")
#             speedup_str = f"{speedup:7.1f}x"
#             same_str = "OK" if old == new == expected else "FAIL"
#             dt_old_str = f"{dt_old:.4f}"

#         print(f"  {N:6d}  {dt_new:10.4f}  {dt_old_str:>10}  {speedup_str:>8}  {same_str:>6}")
#         assert new == expected, f"New path wrong at N={N}"


if __name__ == "__main__":
    import sys, types
    # Collect and run all test_* functions defined in this module
    module = sys.modules[__name__]
    tests = [(name, fn) for name, fn in vars(module).items()
             if name.startswith("test_") and callable(fn)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  PASS  {name}")
        except Exception as exc:
            print(f"  FAIL  {name}: {exc}")
            failed += 1
    print(f"\n{len(tests) - failed}/{len(tests)} tests passed.")
