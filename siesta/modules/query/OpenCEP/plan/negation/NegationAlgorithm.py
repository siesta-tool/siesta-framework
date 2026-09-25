from typing import List, Dict

from base.Pattern import Pattern
from base.PatternStructure import NegationOperator, AndOperator, SeqOperator, KleeneClosureOperator, \
    PrimitiveEventStructure
from plan.TreePlan import TreePlanBinaryNode, TreePlanLeafNode, TreePlanNode, OperatorTypes, TreePlanUnaryNode, \
    TreePlanNegativeBinaryNode, TreePlanNestedNode


class NegationAlgorithm:
    """
    The main class of the negation algorithms, contains the information and parameters needed in order to build the
    tree plan according to the chosen negation algorithm.
    """
    def handle_pattern_negation(self, pattern: Pattern, statistics: Dict, positive_tree_plan: TreePlanBinaryNode,
                                negative_index_to_tree_plan_node: Dict[int, TreePlanNode],
                                negative_index_to_tree_plan_cost: Dict[int, float]):
        """
        Augments the given tree plan, that only contains the positive portion of the given pattern, with the operators
        required for the negative part.
        The last 2 parameters are designed to support nested negation operators.
        negative_index_to_tree_plan_node: A dictionary that maps from the index of the negative node to its tree plan.
        negative_index_to_tree_plan_cost: A dictionary that maps from the index of the negative node to the cost of its
        tree plan.
        """
        if pattern.negative_structure is None:
            return positive_tree_plan
        positive_indices, negative_indices = self.__calculate_positive_and_negative_indices(pattern)
        unbounded_negative_indices = []
        if pattern.full_structure.get_top_operator() == SeqOperator:
            # a sequence can contain bounded negative events - those followed by a positive event in the sequence order
            for index in negative_indices:
                if len([positive_index for positive_index in positive_indices if positive_index > index]) == 0:
                    unbounded_negative_indices.append(index)
        else:
            # no sequence operator - all negative events are unbounded
            unbounded_negative_indices = negative_indices

        full_tree_plan = self._add_negative_part(pattern, statistics, positive_tree_plan,
                                                 negative_indices, unbounded_negative_indices,
                                                 negative_index_to_tree_plan_node,
                                                 negative_index_to_tree_plan_cost)

        self.__adjust_tree_plan_indices(full_tree_plan, pattern)
        return full_tree_plan

    def _add_negative_part(self, pattern: Pattern, statistics: Dict, positive_tree_plan: TreePlanBinaryNode,
                           all_negative_indices: List[int], unbounded_negative_indices: List[int],
                           negative_index_to_tree_plan_node: Dict[int, TreePlanNode],
                           negative_index_to_tree_plan_cost: Dict[int, float]) -> TreePlanNode:
        """
        Actually adds negation operators to the given tree plan.
        """
        raise NotImplementedError()

    @staticmethod
    def __calculate_positive_and_negative_indices(pattern: Pattern):
        """
        Partitions the event indices in a given flat pattern into positive and negative ones (containing events under
        negation).
        """
        positive_indices = []
        negative_indices = []
        for index, arg in enumerate(pattern.get_top_level_structure_args()):
            if type(arg) == NegationOperator:
                negative_indices.append(index)
            else:
                positive_indices.append(index)
        return positive_indices, negative_indices

    def __adjust_tree_plan_indices(self, tree_plan: TreePlanNode, pattern: Pattern, under_negation: bool = False):
        """
        A recursive function that traverses the tree plan (including both positive and negative parts) and adjusts
        the leaf indices to reflect the negative structure.
        Unary and nested nodes on the positive side carry indices in positive-only order as well, so they are
        remapped too; nodes under a negation were already given full-pattern indices by _add_negative_part.
        """
        positive_indices, negative_indices = self.__calculate_positive_and_negative_indices(pattern)
        if isinstance(tree_plan, TreePlanLeafNode):
            old_index = tree_plan.event_index
            if old_index >= len(positive_indices):
                tree_plan.original_event_index = tree_plan.event_index = negative_indices[old_index - len(positive_indices)]
            else:
                tree_plan.original_event_index = tree_plan.event_index = positive_indices[old_index]
            return
        if isinstance(tree_plan, TreePlanUnaryNode):
            if not under_negation and tree_plan.index is not None and tree_plan.index < len(positive_indices):
                tree_plan.index = positive_indices[tree_plan.index]
            self.__adjust_tree_plan_indices(tree_plan.child, pattern, under_negation)
            return
        if isinstance(tree_plan, TreePlanNestedNode):
            if not under_negation:
                tree_plan.nested_event_index = self.__to_full_index(tree_plan.nested_event_index,
                                                                    positive_indices, negative_indices)
            sub_pattern = self.__find_nested_sub_pattern(tree_plan, pattern)
            self.__adjust_tree_plan_indices(tree_plan.sub_tree_plan, sub_pattern)
            return
        if isinstance(tree_plan, TreePlanNegativeBinaryNode):
            self.__adjust_tree_plan_indices(tree_plan.left_child, pattern, under_negation)
            self.__adjust_tree_plan_indices(tree_plan.right_child, pattern, True)
            return
        if isinstance(tree_plan, TreePlanBinaryNode):
            self.__adjust_tree_plan_indices(tree_plan.left_child, pattern, under_negation)
            self.__adjust_tree_plan_indices(tree_plan.right_child, pattern, under_negation)
            return
        raise Exception("Unexpected tree plan node")

    @staticmethod
    def __to_full_index(index: int, positive_indices: List[int], negative_indices: List[int]) -> int:
        """
        Converts an index in positive-first order (positive args, then negative args) into a full-pattern index.
        """
        if index >= len(positive_indices):
            return negative_indices[index - len(positive_indices)]
        return positive_indices[index]

    @staticmethod
    def __find_nested_sub_pattern(tree_plan: TreePlanNestedNode, pattern: Pattern) -> Pattern:
        """
        Returns a nested subpattern of the given pattern corresponding to the specified nested node of the tree plan.
        Expects tree_plan.nested_event_index to be a full-pattern index.
        """
        nested_pattern_structure = pattern.get_top_level_structure_args()[tree_plan.nested_event_index]
        if isinstance(nested_pattern_structure, NegationOperator):
            nested_pattern_structure = nested_pattern_structure.arg
        if isinstance(nested_pattern_structure, KleeneClosureOperator) \
                and not isinstance(nested_pattern_structure.arg, PrimitiveEventStructure):
            nested_pattern_structure = nested_pattern_structure.arg
        return Pattern(nested_pattern_structure, None, pattern.window)

    @staticmethod
    def _instantiate_negative_node(pattern: Pattern, positive_subtree: TreePlanNode, negative_subtree: TreePlanNode,
                                   is_unbounded: bool):
        """
        Creates a node representing a negation operator over the given positive and negative subtrees.
        """
        if isinstance(pattern.positive_structure, AndOperator):
            operator_type = OperatorTypes.NAND
        elif isinstance(pattern.positive_structure, SeqOperator):
            operator_type = OperatorTypes.NSEQ
        else:
            raise Exception("Unsupported operator for negation")
        return TreePlanNegativeBinaryNode(operator_type, positive_subtree, negative_subtree, is_unbounded)
