import functools
import re
import unittest
from pprint import pprint

import copy
import deepdiff
import networkx as nx
from airflow import DAG, settings
from airflow.models import BaseOperator

from ditto.api import Transformer
from ditto.utils import TransformerUtils


class TestUtils:

    @staticmethod
    def assert_dags_equals(test_case: unittest.TestCase, expected_dag: DAG, actual_dag: DAG):
        expected_dg = TransformerUtils.get_digraph_from_airflow_dag(expected_dag)
        actual_dg = TransformerUtils.get_digraph_from_airflow_dag(actual_dag)

        test_case.assertEqual(len(expected_dg.nodes), len(actual_dg.nodes))

        def node_matcher(mismatches, n1, n2):
            i: BaseOperator = n1['op']
            j: BaseOperator = n2['op']
            diff = deepdiff.DeepDiff(i, j,
                                     exclude_paths=['root._dag',
                                                    'root.azure_hook.client'],
                                     exclude_regex_paths=[re.compile(".*hook.*conn.*"),
                                                          re.compile(".*{}.*".format(
                                                              Transformer.TRANSFORMED_BY_HEADER))],
                                     view='tree',
                                     verbose_level=10)
            if diff != {}:
                mismatches.append((i, j, diff))

            return diff == {}

        mismatches = []
        is_isomorphic = nx.is_isomorphic(expected_dg, actual_dg,
                                         node_match=functools.partial(node_matcher, mismatches))
        if not is_isomorphic:
            for i, j, diff in mismatches:
                print(f"Operators {i} and {j} are not equal")
                pprint(diff, indent=2)

        test_case.assertTrue(is_isomorphic)

    @staticmethod
    def copy_op(op: BaseOperator, task_id: str = None):
        copied_op = copy.deepcopy(op)
        copied_op._dag = None
        copied_op._upstream_task_ids.clear()
        copied_op._downstream_task_ids.clear()

        if task_id:
            copied_op.task_id = task_id
        copied_op.dag = settings.CONTEXT_MANAGER_DAG

        return copied_op
