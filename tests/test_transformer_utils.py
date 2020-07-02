import math
import unittest

import networkx as nx
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from parameterized import parameterized

from tests.test_commons import DEFAULT_DAG_ARGS
from ditto.api import DAGFragment
from ditto.matchers import PythonCallTaskMatcher
from ditto.matchers import ClassTaskMatcher
from ditto.utils import TransformerUtils


class TestTransformerUtils(unittest.TestCase):
    def test_add_to_dag_fragment(self):
        with DAG(dag_id='d1', default_args=DEFAULT_DAG_ARGS) as dag:
            op1 = DummyOperator(task_id='d1t1')
            op2 = DummyOperator(task_id='d1t2')
            op3 = DummyOperator(task_id='d1t3')
            op4 = DummyOperator(task_id='d1t4')
            op5 = DummyOperator(task_id='d1t5')
            op6 = DummyOperator(task_id='d1t6')

            op1 >> [op2, op3] >> op4
            op2 >> [op5, op6]

            down_op1 = DummyOperator(task_id='d2t1')
            down_op2 = DummyOperator(task_id='d2t2')
            down_op3 = DummyOperator(task_id='d2t3')
            down_op4 = DummyOperator(task_id='d2t4')
            down_op5 = DummyOperator(task_id='d2t5')

            [down_op1, down_op2] >> down_op3 >> [down_op4, down_op5]

        frag_up: DAGFragment = DAGFragment([op1])
        frag_down: DAGFragment = DAGFragment([down_op1, down_op2])

        TransformerUtils.add_downstream_dag_fragment(frag_up, frag_down)

        self.assertTrue(down_op1 in op4.downstream_list)
        self.assertTrue(down_op2 in op4.downstream_list)
        self.assertTrue(down_op1 in op5.downstream_list)
        self.assertTrue(down_op2 in op5.downstream_list)
        self.assertTrue(down_op1 in op6.downstream_list)
        self.assertTrue(down_op2 in op6.downstream_list)

    def test_find_op_in_dag_fragment(self):
        with DAG(dag_id='d1', default_args=DEFAULT_DAG_ARGS) as dag:
            op1 = DummyOperator(task_id='d1t1')
            op2 = DummyOperator(task_id='d1t2')
            op3 = PythonOperator(task_id='d1t3', python_callable=print)
            op4 = DummyOperator(task_id='d1t4')

            op1 >> [op2, op3] >> op4

            self.assertEqual(op3,
                             TransformerUtils.find_op_in_dag_fragment(
                                 DAGFragment([op1]), PythonOperator
                             ))

    @parameterized.expand([
        ('{{ ti.xcom_pull("add_steps_to_cluster", key="return_value")[2] }}', '2'),
        ('{{ task_instance.xcom_pull("add_steps_to_cluster", key="return_value")[3] }}', '3'),
        ("{{   task_instance.xcom_pull('add_steps_to_cluster', key='return_value')[4] }}", '4')
    ])
    def test_get_step_id_from_xcom_pull(self, test, result):
        self.assertEqual(result,
                         TransformerUtils.get_list_index_from_xcom_pull(test))

    @parameterized.expand([
        ('{{ ti.xcom_pull("add_steps_to_cluster", key="return_value")[0] }}',
         'add_steps_to_cluster'),
        ('{{ task_instance.xcom_pull("add_steps_to_cluster", key="return_value")[0] }}',
         'add_steps_to_cluster'),
        ("{{   task_instance.xcom_pull('add_steps_to_cluster', key='return_value')[0] }}",
         'add_steps_to_cluster'),
        ("{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
         'add_steps')
    ])
    def test_get_task_id_from_xcom_pull(self, test, result):
        self.assertEqual(result,
                         TransformerUtils.get_task_id_from_xcom_pull(test))

    def test_get_digraph_from_airflow_dag(self):
        with DAG(dag_id='d1', default_args=DEFAULT_DAG_ARGS) as dag:
            op1 = DummyOperator(task_id='op1')
            op2 = DummyOperator(task_id='op2')
            op3 = DummyOperator(task_id='op3')
            op4 = DummyOperator(task_id='op4')

            op1 >> [op2, op3] >> op4

        expected_dg = nx.DiGraph()
        expected_dg.add_edge(op1, op2)
        expected_dg.add_edge(op1, op3)
        expected_dg.add_edge(op2, op4)
        expected_dg.add_edge(op3, op4)

        actual_dg = TransformerUtils.get_digraph_from_airflow_dag(dag)
        diff_dg: nx.DiGraph = nx.symmetric_difference(expected_dg, actual_dg)
        self.assertEqual(len(diff_dg.edges), 0)

    def test_find_sub_dag(self):
        with DAG(dag_id='d1', default_args=DEFAULT_DAG_ARGS) as dag:
            def fn0():
                print("hi")

            def fn1():
                math.pow(1, 2)

            def fn2():
                math.factorial(1)

            op1 = DummyOperator(task_id='op1')
            op2 = DummyOperator(task_id='op2')
            op3 = PythonOperator(task_id='op3', python_callable=fn0)
            op4 = DummyOperator(task_id='op4')

            op1 >> [op2, op3] >> op4

            op5 = PythonOperator(task_id='op5', python_callable=fn1)
            op6 = PythonOperator(task_id='op6', python_callable=fn0)
            op7 = DummyOperator(task_id='op7')
            op8 = PythonOperator(task_id='op8', python_callable=fn2)

            op2 >> op5 >> [op6, op7] >> op8

        op1_matcher = ClassTaskMatcher(DummyOperator)
        op2_matcher = ClassTaskMatcher(DummyOperator)
        op3_matcher = PythonCallTaskMatcher(print)
        op4_matcher = ClassTaskMatcher(DummyOperator)

        op1_matcher >> [op2_matcher, op3_matcher] >> op4_matcher

        op5_matcher = PythonCallTaskMatcher(math.pow)
        op6_matcher = ClassTaskMatcher(PythonOperator)
        op7_matcher = ClassTaskMatcher(DummyOperator)
        op8_matcher = PythonCallTaskMatcher(math.factorial)

        op5_matcher >> [op6_matcher, op7_matcher] >> op8_matcher

        expected_sub_dag = nx.DiGraph([(op5, op6), (op5, op7), (op6, op8), (op7, op8)])
        dag_dg, found_sub_dags = TransformerUtils.find_sub_dag(dag, [op5_matcher])
        self.assertEqual(len(found_sub_dags), 1)
        diff_dg: nx.DiGraph = nx.symmetric_difference(expected_sub_dag, found_sub_dags[0])
        self.assertEqual(len(diff_dg.edges), 0)

        expected_sub_dag = nx.DiGraph([(op1, op2), (op1, op3), (op2, op4), (op3, op4)])
        dag_dg, found_sub_dags = TransformerUtils.find_sub_dag(dag, [op1_matcher])
        self.assertEqual(len(found_sub_dags), 1)
        diff_dg: nx.DiGraph = nx.symmetric_difference(expected_sub_dag, found_sub_dags[0])
        self.assertEqual(len(diff_dg.edges), 0)
