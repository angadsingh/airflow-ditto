import copy
import math
import unittest
from datetime import timedelta
from typing import List

import networkx as nx
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.file_to_wasb import FileToWasbOperator
from airflow.contrib.operators.s3_copy_object_operator import S3CopyObjectOperator
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import BaseOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from ditto import rendering
from airflowlivy.operators.livy_batch_operator import LivyBatchOperator
from airflowlivy.sensors.livy_batch_sensor import LivyBatchSensor
from tests.test_commons import DEFAULT_DAG_ARGS
from tests.test_utils import TestUtils
from ditto.utils import TransformerUtils
from ditto.transformers import CopyTransformer

copy_op = TestUtils.copy_op
from ditto.api import OperatorTransformer, DAGFragment, SubDagTransformer, TaskMatcher
from ditto.ditto import AirflowDagTransformer
from ditto.matchers import PythonCallTaskMatcher
from ditto.matchers import ClassTaskMatcher
from ditto.resolvers import ClassTransformerResolver


class TestTransformer1(OperatorTransformer):
    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment,
                  upstream_fragments: List[DAGFragment]) -> DAGFragment:
        TestTransformer1.tp1 = LivyBatchOperator(
            name="foo",
            file="foo",
            arguments=["foo"],
            class_name="foo",
            azure_conn_id=src_operator.file_path,
            cluster_name="foo",
            proxy_user="foo",
            conf=None,
            task_id='tp1',
            dag=self.dag
        )

        TestTransformer1.tp2 = PythonOperator(task_id='tp2', python_callable=print, dag=self.dag)
        TestTransformer1.tp3 = DummyOperator(task_id='tp3', dag=self.dag)
        TestTransformer1.tp4 = DummyOperator(task_id='tp4', dag=self.dag)
        TestTransformer1.tp5 = DummyOperator(task_id='tp5', dag=self.dag)

        TestTransformer1.tp1 >> [TestTransformer1.tp2, TestTransformer1.tp3] >> TestTransformer1.tp4

        return DAGFragment([TestTransformer1.tp1, TestTransformer1.tp5])


class TestTransformer2(OperatorTransformer):
    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment,
                  upstream_fragments: List[DAGFragment]) -> DAGFragment:
        TestTransformer2.tp1 = LivyBatchSensor(
            batch_id="foo",
            task_id = "t2p1",
            azure_conn_id="foo",
            cluster_name=src_operator.dest_bucket_key,
            verify_in="yarn",
            dag=self.dag
        )

        TestTransformer2.tp2 = DummyOperator(task_id='t2p2', dag=self.dag)
        TestTransformer2.tp3 = DummyOperator(task_id='t2p3', dag=self.dag)
        TestTransformer2.tp4 = DummyOperator(task_id='t2p4', dag=self.dag)
        TestTransformer2.tp5 = PythonOperator(task_id='t2p5', python_callable=print, dag=self.dag)

        TestTransformer2.tp1 >> [TestTransformer2.tp2, TestTransformer2.tp3] >> TestTransformer2.tp4

        return DAGFragment([TestTransformer2.tp1, TestTransformer2.tp5])


class TestTransformer3(CopyTransformer):
    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment,
                  upstream_fragments: List[DAGFragment]) -> DAGFragment:
        TestTransformer3.livy_batch_op = TransformerUtils.find_op_in_fragment_list_strict(
            upstream_fragments,
            operator_type=LivyBatchOperator
        )
        return super(TestTransformer3, self).transform(src_operator, parent_fragment, upstream_fragments)


class TestTransformer4(CopyTransformer):
    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment,
                  upstream_fragments: List[DAGFragment]) -> DAGFragment:
        TestTransformer4.livy_sensor_op = TransformerUtils.find_op_in_parent_fragment_chain(
            parent_fragment,
            operator_type=LivyBatchSensor
        )
        return super(TestTransformer4, self).transform(src_operator, parent_fragment, upstream_fragments)

class TestTransformer5(OperatorTransformer):
    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment,
                  upstream_fragments: List[DAGFragment]) -> DAGFragment:
        op = LivyBatchSensor(
            batch_id="foo",
            task_id=src_operator.task_id,
            azure_conn_id="foo",
            cluster_name="foo",
            verify_in="yarn",
            dag=self.dag
        )
        return DAGFragment([op])

class TestSubDagTransformer1(SubDagTransformer):
    def get_sub_dag_matcher(self) -> TaskMatcher:
        op2_6_matcher = PythonCallTaskMatcher(math.pow)
        op3_7_matcher = PythonCallTaskMatcher(print)
        op4_8_matcher = PythonCallTaskMatcher(math.factorial)

        [op2_6_matcher, op3_7_matcher] >> op4_8_matcher

        return [op2_6_matcher, op3_7_matcher]

    def transform(self, subdag: nx.DiGraph, parent_fragment: DAGFragment) -> DAGFragment:
        subdag_roots = [n for n, d in subdag.in_degree() if d == 0]
        first_root = subdag_roots[0].task_id

        task_id_prefix = '' if first_root in ['op2', 'op3'] else '2'

        TestSubDagTransformer1.op1 = SparkSubmitOperator(task_id=f"t{task_id_prefix}p1", dag=self.dag)
        TestSubDagTransformer1.op2 = EmrAddStepsOperator(task_id=f"t{task_id_prefix}p2", job_flow_id='foo', dag=self.dag)
        TestSubDagTransformer1.op3 = S3ListOperator(task_id=f"t{task_id_prefix}p3", bucket='foo', dag=self.dag)
        TestSubDagTransformer1.op4 = EmrCreateJobFlowOperator(task_id=f"t{task_id_prefix}p4", dag=self.dag)
        TestSubDagTransformer1.op5 = DummyOperator(task_id=f"t{task_id_prefix}p5", dag=self.dag)

        TestSubDagTransformer1.op1 >> [TestSubDagTransformer1.op2, TestSubDagTransformer1.op3] >> TestSubDagTransformer1.op4

        return DAGFragment([TestSubDagTransformer1.op1, TestSubDagTransformer1.op5])

class TestSubDagTransformer2(SubDagTransformer):
    def get_sub_dag_matcher(self) -> TaskMatcher:
        op5_matcher = ClassTaskMatcher(DummyOperator)
        op6_matcher = PythonCallTaskMatcher(math.pow)
        op7_matcher = PythonCallTaskMatcher(print)
        op8_matcher = PythonCallTaskMatcher(math.factorial)
        op9_matcher = ClassTaskMatcher(DummyOperator)

        op5_matcher >> [op6_matcher, op7_matcher] >> op8_matcher
        [op6_matcher, op8_matcher] >> op9_matcher

        return [op5_matcher]

    def transform(self, subdag: nx.DiGraph, parent_fragment: DAGFragment) -> DAGFragment:
        TestSubDagTransformer2.tp1 = PythonOperator(python_callable=exec, task_id='t2p1', dag=self.dag)
        return DAGFragment([TestSubDagTransformer2.tp1])

class TestDagTransformer(unittest.TestCase):
    def setUp(self) -> None:
        self.show_graphs = False

    def _get_test_dag(self):
        with DAG(dag_id='test_dag', default_args=DEFAULT_DAG_ARGS) as dag:
            op1 = SparkSubmitOperator(task_id='op1')
            op2 = EmrAddStepsOperator(task_id='op2', job_flow_id='foo')
            op3 = S3ListOperator(task_id='op3', bucket='foo')
            op4 = EmrCreateJobFlowOperator(task_id='op4')
            op5 = TriggerDagRunOperator(task_id='op5', trigger_dag_id='foo')
            op6 = FileToWasbOperator(task_id='op6', container_name='foo', blob_name='foo', file_path='foo')
            op7 = EmailOperator(task_id='op7', subject='foo', to='foo', html_content='foo')
            op8 = S3CopyObjectOperator(task_id='op8', dest_bucket_key='foo', source_bucket_key='foo')
            op9 = BranchPythonOperator(task_id='op9', python_callable=print)
            op10 = PythonOperator(task_id='op10', python_callable=range)

            op1 >> [op2, op3, op4]
            op2 >> [op5, op6]
            op6 >> [op7, op8, op9]
            op3 >> [op7, op8]
            op8 >> [op9, op10]

        return dag

    def test_transform_operators_single_subdag(self):
        """
        tests:
            transformer transforms operator to subdag between a dag
        """
        dag = self._get_test_dag()

        transformer = AirflowDagTransformer(DAG(
            dag_id='transformed_dag',
            default_args=DEFAULT_DAG_ARGS,
            dagrun_timeout=timedelta(hours=2),
            max_active_runs=1,
            schedule_interval=None
        ), transformer_resolvers=[ClassTransformerResolver(
            {FileToWasbOperator: TestTransformer1}
        )])

        transformer.transform_operators(dag)

        with DAG(dag_id='expected_dag', default_args=DEFAULT_DAG_ARGS) as exp_dag:
            op1 = copy_op(dag.task_dict['op1'])
            op2 = copy_op(dag.task_dict['op2'])
            op3 = copy_op(dag.task_dict['op3'])
            op4 = copy_op(dag.task_dict['op4'])
            op5 = copy_op(dag.task_dict['op5'])
            op7 = copy_op(dag.task_dict['op7'])
            op8 = copy_op(dag.task_dict['op8'])
            op9 = copy_op(dag.task_dict['op9'])
            op10 = copy_op(dag.task_dict['op10'])
            tp1 = copy_op(TestTransformer1.tp1)
            tp2 = copy_op(TestTransformer1.tp2)
            tp3 = copy_op(TestTransformer1.tp3)
            tp4 = copy_op(TestTransformer1.tp4)
            tp5 = copy_op(TestTransformer1.tp5)

            op1 >> [op2, op3, op4]
            op2 >> [op5, tp1, tp5]
            tp1 >> [tp2, tp3] >> tp4
            tp4 >> [op7, op8, op9]
            tp5 >> [op7, op8, op9]
            op3 >> [op7, op8]
            op8 >> [op9, op10]

        if self.show_graphs:
            rendering.show_multi_dag_graphviz([dag, exp_dag, transformer.target_dag])

        TestUtils.assert_dags_equals(self, exp_dag, transformer.target_dag)

    def test_transform_operators_multi_subdag(self):
        """
            tests:
                connected task's transformers returning multiple subdags
                finding a task in a parent chain of multiple transformed subdags
                finding a task in upstream chain of multiple transformed subdags
        """
        dag = self._get_test_dag()

        transformer = AirflowDagTransformer(DAG(
            dag_id='transformed_dag',
            default_args=DEFAULT_DAG_ARGS,
            dagrun_timeout=timedelta(hours=2),
            max_active_runs=1,
            schedule_interval=None
        ), transformer_resolvers=[ClassTransformerResolver(
            {FileToWasbOperator: TestTransformer1,
             S3CopyObjectOperator: TestTransformer2,
             BranchPythonOperator: TestTransformer3,
             PythonOperator: TestTransformer4}
        )])

        transformer.transform_operators(dag)

        with DAG(dag_id='expected_dag', default_args=DEFAULT_DAG_ARGS) as exp_dag:
            op1 = copy_op(dag.task_dict['op1'])
            op2 = copy_op(dag.task_dict['op2'])
            op3 = copy_op(dag.task_dict['op3'])
            op4 = copy_op(dag.task_dict['op4'])
            op5 = copy_op(dag.task_dict['op5'])
            op7 = copy_op(dag.task_dict['op7'])
            op9 = copy_op(dag.task_dict['op9'])
            op10 = copy_op(dag.task_dict['op10'])
            tp1 = copy_op(TestTransformer1.tp1)
            tp2 = copy_op(TestTransformer1.tp2)
            tp3 = copy_op(TestTransformer1.tp3)
            tp4 = copy_op(TestTransformer1.tp4)
            tp5 = copy_op(TestTransformer1.tp5)
            t2p1 = copy_op(TestTransformer2.tp1)
            t2p2 = copy_op(TestTransformer2.tp2)
            t2p3 = copy_op(TestTransformer2.tp3)
            t2p4 = copy_op(TestTransformer2.tp4)
            t2p5 = copy_op(TestTransformer2.tp5)

            op1 >> [op2, op3, op4]
            op2 >> [op5, tp1, tp5]
            tp1 >> [tp2, tp3] >> tp4
            t2p1 >> [t2p2, t2p3] >> t2p4
            tp4 >> [op7, t2p1, t2p5, op9]
            tp5 >> [op7, t2p1, t2p5, op9]
            op3 >> [op7, t2p1, t2p5]
            [t2p4, t2p5] >> op9
            [t2p4, t2p5] >> op10

        if self.show_graphs:
            rendering.show_multi_dag_graphviz([dag, exp_dag, transformer.target_dag])

        TestUtils.assert_dags_equals(self, exp_dag, transformer.target_dag)
        self.assertEqual(TestTransformer3.livy_batch_op, tp1)
        self.assertEqual(TestTransformer4.livy_sensor_op, t2p1)

    def _get_expected_dag_sub_dags_match_multi(self, dag, tp1_op):
        with DAG(dag_id='expected_dag', default_args=DEFAULT_DAG_ARGS) as exp_dag:
            op1 = copy_op(dag.task_dict['op1'])
            op5 = copy_op(dag.task_dict['op5'])
            op9 = copy_op(dag.task_dict['op9'])

            tp1 = copy_op(tp1_op, task_id='tp1')
            tp2 = copy_op(TestSubDagTransformer1.op2, task_id='tp2')
            tp3 = copy_op(TestSubDagTransformer1.op3, task_id='tp3')
            tp4 = copy_op(TestSubDagTransformer1.op4, task_id='tp4')
            tp5 = copy_op(TestSubDagTransformer1.op5, task_id='tp5')

            tp1 >> [tp2, tp3] >> tp4

            op1 >> [tp1, tp5]
            [tp4, tp5] >> op5
            [tp4, tp5] >> op9

            t2p1 = copy_op(tp1_op, task_id='t2p1')
            t2p2 = copy_op(TestSubDagTransformer1.op2, task_id='t2p2')
            t2p3 = copy_op(TestSubDagTransformer1.op3, task_id='t2p3')
            t2p4 = copy_op(TestSubDagTransformer1.op4, task_id='t2p4')
            t2p5 = copy_op(TestSubDagTransformer1.op5, task_id='t2p5')

            t2p1 >> [t2p2, t2p3] >> t2p4
            op5 >> [t2p1, t2p5]
            [t2p4, t2p5] >> op9

        return exp_dag

    def _get_subdag_test_dag(self):
        with DAG(dag_id='test_dag', default_args=DEFAULT_DAG_ARGS) as dag:
            def fn0():
                math.pow(1, 2)

            def fn1():
                print("hi")

            def fn2():
                math.factorial(1)

            op1 = BranchPythonOperator(task_id='op1', python_callable=range)
            op2 = PythonOperator(task_id='op2', python_callable=fn0)
            op3 = PythonOperator(task_id='op3', python_callable=fn1)
            op4 = PythonOperator(task_id='op4', python_callable=fn2)
            op5 = DummyOperator(task_id='op5')
            op6 = PythonOperator(task_id='op6', python_callable=fn0)
            op7 = PythonOperator(task_id='op7', python_callable=fn1)
            op8 = PythonOperator(task_id='op8', python_callable=fn2)
            op9 = DummyOperator(task_id='op9')

            op1 >> [op2, op3] >> op4
            op2 >> op5 >> [op6, op7] >> op8
            [op4, op6, op8] >> op9

        return dag

    def test_transform_sub_dags_match_multi(self):
        """
            tests:
                finding multiple matching sub-dags and transforming them
                converting a sub-dag to another transformed sub-dag (with multiple roots)
                finding a sub-dag which isn't at the root
                returned sub-dag contains tasks which can be transformed
        """
        dag = self._get_subdag_test_dag()

        transformer = AirflowDagTransformer(DAG(
            dag_id='transformed_dag',
            default_args=DEFAULT_DAG_ARGS,
            dagrun_timeout=timedelta(hours=2),
            max_active_runs=1,
            schedule_interval=None
        ), subdag_transformers=[TestSubDagTransformer1],
        transformer_resolvers=[ClassTransformerResolver(
            {SparkSubmitOperator: TestTransformer5}
        )])

        src_dag = copy.deepcopy(dag)
        src_dag.dag_id='transformed_dag'
        transformer.transform_sub_dags(src_dag)

        exp_dag = self._get_expected_dag_sub_dags_match_multi(dag, TestSubDagTransformer1.op1)

        if self.show_graphs:
            rendering.show_multi_dag_graphviz([dag, exp_dag, src_dag])

        TestUtils.assert_dags_equals(self, exp_dag, src_dag)

        # transform operators in the transformed subdags
        transformer.transform_operators(src_dag)
        exp_dag = self._get_expected_dag_sub_dags_match_multi(dag, LivyBatchSensor(
            batch_id="foo",
            task_id="foo",
            azure_conn_id="foo",
            cluster_name="foo",
            verify_in="yarn",
            dag=src_dag
        ))

        if self.show_graphs:
            rendering.show_multi_dag_graphviz([dag, exp_dag, transformer.target_dag])
        TestUtils.assert_dags_equals(self, exp_dag, transformer.target_dag)

    def test_mutiple_sub_dag_transformers(self):
        """
            tests:
                multiple sub-dag transformers matching overlapping subdags
                order of the transformers decides result
        """
        dag = self._get_subdag_test_dag()

        transformer = AirflowDagTransformer(DAG(
            dag_id='transformed_dag',
            default_args=DEFAULT_DAG_ARGS,
            dagrun_timeout=timedelta(hours=2),
            max_active_runs=1,
            schedule_interval=None
        ), subdag_transformers=[TestSubDagTransformer2, TestSubDagTransformer1])

        src_dag = copy.deepcopy(dag)
        src_dag.dag_id = "transformed_dag"
        transformer.transform_sub_dags(src_dag)

        with DAG(dag_id='expected_dag', default_args=DEFAULT_DAG_ARGS) as exp_dag:
            op1 = copy_op(dag.task_dict['op1'])
            t2p1 = copy_op(TestSubDagTransformer2.tp1)

            tp1 = copy_op(TestSubDagTransformer1.op1, task_id='tp1')
            tp2 = copy_op(TestSubDagTransformer1.op2, task_id='tp2')
            tp3 = copy_op(TestSubDagTransformer1.op3, task_id='tp3')
            tp4 = copy_op(TestSubDagTransformer1.op4, task_id='tp4')
            tp5 = copy_op(TestSubDagTransformer1.op5, task_id='tp5')

            tp1 >> [tp2, tp3] >> tp4

            op1 >> [tp1, tp5]
            [tp4, tp5] >> t2p1

        if self.show_graphs:
            rendering.show_multi_dag_graphviz([dag, exp_dag, src_dag])

        TestUtils.assert_dags_equals(self, exp_dag, src_dag)