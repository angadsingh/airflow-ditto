from typing import List

import networkx as nx
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.operators.dummy_operator import DummyOperator

from cloud_utils.airflow import xcom_pull

from ditto.api import SubDagTransformer, TaskMatcher, DAGFragment
from ditto.templates.emr_hdi_template import EmrHdiDagTransformerTemplate
from ditto.matchers import PythonCallTaskMatcher
from ditto.matchers import ClassTaskMatcher
from ditto.transformers.emr import EmrCreateJobFlowOperatorTransformer
from cloud_utils.emr import check_for_existing_emr_cluster
from ditto.utils import TransformerUtils


class CheckClusterSubDagTransformer(SubDagTransformer):
    def get_sub_dag_matcher(self) -> List[TaskMatcher]:
        check_for_emr_cluster_op = PythonCallTaskMatcher(check_for_existing_emr_cluster)
        create_cluster_op = ClassTaskMatcher(EmrCreateJobFlowOperator)
        cluster_exists_op = ClassTaskMatcher(DummyOperator)
        get_cluster_id_op = PythonCallTaskMatcher(xcom_pull)

        check_for_emr_cluster_op >> [create_cluster_op, cluster_exists_op]
        create_cluster_op >> get_cluster_id_op
        cluster_exists_op >> get_cluster_id_op

        return [check_for_emr_cluster_op]

    def transform(self, subdag: nx.DiGraph, parent_fragment: DAGFragment) -> DAGFragment:
        transformer = EmrCreateJobFlowOperatorTransformer(self.dag, self.defaults)
        return transformer.transform(
            TransformerUtils.find_matching_tasks(
                subdag, ClassTaskMatcher(EmrCreateJobFlowOperator))[0], parent_fragment)


class TdmpEmr2HdiDagTransformerTemplate(EmrHdiDagTransformerTemplate):
    def __init__(self, src_dag: DAG, *args, **kwargs):
        super().__init__(src_dag, subdag_transformers=[CheckClusterSubDagTransformer],
                         *args, **kwargs)
