from typing import List

import networkx as nx
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.operators.dummy_operator import DummyOperator

from cloud_utils.airflow import xcom_pull
from cloud_utils.emr import check_for_existing_emr_cluster
from ditto.api import SubDagTransformer, TaskMatcher, DAGFragment
from ditto.matchers import PythonCallTaskMatcher, ClassTaskMatcher
from ditto.transformers.emr import EmrCreateJobFlowOperatorTransformer
from ditto.utils import TransformerUtils


class CheckClusterSubDagTransformer(SubDagTransformer):
    """
    Transforms a create-if-not-exists subdag pattern
    commonly used in EMR airflow DAGs to the corresponding
    pattern for an Azure HDI airflow DAG
    """
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