from typing import List, Callable

import networkx as nx
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.models.taskinstance import TaskInstance
from ditto.api import SubDagTransformer, TaskMatcher, DAGFragment, TransformerDefaults
from ditto.matchers import PythonCallTaskMatcher, ClassTaskMatcher
from ditto.transformers.emr import EmrCreateJobFlowOperatorTransformer
from ditto.utils import TransformerUtils


class CheckClusterSubDagTransformer(SubDagTransformer):
    """
    Transforms a create-if-not-exists subdag pattern
    commonly used in EMR airflow DAGs to the corresponding
    pattern for an Azure HDI airflow DAG
    """
    def __init__(self, dag: DAG, defaults: TransformerDefaults):
        super().__init__(dag, defaults)
        #: the python callable which checks if the cluster exists.
        self.check_for_existing_emr_cluster = defaults.other_defaults['pycall_check_cluster']

    def get_sub_dag_matcher(self) -> List[TaskMatcher]:
        check_for_emr_cluster_op = PythonCallTaskMatcher(self.check_for_existing_emr_cluster)
        create_cluster_op = ClassTaskMatcher(EmrCreateJobFlowOperator)
        cluster_exists_op = ClassTaskMatcher(DummyOperator)
        get_cluster_id_op = PythonCallTaskMatcher(TaskInstance.xcom_pull)

        check_for_emr_cluster_op >> [create_cluster_op, cluster_exists_op]
        create_cluster_op >> get_cluster_id_op
        cluster_exists_op >> get_cluster_id_op

        return [check_for_emr_cluster_op]

    def transform(self, subdag: nx.DiGraph, parent_fragment: DAGFragment) -> DAGFragment:
        transformer = EmrCreateJobFlowOperatorTransformer(self.dag, self.defaults)
        return transformer.transform(
            TransformerUtils.find_matching_tasks(
                subdag, ClassTaskMatcher(EmrCreateJobFlowOperator))[0], parent_fragment)