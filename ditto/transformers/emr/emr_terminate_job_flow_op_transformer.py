from typing import List

from airflow import DAG
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.models import BaseOperator
from airflow.utils.trigger_rule import TriggerRule

from ditto.api import OperatorTransformer, TransformerDefaults, DAGFragment, UpstreamOperatorNotFoundException
from ditto.utils import TransformerUtils
from airflowhdi.operators import ConnectedAzureHDInsightCreateClusterOperator
from airflowhdi.operators import AzureHDInsightDeleteClusterOperator


class EmrTerminateJobFlowOperatorTransformer(OperatorTransformer[EmrTerminateJobFlowOperator]):
    """
    Transforms the operator :class:`~airflow.contrib.operators.emr_terminate_job_flow_operator.EmrTerminateJobFlowOperator`
    """

    def __init__(self, target_dag: DAG, defaults: TransformerDefaults):
        super().__init__(target_dag, defaults)

    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment]) -> DAGFragment:
        """
        This transformer assumes and relies on the fact that an upstream transformation
        of a :class:`~airflow.contrib.operators.emr_create_job_flow_operator.EmrCreateJobFlowOperator`
        has already taken place, since it needs to find the output of that transformation
        to get the `cluster_name` and `azure_conn_id` from that operator (which should have been a
        :class:`~airflowhdi.operators.AzureHDInsightCreateClusterOperator`)

        Creates a :class:`~airflowhdi.operators.AzureHDInsightDeleteClusterOperator` to terminate
        the cluster
        """
        create_op_task_id = TransformerUtils.get_task_id_from_xcom_pull(src_operator.job_flow_id)
        create_op: BaseOperator = \
            TransformerUtils.find_op_in_fragment_list(
                upstream_fragments,
                operator_type=ConnectedAzureHDInsightCreateClusterOperator,
                task_id=create_op_task_id)

        if not create_op:
            raise UpstreamOperatorNotFoundException(ConnectedAzureHDInsightCreateClusterOperator,
                                                    EmrTerminateJobFlowOperator)

        emr_terminate_op: EmrTerminateJobFlowOperator = src_operator
        terminate_cluster_op = AzureHDInsightDeleteClusterOperator(task_id=emr_terminate_op.task_id,
                                                                   azure_conn_id=create_op.azure_conn_id,
                                                                   cluster_name=create_op.cluster_name,
                                                                   dag=self.dag)

        self.copy_op_attrs(terminate_cluster_op, src_operator)
        self.sign_op(terminate_cluster_op)
        terminate_cluster_op.trigger_rule = TriggerRule.ALL_DONE

        return DAGFragment([terminate_cluster_op])