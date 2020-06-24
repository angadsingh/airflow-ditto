from typing import List

from airflow import DAG
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.models import BaseOperator

from ditto.api import OperatorTransformer, TransformerDefaults, DAGFragment, UpstreamOperatorNotFoundException
from ditto.utils import TransformerUtils
from airflowhdi.operators import ConnectedAzureHDInsightCreateClusterOperator
from airflowhdi.sensors import AzureHDInsightClusterSensor


class EmrJobFlowSensorTransformer(OperatorTransformer[EmrJobFlowSensor]):
    def __init__(self, target_dag: DAG, defaults: TransformerDefaults):
        super().__init__(target_dag, defaults)

    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment]) -> DAGFragment:
        create_op_task_id = TransformerUtils.get_task_id_from_xcom_pull(src_operator.job_flow_id)
        create_op: BaseOperator = \
            TransformerUtils.find_op_in_fragment_list(
                upstream_fragments,
                operator_type=ConnectedAzureHDInsightCreateClusterOperator,
                task_id=create_op_task_id)

        if not create_op:
            raise UpstreamOperatorNotFoundException(ConnectedAzureHDInsightCreateClusterOperator,
                                                    EmrJobFlowSensor)

        monitor_cluster_op = AzureHDInsightClusterSensor(create_op.cluster_name,
                                                         azure_conn_id=create_op.azure_conn_id,
                                                         poke_interval=5,
                                                         task_id=f"{create_op.task_id}_monitor_cluster",
                                                         dag=self.dag)

        self.copy_op_params(monitor_cluster_op, src_operator)
        self.sign_op(monitor_cluster_op)

        return DAGFragment([monitor_cluster_op])