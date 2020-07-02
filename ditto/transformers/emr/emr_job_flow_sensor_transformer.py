from typing import List

from airflow import DAG
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.models import BaseOperator

from ditto.api import OperatorTransformer, TransformerDefaults, DAGFragment, UpstreamOperatorNotFoundException
from ditto.utils import TransformerUtils
from airflowhdi.operators import ConnectedAzureHDInsightCreateClusterOperator
from airflowhdi.sensors import AzureHDInsightClusterSensor


class EmrJobFlowSensorTransformer(OperatorTransformer[EmrJobFlowSensor]):
    """
    Transforms the sensor :class:`~airflow.contrib.operators.emr_job_flow_sensor.EmrJobFlowSensor`
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

        Creates a :class:`~airflowhdi.sensors.AzureHDInsightClusterSensor` in non-provisioning mode
        to monitor the cluster till it reaches a terminal state (cluster shutdown by user or failed).

        .. warning::

            We do not have a way to tell the HDInsight cluster to halt if a job has failed, unlike EMR.
            So the cluster will continue to run even on job failure. You have to add a terminate cluster
            operator on step failure through ditto itself.
        """
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

        self.copy_op_attrs(monitor_cluster_op, src_operator)
        self.sign_op(monitor_cluster_op)

        return DAGFragment([monitor_cluster_op])