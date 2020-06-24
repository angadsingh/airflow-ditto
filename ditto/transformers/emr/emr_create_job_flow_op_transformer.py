from typing import List

from airflow import DAG, settings
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.models import Connection, BaseOperator

from ditto.api import OperatorTransformer, TransformerDefaults, DAGFragment
from airflowhdi.operators import AzureHDInsightCreateClusterOperator
from airflowhdi.sensors.azure_hdinsight_cluster_sensor import AzureHDInsightClusterSensor


class EmrCreateJobFlowOperatorTransformer(OperatorTransformer[EmrCreateJobFlowOperator]):
    def __init__(self, target_dag: DAG, defaults: TransformerDefaults):
        super().__init__(target_dag, defaults)

    def get_cluster_name(self, src_operator: EmrCreateJobFlowOperator):
        emr_conn_id = src_operator.emr_conn_id
        session = settings.Session()
        emr_conn = session.query(Connection).filter(Connection.conn_id == emr_conn_id).first()
        config = emr_conn.extra_dejson.copy()
        config.update(src_operator.job_flow_overrides)
        return config['Name']

    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment] = None) -> DAGFragment:
        create_cluster_op: AzureHDInsightCreateClusterOperator = self.get_default_op(src_operator)
        if create_cluster_op is None:
            raise Exception("This transformer needs a default output operator")
        create_cluster_op.dag = self.dag
        create_cluster_op.cluster_name = self.get_cluster_name(src_operator)
        self.sign_op(create_cluster_op)

        monitor_provisioning_op = AzureHDInsightClusterSensor(create_cluster_op.cluster_name,
                                                              azure_conn_id=create_cluster_op.azure_conn_id,
                                                              poke_interval=5,
                                                              provisioning_only=True,
                                                              task_id=f"{create_cluster_op.task_id}_monitor_provisioning",
                                                              dag=self.dag)
        self.copy_op_params(monitor_provisioning_op, src_operator)
        self.sign_op(monitor_provisioning_op)
        create_cluster_op.set_downstream(monitor_provisioning_op)
        return DAGFragment([create_cluster_op])