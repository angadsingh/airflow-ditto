from typing import List

from airflow import DAG, settings
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.models import Connection, BaseOperator

from ditto.api import OperatorTransformer, TransformerDefaults, DAGFragment
from airflowhdi.operators import AzureHDInsightCreateClusterOperator
from airflowhdi.sensors.azure_hdinsight_cluster_sensor import AzureHDInsightClusterSensor


class EmrCreateJobFlowOperatorTransformer(OperatorTransformer[EmrCreateJobFlowOperator]):
    """
    Transforms the operator :class:`~airflow.contrib.operators.emr_create_job_flow_operator.EmrCreateJobFlowOperator`
    """

    def __init__(self, target_dag: DAG, defaults: TransformerDefaults):
        super().__init__(target_dag, defaults)

    def get_cluster_name(self, src_operator: EmrCreateJobFlowOperator):
        """
        get the cluster name from the EMR job flow and it's connection
        """
        emr_conn_id = src_operator.emr_conn_id
        session = settings.Session()
        emr_conn = session.query(Connection).filter(Connection.conn_id == emr_conn_id).first()
        config = emr_conn.extra_dejson.copy()
        config.update(src_operator.job_flow_overrides)
        return config['Name']

    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment] = None) -> DAGFragment:
        """
        Copies the :class:`~airflowhdi.operators.AzureHDInsightCreateClusterOperator` from the given
        :paramref:`~ditto.api.TransformerDefaults.default_operator` provided and attaches it to the
        target DAG after transferring airflow attributes from the source
        :class:`~airflow.contrib.operators.emr_create_job_flow_operator.EmrCreateJobFlowOperator`

        Relies on the default op as it is not possible to translate an EMR cluster spec to an HDInsight
        cluster spec automatically, so it is best to accept that operator from the user itself

        What it does do though is attach a :class:`~airflowhdi.sensors.AzureHDInsightClusterSensor`
        to monitor the provisioning of this newly created cluster.

        Since the HDInsight management client is idempotent, it does not matter if the cluster already exists
        and the operator simply moves on if that is the case.
        """
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
        self.copy_op_attrs(monitor_provisioning_op, src_operator)
        self.sign_op(monitor_provisioning_op)
        create_cluster_op.set_downstream(monitor_provisioning_op)
        return DAGFragment([create_cluster_op])