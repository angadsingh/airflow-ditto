from typing import List

from airflow import DAG
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator

from ditto.api import OperatorTransformer, TransformerDefaults, DAGFragment, UpstreamOperatorNotFoundException
from ditto.utils import TransformerUtils
from ditto.transformers.emr import EmrAddStepsOperatorTransformer
from airflowhdi.operators import ConnectedAzureHDInsightCreateClusterOperator
from airflowhdi.operators import LivyBatchOperator
from airflowhdi.sensors.livy_batch_sensor import LivyBatchSensor


class EmrStepSensorTransformer(OperatorTransformer[EmrStepSensor]):
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
                                                    EmrStepSensor)

        emr_step_sensor_op: EmrStepSensor = src_operator

        emr_add_step_task_id = TransformerUtils.get_task_id_from_xcom_pull(emr_step_sensor_op.step_id)
        emr_add_step_step_id = TransformerUtils.get_step_id_from_xcom_pull(emr_step_sensor_op.step_id)
        target_step_task_id = EmrAddStepsOperatorTransformer.get_target_step_task_id(emr_add_step_task_id, emr_add_step_step_id)

        add_step_op: BaseOperator = \
            TransformerUtils.find_op_in_fragment_list_strict(
                upstream_fragments,
                task_id=target_step_task_id)

        if isinstance(add_step_op, LivyBatchOperator):
            step_sensor_op = LivyBatchSensor(
                batch_id=f"{{{{ task_instance.xcom_pull('{target_step_task_id}', key='return_value') }}}}",
                task_id = emr_step_sensor_op.task_id,
                azure_conn_id=create_op.azure_conn_id,
                cluster_name=create_op.cluster_name,
                verify_in="yarn",
                dag=self.dag
            )
        else:
            # don't need a sensor for the ssh operator
            step_sensor_op = DummyOperator(task_id=emr_step_sensor_op.task_id, dag=self.dag)

        self.copy_op_params(step_sensor_op, emr_step_sensor_op)
        self.sign_op(step_sensor_op)

        return DAGFragment([step_sensor_op])