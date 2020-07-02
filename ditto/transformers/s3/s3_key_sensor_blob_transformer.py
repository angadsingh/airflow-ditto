from typing import List

from airflow import DAG
from airflow.contrib.sensors.wasb_sensor import WasbPrefixSensor
from airflow.models import BaseOperator
from airflow.operators.sensors import S3KeySensor

from ditto.api import OperatorTransformer, TransformerDefaults, DAGFragment, TransformerException
from airflowhdi.sensors import WasbWildcardPrefixSensor


class S3KeySensorBlobOperatorTransformer(OperatorTransformer[S3KeySensor]):
    """
    Transforms a :class:`~airflow.operators.sensors.S3KeySensor`
    into either a :class:`~airflowhdi.sensors.WasbWildcardPrefixSensor`
    or a :class:`~airflow.contrib.sensors.wasb_sensor.WasbPrefixSensor`
    """
    def __init__(self, target_dag: DAG, defaults: TransformerDefaults):
        super().__init__(target_dag, defaults)

    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment]) -> DAGFragment:
        """
        You need to add the ``wasb_conn_id`` to the source operator (or preferably DAG) for this to work.
        The ``container_name`` and ``prefix`` for the blob based sensors are coped from the ``bucket_name``
        and ``bucket_key`` of the s3 sensor, so make sure they are templatized for changing between
        `s3://` and `wasb://` paths, etc. using config
        """
        s3_key_sensor: S3KeySensor = src_operator
        wasb_conn_id = s3_key_sensor.params.get('wasb_conn_id', None)
        if not wasb_conn_id:
            wasb_conn_id = self.dag.params.get('wasb_conn_id', None)

        if not wasb_conn_id:
            raise TransformerException("Could not find wasb_conn_id in operator or DAG params")

        if s3_key_sensor.wildcard_match:
            wasb_sensor_op = WasbWildcardPrefixSensor(
                task_id=src_operator.task_id,
                wasb_conn_id=wasb_conn_id,
                container_name=s3_key_sensor.bucket_name,
                wildcard_prefix=s3_key_sensor.bucket_key,
                dag=self.dag
            )
        else:
            wasb_sensor_op = WasbPrefixSensor(
                task_id=src_operator.task_id,
                wasb_conn_id=wasb_conn_id,
                container_name=s3_key_sensor.bucket_name,
                prefix=s3_key_sensor.bucket_key,
                dag=self.dag
            )

        self.copy_op_attrs(wasb_sensor_op, src_operator)
        self.sign_op(wasb_sensor_op)

        return DAGFragment([wasb_sensor_op])