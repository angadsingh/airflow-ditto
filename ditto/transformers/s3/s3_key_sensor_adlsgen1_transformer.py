from typing import List

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.sensors import S3KeySensor

from ditto.api import OperatorTransformer, TransformerDefaults, DAGFragment, TransformerException
from airflowhdi.sensors import AzureDataLakeStorageGen1WebHdfsSensor


class S3KeySensorAdlsGen1OperatorTransformer(OperatorTransformer[S3KeySensor]):
    """
    Transforms a :class:`~airflow.operators.sensors.S3KeySensor`
    into a :class:`~airflowhdi.sensors.AzureDataLakeStorageGen1WebHdfsSensor`
    """
    def __init__(self, target_dag: DAG, defaults: TransformerDefaults):
        super().__init__(target_dag, defaults)

    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment]) -> DAGFragment:
        """
        You need to add the ``adls_conn_id`` to the source operator (or preferably DAG) for this to work.
        The ``glob_path`` for the ADLS sensor is coped from the ``bucket_key`` of the s3 sensor,
        so make sure that is templatized for changing between `s3://` and `adls://` paths using config
        """
        s3_key_sensor: S3KeySensor = src_operator
        adls_conn_id = s3_key_sensor.params.get('adls_conn_id', None)
        if not adls_conn_id:
            adls_conn_id = self.dag.params.get('adls_conn_id', None)

        if not adls_conn_id:
            raise TransformerException("Could not find adls_conn_id in operator or DAG params")

        adls_gen1_sensor_op = AzureDataLakeStorageGen1WebHdfsSensor(
            task_id=src_operator.task_id,
            azure_data_lake_conn_id=adls_conn_id,
            glob_path=s3_key_sensor.bucket_key,
            dag=self.dag
        )

        self.copy_op_attrs(adls_gen1_sensor_op, src_operator)
        self.sign_op(adls_gen1_sensor_op)

        return DAGFragment([adls_gen1_sensor_op])