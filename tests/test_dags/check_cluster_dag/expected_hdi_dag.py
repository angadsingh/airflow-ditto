from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import trigger_rule
from airflow.utils.trigger_rule import TriggerRule

from airflowhdi.operators.azure_hdinsight_create_cluster_operator import ConnectedAzureHDInsightCreateClusterOperator
from airflowhdi.operators.azure_hdinsight_delete_cluster_operator import AzureHDInsightDeleteClusterOperator
from airflowhdi.operators import LivyBatchOperator
from airflowhdi.sensors.azure_hdinsight_cluster_sensor import AzureHDInsightClusterSensor
from airflowhdi.sensors.livy_batch_sensor import LivyBatchSensor
from tests.test_dags.simple_dag.emr_dag import handle_failure_task
from tests.test_commons import *


def create_dag():
    with DAG(dag_id='HDI_emr_job_flow_manual_steps_dag',
             default_args=DEFAULT_DAG_ARGS,
             dagrun_timeout=timedelta(hours=2),
             max_active_runs=1,
             schedule_interval=None) as dag:
        create_cluster_op = ConnectedAzureHDInsightCreateClusterOperator(task_id="create_cluster",
                                                                         azure_conn_id=AZURE_CONN_ID,
                                                                         hdi_conn_id=HDI_CONN_ID,
                                                                         cluster_name=CLUSTER_NAME,
                                                                         trigger_rule=TriggerRule.ALL_SUCCESS)

        monitor_prov_op = AzureHDInsightClusterSensor(create_cluster_op.cluster_name,
                                                      azure_conn_id=create_cluster_op.azure_conn_id,
                                                      poke_interval=5,
                                                      provisioning_only=True,
                                                      task_id=f"{create_cluster_op.task_id}_monitor_provisioning")

        monitor_cluster_op = AzureHDInsightClusterSensor(create_cluster_op.cluster_name,
                                                         azure_conn_id=create_cluster_op.azure_conn_id,
                                                         poke_interval=5,
                                                         task_id=f"{create_cluster_op.task_id}_monitor_cluster")

        livy_submit_op = LivyBatchOperator(
            name='calculate_pi',
            file='s3://psm-poc-dmp-temp/spark-examples.jar',
            arguments=['10'],
            class_name='org.apache.spark.examples.SparkPi',
            azure_conn_id=create_cluster_op.azure_conn_id,
            cluster_name=create_cluster_op.cluster_name,
            proxy_user='admin',
            conf={'spark.shuffle.compress': 'false'},
            task_id="add_steps_0",
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        livy_sensor = LivyBatchSensor(
            batch_id=f"{{{{ task_instance.xcom_pull('add_steps_0', key='return_value') }}}}",
            task_id='watch_step',
            azure_conn_id=create_cluster_op.azure_conn_id,
            cluster_name=create_cluster_op.cluster_name,
            verify_in="yarn"
        )

        terminate_cluster_op = AzureHDInsightDeleteClusterOperator(task_id="remove_cluster",
                                                                   azure_conn_id=create_cluster_op.azure_conn_id,
                                                                   cluster_name=create_cluster_op.cluster_name,
                                                                   trigger_rule=TriggerRule.ALL_DONE)

        handle_failure_op = PythonOperator(
            task_id='handle_failure',
            python_callable=handle_failure_task,
            trigger_rule=trigger_rule.TriggerRule.ONE_FAILED)

        steps_added_op = DummyOperator(
            task_id=f"add_steps_added")

        create_cluster_op >> monitor_prov_op >> monitor_cluster_op >> handle_failure_op
        monitor_prov_op >> livy_submit_op >> steps_added_op >> livy_sensor >> terminate_cluster_op
    return dag