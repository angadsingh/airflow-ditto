from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import trigger_rule
from airflow.utils.trigger_rule import TriggerRule

from airflowhdi.operators.azure_hdinsight_create_cluster_operator import ConnectedAzureHDInsightCreateClusterOperator
from airflowhdi.operators.azure_hdinsight_delete_cluster_operator import AzureHDInsightDeleteClusterOperator
from airflowlivy.operators.livy_batch_operator import LivyBatchOperator
from airflowhdi.sensors.azure_hdinsight_cluster_sensor import AzureHDInsightClusterSensor
from airflowlivy.sensors.livy_batch_sensor import LivyBatchSensor
from tests.test_dags.simple_dag.emr_dag import handle_failure_task
from tests.test_commons import *


def create_dag():
    with DAG(dag_id='HDI_emr_job_flow_manual_steps_dag',
             default_args=DEFAULT_DAG_ARGS,
             dagrun_timeout=timedelta(hours=2),
             max_active_runs=1,
             schedule_interval=None) as dag:
        create_cluster1_op = ConnectedAzureHDInsightCreateClusterOperator(task_id="create_cluster_1",
                                                                         azure_conn_id=AZURE_CONN_ID,
                                                                         hdi_conn_id=HDI_CONN_ID,
                                                                         cluster_name='PiCalc')

        create_cluster2_op = ConnectedAzureHDInsightCreateClusterOperator(task_id="create_cluster_2",
                                                                          azure_conn_id=AZURE_CONN_ID,
                                                                          hdi_conn_id=HDI_CONN_ID,
                                                                          cluster_name='PiCalc2')

        monitor_prov_op_1 = AzureHDInsightClusterSensor(create_cluster1_op.cluster_name,
                                                      azure_conn_id=create_cluster1_op.azure_conn_id,
                                                      poke_interval=5,
                                                      provisioning_only=True,
                                                      task_id=f"{create_cluster1_op.task_id}_monitor_provisioning")

        monitor_prov_op_2 = AzureHDInsightClusterSensor(create_cluster2_op.cluster_name,
                                                        azure_conn_id=create_cluster2_op.azure_conn_id,
                                                        poke_interval=5,
                                                        provisioning_only=True,
                                                        task_id=f"{create_cluster2_op.task_id}_monitor_provisioning")

        monitor_cluster1_op = AzureHDInsightClusterSensor(create_cluster1_op.cluster_name,
                                                         azure_conn_id=create_cluster1_op.azure_conn_id,
                                                         poke_interval=5,
                                                         task_id=f"{create_cluster1_op.task_id}_monitor_cluster")

        monitor_cluster2_op = AzureHDInsightClusterSensor(create_cluster2_op.cluster_name,
                                                          azure_conn_id=create_cluster2_op.azure_conn_id,
                                                          poke_interval=5,
                                                          task_id=f"{create_cluster2_op.task_id}_monitor_cluster")

        livy_submit_cluster1_step1_op = LivyBatchOperator(
            name='calculate_pi',
            file='s3://psm-poc-dmp-temp/spark-examples.jar',
            arguments=['10'],
            class_name='org.apache.spark.examples.SparkPi',
            azure_conn_id=create_cluster1_op.azure_conn_id,
            cluster_name=create_cluster1_op.cluster_name,
            proxy_user='admin',
            conf={'spark.shuffle.compress': 'false'},
            task_id="add_steps_cluster_1_0",
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        livy_submit_cluster2_step1_op = LivyBatchOperator(
            name='calculate_pi',
            file='s3://psm-poc-dmp-temp/spark-examples.jar',
            arguments=['10'],
            class_name='org.apache.spark.examples.SparkPi',
            azure_conn_id=create_cluster2_op.azure_conn_id,
            cluster_name=create_cluster2_op.cluster_name,
            proxy_user='admin',
            conf={'spark.shuffle.compress': 'false'},
            task_id="add_steps_cluster_2_0",
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        livy_submit_cluster1_step2_op = LivyBatchOperator(
            name='calculate_pi_2',
            file='s3://psm-poc-dmp-temp/spark-examples.jar',
            arguments=['10'],
            class_name='org.apache.spark.examples.SparkPi2',
            azure_conn_id=create_cluster1_op.azure_conn_id,
            cluster_name=create_cluster1_op.cluster_name,
            proxy_user='admin',
            conf={'spark.shuffle.compress': 'false'},
            task_id="add_steps_cluster_1_1",
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        livy_submit_cluster2_step2_op = LivyBatchOperator(
            name='calculate_pi_2',
            file='s3://psm-poc-dmp-temp/spark-examples.jar',
            arguments=['10'],
            class_name='org.apache.spark.examples.SparkPi2',
            azure_conn_id=create_cluster2_op.azure_conn_id,
            cluster_name=create_cluster2_op.cluster_name,
            proxy_user='admin',
            conf={'spark.shuffle.compress': 'false'},
            task_id="add_steps_cluster_2_1",
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        livy_sensor_cluster1_step1_op = LivyBatchSensor(
            batch_id=f"{{{{ task_instance.xcom_pull('add_steps_cluster_1_0', key='return_value') }}}}",
            task_id='watch_step_cluster1_step0',
            azure_conn_id=create_cluster1_op.azure_conn_id,
            cluster_name=create_cluster1_op.cluster_name,
            verify_in="yarn"
        )

        livy_sensor_cluster1_step2_op = LivyBatchSensor(
            batch_id=f"{{{{ task_instance.xcom_pull('add_steps_cluster_1_1', key='return_value') }}}}",
            task_id='watch_step_cluster1_step1',
            azure_conn_id=create_cluster1_op.azure_conn_id,
            cluster_name=create_cluster1_op.cluster_name,
            verify_in="yarn"
        )

        livy_sensor_cluster2_step1_op = LivyBatchSensor(
            batch_id=f"{{{{ task_instance.xcom_pull('add_steps_cluster_2_0', key='return_value') }}}}",
            task_id='watch_step_cluster2_step0',
            azure_conn_id=create_cluster2_op.azure_conn_id,
            cluster_name=create_cluster2_op.cluster_name,
            verify_in="yarn"
        )

        livy_sensor_cluster2_step2_op = LivyBatchSensor(
            batch_id=f"{{{{ task_instance.xcom_pull('add_steps_cluster_2_1', key='return_value') }}}}",
            task_id='watch_step_cluster2_step1',
            azure_conn_id=create_cluster2_op.azure_conn_id,
            cluster_name=create_cluster2_op.cluster_name,
            verify_in="yarn"
        )

        terminate_cluster1_op = AzureHDInsightDeleteClusterOperator(task_id="remove_cluster_1",
                                                                   azure_conn_id=create_cluster1_op.azure_conn_id,
                                                                   cluster_name=create_cluster1_op.cluster_name,
                                                                   trigger_rule=TriggerRule.ALL_DONE)

        terminate_cluster2_op = AzureHDInsightDeleteClusterOperator(task_id="remove_cluster_2",
                                                                    azure_conn_id=create_cluster2_op.azure_conn_id,
                                                                    cluster_name=create_cluster2_op.cluster_name,
                                                                    trigger_rule=TriggerRule.ALL_DONE)

        handle_failure_op = PythonOperator(
            task_id='handle_failure',
            python_callable=handle_failure_task,
            trigger_rule=trigger_rule.TriggerRule.ONE_FAILED)

        clusters_created_op = DummyOperator(task_id='clusters_created')

        steps_added_op = DummyOperator(task_id='steps_added')
        steps_completed_op = DummyOperator(task_id='steps_completed')
        steps_cluster_1_added_op = DummyOperator(task_id='add_steps_cluster_1_added')
        steps_cluster_2_added_op = DummyOperator(task_id='add_steps_cluster_2_added')

        create_cluster1_op >> monitor_prov_op_1
        create_cluster2_op >> monitor_prov_op_2
        [monitor_prov_op_1, monitor_prov_op_2] >> clusters_created_op
        clusters_created_op >> [monitor_cluster1_op, monitor_cluster2_op] >> handle_failure_op
        clusters_created_op >> [livy_submit_cluster1_step1_op, livy_submit_cluster1_step2_op, livy_submit_cluster2_step1_op, livy_submit_cluster2_step2_op]
        [livy_submit_cluster1_step1_op, livy_submit_cluster1_step2_op] >> steps_cluster_1_added_op
        [livy_submit_cluster2_step1_op, livy_submit_cluster2_step2_op] >> steps_cluster_2_added_op
        [steps_cluster_1_added_op, steps_cluster_2_added_op] >> steps_added_op
        steps_added_op >> [livy_sensor_cluster1_step1_op, livy_sensor_cluster1_step2_op, livy_sensor_cluster2_step1_op, livy_sensor_cluster2_step2_op] >> steps_completed_op
        steps_completed_op >> [terminate_cluster1_op, terminate_cluster2_op]

    return dag