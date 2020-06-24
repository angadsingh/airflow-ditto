import logging
from datetime import timedelta

import boto3
from airflow import DAG, AirflowException
from airflow.contrib.operators.emr_create_job_flow_operator \
    import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.contrib.operators.emr_add_steps_operator \
    import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator \
    import EmrTerminateJobFlowOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils import trigger_rule
from cloud_utils.airflow import xcom_pull
from cloud_utils.emr import check_for_existing_emr_cluster

from tests.test_commons import DEFAULT_DAG_ARGS, EMR_CLUSTER_ID, AWS_CONN_ID, EMR_CONN_ID, CLUSTER_NAME


def handle_failure_task():
    raise AirflowException('Marking DAG as failed due to an upstream failure!')


def check_for_cluster():
    """
    Checks if an existing cluster should be used for submitting steps.
    """
    emr_client = boto3.client('emr')
    return check_for_existing_emr_cluster(
        logger=logging.getLogger(), emr_client=emr_client, cluster_id=EMR_CLUSTER_ID)


def get_cluster_id(**kwargs):
    """
    Gets the cluster id, which can be either an existing cluster ready to
    accept steps, or a newly created cluster.
    """
    cluster_exists_id = EMR_CLUSTER_ID
    create_cluster_id = xcom_pull(
        logging.getLogger(), 'return_value', 'create_cluster', **kwargs)

    return create_cluster_id if create_cluster_id is not None \
        else cluster_exists_id


def create_dag():
    with DAG(
        dag_id='emr_job_flow_manual_steps_dag',
        default_args=DEFAULT_DAG_ARGS,
        dagrun_timeout=timedelta(hours=2),
        max_active_runs=1,
        schedule_interval=None
    ) as dag:

        create_cluster_op = EmrCreateJobFlowOperator(
            task_id='create_cluster',
            job_flow_overrides={'Name': CLUSTER_NAME},
            aws_conn_id=AWS_CONN_ID,
            emr_conn_id=EMR_CONN_ID
        )

        add_steps_to_cluster_op = EmrAddStepsOperator(
            task_id='add_steps',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='get_cluster_id', key='return_value') }}",
            aws_conn_id=AWS_CONN_ID,
            steps=[
                {
                    'Name': 'calculate_pi',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 's3://psm-poc-dmp-temp/spark-examples.jar',
                        'Args': [
                            '10'
                        ],
                        'MainClass': 'org.apache.spark.examples.SparkPi'
                    }
                }
            ]
        )

        monitor_cluster_op = EmrJobFlowSensor(
            task_id='monitor_cluster',
            retries=0,
            aws_conn_id=AWS_CONN_ID,
            job_flow_id='{{ task_instance.xcom_pull("get_cluster_id", key="return_value") }}',
            timeout=1800)

        monitor_step_op = EmrStepSensor(
            task_id='watch_step',
            job_flow_id="{{ task_instance.xcom_pull('get_cluster_id', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
            aws_conn_id=AWS_CONN_ID
        )

        terminate_cluster_op = EmrTerminateJobFlowOperator(
            task_id='remove_cluster',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='get_cluster_id', key='return_value') }}",
            aws_conn_id=AWS_CONN_ID
        )

        handle_failure_op = PythonOperator(
            task_id='handle_failure',
            python_callable=handle_failure_task,
            trigger_rule=trigger_rule.TriggerRule.ONE_FAILED)

        check_for_emr_cluster_op = BranchPythonOperator(
            task_id='check_for_cluster',
            provide_context=False,
            python_callable=check_for_cluster)

        cluster_exists_op = DummyOperator(
            task_id='cluster_exists')

        get_cluster_id_op = PythonOperator(
            task_id='get_cluster_id',
            trigger_rule=trigger_rule.TriggerRule.ONE_SUCCESS,
            python_callable=get_cluster_id)

        check_for_emr_cluster_op >> [create_cluster_op, cluster_exists_op]
        create_cluster_op >> get_cluster_id_op
        cluster_exists_op >> get_cluster_id_op
        get_cluster_id_op >> monitor_cluster_op >> handle_failure_op
        get_cluster_id_op >> add_steps_to_cluster_op >> monitor_step_op >> terminate_cluster_op

    return dag