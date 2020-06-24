from datetime import timedelta

from airflow import DAG, AirflowException
from airflow.contrib.operators.emr_create_job_flow_operator \
    import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.contrib.operators.emr_add_steps_operator \
    import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator \
    import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import trigger_rule

from tests.test_commons import DEFAULT_DAG_ARGS, AWS_CONN_ID, EMR_CONN_ID, CLUSTER_NAME


def handle_failure_task():
    raise AirflowException('Marking DAG as failed due to an upstream failure!')


class TemplatedEmrAddStepsOperator(EmrAddStepsOperator):
    pass


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

        add_steps_to_cluster_op = TemplatedEmrAddStepsOperator(
            task_id='add_steps',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
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
            job_flow_id='{{ task_instance.xcom_pull("create_cluster", key="return_value") }}',
            timeout=1800)

        monitor_step_op = EmrStepSensor(
            task_id='watch_step',
            job_flow_id="{{ task_instance.xcom_pull('create_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
            aws_conn_id=AWS_CONN_ID
        )

        terminate_cluster_op = EmrTerminateJobFlowOperator(
            task_id='remove_cluster',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
            aws_conn_id=AWS_CONN_ID
        )

        handle_failure_op = PythonOperator(
            task_id='handle_failure',
            python_callable=handle_failure_task,
            trigger_rule=trigger_rule.TriggerRule.ONE_FAILED)

        create_cluster_op >> monitor_cluster_op >> handle_failure_op
        create_cluster_op >> add_steps_to_cluster_op >> monitor_step_op >> terminate_cluster_op

    return dag