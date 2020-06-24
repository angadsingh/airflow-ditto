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
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import trigger_rule

from tests.test_commons import DEFAULT_DAG_ARGS, AWS_CONN_ID, EMR_CONN_ID


def handle_failure_task():
    raise AirflowException('Marking DAG as failed due to an upstream failure!')

def create_dag():
    with DAG(
        dag_id='emr_job_flow_manual_steps_dag',
        default_args=DEFAULT_DAG_ARGS,
        dagrun_timeout=timedelta(hours=2),
        max_active_runs=1,
        schedule_interval=None
    ) as dag:

        create_cluster_op_1 = EmrCreateJobFlowOperator(
            task_id='create_cluster_1',
            job_flow_overrides={'Name': 'PiCalc'},
            aws_conn_id=AWS_CONN_ID,
            emr_conn_id=EMR_CONN_ID
        )

        create_cluster_op_2 = EmrCreateJobFlowOperator(
            task_id='create_cluster_2',
            job_flow_overrides={'Name': 'PiCalc2'},
            aws_conn_id=AWS_CONN_ID,
            emr_conn_id=EMR_CONN_ID
        )

        add_steps_to_cluster1_op = EmrAddStepsOperator(
            task_id='add_steps_cluster_1',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster_1', key='return_value') }}",
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
                },
                {
                    'Name': 'calculate_pi_2',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 's3://psm-poc-dmp-temp/spark-examples.jar',
                        'Args': [
                            '10'
                        ],
                        'MainClass': 'org.apache.spark.examples.SparkPi2'
                    }
                }
            ]
        )

        add_steps_to_cluster2_op = EmrAddStepsOperator(
            task_id='add_steps_cluster_2',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster_2', key='return_value') }}",
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
                },
                {
                    'Name': 'calculate_pi_2',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 's3://psm-poc-dmp-temp/spark-examples.jar',
                        'Args': [
                            '10'
                        ],
                        'MainClass': 'org.apache.spark.examples.SparkPi2'
                    }
                }
            ]
        )

        monitor_cluster1_op = EmrJobFlowSensor(
            task_id='monitor_cluster_1',
            retries=0,
            aws_conn_id=AWS_CONN_ID,
            job_flow_id='{{ task_instance.xcom_pull("create_cluster_1", key="return_value") }}',
            timeout=1800)

        monitor_cluster2_op = EmrJobFlowSensor(
            task_id='monitor_cluster_2',
            retries=0,
            aws_conn_id=AWS_CONN_ID,
            job_flow_id='{{ task_instance.xcom_pull("create_cluster_2", key="return_value") }}',
            timeout=1800)

        monitor_step_op_cluster1_step_1 = EmrStepSensor(
            task_id='watch_step_cluster1_step0',
            job_flow_id="{{ task_instance.xcom_pull('create_cluster_1', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='add_steps_cluster_1', key='return_value')[0] }}",
            aws_conn_id=AWS_CONN_ID
        )

        monitor_step_op_cluster1_step_2 = EmrStepSensor(
            task_id='watch_step_cluster1_step1',
            job_flow_id="{{ task_instance.xcom_pull('create_cluster_1', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='add_steps_cluster_1', key='return_value')[1] }}",
            aws_conn_id=AWS_CONN_ID
        )

        monitor_step_op_cluster2_step_1 = EmrStepSensor(
            task_id='watch_step_cluster2_step0',
            job_flow_id="{{ task_instance.xcom_pull('create_cluster_2', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='add_steps_cluster_2', key='return_value')[0] }}",
            aws_conn_id=AWS_CONN_ID
        )

        monitor_step_op_cluster2_step_2 = EmrStepSensor(
            task_id='watch_step_cluster2_step1',
            job_flow_id="{{ task_instance.xcom_pull('create_cluster_2', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='add_steps_cluster_2', key='return_value')[1] }}",
            aws_conn_id=AWS_CONN_ID
        )

        terminate_cluster1_op = EmrTerminateJobFlowOperator(
            task_id='remove_cluster_1',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster_1', key='return_value') }}",
            aws_conn_id=AWS_CONN_ID
        )

        terminate_cluster2_op = EmrTerminateJobFlowOperator(
            task_id='remove_cluster_2',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster_2', key='return_value') }}",
            aws_conn_id=AWS_CONN_ID
        )

        handle_failure_op = PythonOperator(
            task_id='handle_failure',
            python_callable=handle_failure_task,
            trigger_rule=trigger_rule.TriggerRule.ONE_FAILED)


        clusters_created_op = DummyOperator(task_id='clusters_created')
        steps_added_op = DummyOperator(task_id='steps_added')
        steps_completed_op = DummyOperator(task_id='steps_completed')

        [create_cluster_op_1, create_cluster_op_2] >> clusters_created_op
        clusters_created_op >> [monitor_cluster1_op, monitor_cluster2_op] >> handle_failure_op
        clusters_created_op >> [add_steps_to_cluster1_op, add_steps_to_cluster2_op] >> steps_added_op
        steps_added_op >> [monitor_step_op_cluster1_step_1, monitor_step_op_cluster1_step_2, monitor_step_op_cluster2_step_1, monitor_step_op_cluster2_step_2] >> steps_completed_op
        steps_completed_op >> [terminate_cluster1_op, terminate_cluster2_op]

    return dag