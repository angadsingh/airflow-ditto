import os

import boto3
from airflow.operators.dummy_operator import DummyOperator

from ditto import rendering

from ditto.templates import CheckClusterEmr2HdiDagTransformerTemplate
from ditto.transformers.subdag import CheckClusterSubDagTransformer

from datetime import timedelta
import yaml
from airflow import DAG, AirflowException
from airflow.contrib.operators.emr_create_job_flow_operator \
    import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.contrib.operators.emr_add_steps_operator \
    import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator \
    import EmrTerminateJobFlowOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils import trigger_rule
from airflow.utils.trigger_rule import TriggerRule

from ditto.api import TransformerDefaults, TransformerDefaultsConf
from airflowhdi.operators.azure_hdinsight_create_cluster_operator import ConnectedAzureHDInsightCreateClusterOperator

DEFAULT_ARGS = {
    'owner': 'angad',
    'depends_on_past': False,
    'start_date': days_ago(2),
    "provide_context": True,
    "retries": 0
}

CONFIG = None
CONFIG_YAML_PATH = f"{os.path.dirname(os.path.realpath(__file__))}/config/dag_config.yaml"


def get_config(key):
    global CONFIG

    if not CONFIG:
        with open(CONFIG_YAML_PATH, 'r') as stream:
            try:
                CONFIG = yaml.safe_load(stream)

            except yaml.YAMLError as exc:
                print(exc)

    return CONFIG[key]


def handle_failure_task():
    raise AirflowException('Marking DAG as failed due to an upstream failure!')


def check_for_existing_emr_cluster(emr_client, cluster_id):
    print("checking for cluster")


def check_for_cluster():
    """
    Checks if an existing cluster should be used for submitting steps.
    """
    emr_client = boto3.client('emr')

    return check_for_existing_emr_cluster(
            emr_client=emr_client, cluster_id=get_config('emr')['emr_cluster_id'])


def get_cluster_id(**kwargs):
    """
    Gets the cluster id, which can be either an existing cluster ready to
    accept steps, or a newly created cluster.
    """
    cluster_exists_id = get_config('emr')['emr_cluster_id']
    create_cluster_id = kwargs['ti'].xcom_pull('return_value', 'create_cluster')

    return create_cluster_id if create_cluster_id is not None \
        else cluster_exists_id

with DAG(
    dag_id='example_emr_job_flow_dag_2',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    schedule_interval=None,
    params=get_config('emr')
) as dag:

    create_cluster_op = EmrCreateJobFlowOperator(
        task_id='create_cluster',
        job_flow_overrides={'Name': 'PiCalc'},
        aws_conn_id=get_config('emr')['aws_conn_id'],
        emr_conn_id=get_config('emr')['emr_conn_id']
    )

    add_steps_to_cluster_op = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
        aws_conn_id=get_config('emr')['aws_conn_id'],
        steps=[
            {
                'Name': 'calculate_pi',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': '{{ params.hadoop_jar_path }}',
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
        aws_conn_id=get_config('emr')['aws_conn_id'],
        job_flow_id='{{ task_instance.xcom_pull("create_cluster", key="return_value") }}',
        timeout=1800)

    monitor_step_op = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id=get_config('emr')['aws_conn_id']
    )

    terminate_cluster_op = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
        aws_conn_id=get_config('emr')['aws_conn_id']
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

hdi_create_cluster_op = ConnectedAzureHDInsightCreateClusterOperator(task_id="inferred",
                                                                     azure_conn_id=get_config('hdi')['azure_conn_id'],
                                                                     hdi_conn_id=get_config('hdi')['hdi_conn_id'],
                                                                     cluster_name='inferred',
                                                                     trigger_rule=TriggerRule.ALL_SUCCESS)
hdidag = CheckClusterEmr2HdiDagTransformerTemplate(DAG(
                dag_id='HDI_example_emr_job_flow_dag_2',
                default_args=DEFAULT_ARGS,
                dagrun_timeout=timedelta(hours=2),
                max_active_runs=1,
                schedule_interval=None,
                params=get_config('hdi')
        ),  transformer_defaults=TransformerDefaultsConf({
        CheckClusterSubDagTransformer: TransformerDefaults(
            default_operator=hdi_create_cluster_op,
            other_defaults=
                {'pycall_check_cluster': check_for_existing_emr_cluster})}),
        debug_mode=False).transform(dag)

if __name__ == '__main__':
    rendering.debug_dags(
        [dag, hdidag],
        figsize=[10, 5])

    # hdidag.clear(reset_dag_runs=True)
    # hdidag.run()