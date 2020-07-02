from typing import List

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from ditto.api import OperatorTransformer, TransformerDefaults, DAGFragment, UpstreamOperatorNotFoundException
from ditto.utils import TransformerUtils
from airflowhdi.operators.azure_hdinsight_create_cluster_operator import ConnectedAzureHDInsightCreateClusterOperator
from airflowhdi.operators.azure_hdinsight_ssh_operator import AzureHDInsightSshOperator
from airflowhdi.operators import LivyBatchOperator


class EmrAddStepsOperatorTransformer(OperatorTransformer[EmrAddStepsOperator]):
    """
    Transforms the operator :class:`~airflow.contrib.operators.emr_add_steps_operator.EmrAddStepsOperator`
    """

    #: default livy proxy user
    DEFAULT_PROXY_USER = "admin"

    #: default spark configuration to use
    DEFAULT_SPARK_CONF = {'spark.shuffle.compress': 'false'}

    #: number of mappers to use in case its a distcp
    HADOOP_DISTCP_DEFAULT_MAPPERS = 100

    def __init__(self, target_dag: DAG, defaults: TransformerDefaults):
        super().__init__(target_dag, defaults)
        self.proxy_user = self.defaults.other_defaults.get('proxy_user', EmrAddStepsOperatorTransformer.DEFAULT_PROXY_USER)
        self.spark_conf = self.defaults.other_defaults.get('spark_conf', EmrAddStepsOperatorTransformer.DEFAULT_SPARK_CONF)

    @staticmethod
    def get_target_step_task_id(add_step_task_id, add_step_step_id):
        """
        generates a task_id for the transformed output operators
        for each input EMR step
        """
        return f"{add_step_task_id}_{add_step_step_id}"

    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment]) -> DAGFragment:
        """
        This transformer assumes and relies on the fact that an upstream transformation
        of a :class:`~airflow.contrib.operators.emr_create_job_flow_operator.EmrCreateJobFlowOperator`
        has already taken place, since it needs to find the output of that transformation
        to get the `cluster_name` and `azure_conn_id` from that operator (which should have been a
        :class:`~airflowhdi.operators.AzureHDInsightCreateClusterOperator`)

        It then goes through the EMR steps of this :class:`~airflow.contrib.operators.emr_add_steps_operator.EmrAddStepsOperator`
        and creates a :class:`~airflowhdi.operators.LivyBatchOperator` or an :class:`~airflowhdi.operators.AzureHDInsightSshOperator`
        for each corresponding step, based on grokking the step's params and figuring out whether its a spark job being run
        on an arbitrary hadoop command like `distcp`, `hdfs` or the like.

        .. note::

            This transformer creates multiple operators from a single source operator

        .. note::

            The spark configuration for the livy spark job are derived from `step['HadoopJarStep']['Properties']` of the EMR step,
            or could even be specified at the cluster level itself when transforming the job flow
        """
        create_op_task_id = TransformerUtils.get_task_id_from_xcom_pull(src_operator.job_flow_id)
        create_op: BaseOperator = \
            TransformerUtils.find_op_in_fragment_list(
                upstream_fragments,
                operator_type=ConnectedAzureHDInsightCreateClusterOperator,
                task_id=create_op_task_id)

        if not create_op:
            raise UpstreamOperatorNotFoundException(ConnectedAzureHDInsightCreateClusterOperator,
                                                    EmrAddStepsOperator)

        emr_add_steps_op: EmrAddStepsOperator = src_operator
        dag_fragment_steps = []
        steps_added_op = DummyOperator(
            task_id=f"{emr_add_steps_op.task_id}_added",
            dag=self.dag)
        self.sign_op(steps_added_op)

        for step in emr_add_steps_op.steps:
            name = step['Name']

            ssh_command = None
            livy_file = None
            livy_arguments = None
            livy_main_class = None

            if 'command-runner' in step['HadoopJarStep']['Jar']:
                command_runner_cmd = step['HadoopJarStep']['Args']
                if '/usr/bin/spark-submit' in command_runner_cmd[0]:
                    livy_file = command_runner_cmd[1]
                    livy_arguments = command_runner_cmd[2:]
                elif 's3-dist-cp' in command_runner_cmd[0]:
                    src = None
                    dest = None
                    for arg in command_runner_cmd[1:]:
                        if arg.startswith('--src='):
                            src = arg.split("--src=", 1)[1]
                        if arg.startswith('--dest='):
                            dest = arg.split("--dest=", 1)[1]
                    mappers = EmrAddStepsOperatorTransformer.HADOOP_DISTCP_DEFAULT_MAPPERS
                    ssh_command = f"hadoop distcp -m {mappers} {src} {dest}"
                elif 'hdfs' in command_runner_cmd[0]:
                    ssh_command = " ".join(command_runner_cmd)
                else:
                    raise Exception("This kind of step is not supported right now", command_runner_cmd[0])
            else:
                livy_file = step['HadoopJarStep']['Jar']
                livy_arguments = step['HadoopJarStep']['Args']
                livy_main_class = step['HadoopJarStep'].get('MainClass', None)

            if 'Properties' in step['HadoopJarStep']:
                properties = ""
                for key, val in step['HadoopJarStep']['Properties']:
                    properties += f"-D{key}={val} "

                self.spark_conf['spark.executor.extraJavaOptions'] = properties
                self.spark_conf['spark.driver.extraJavaOptions'] = properties

            target_step_task_id = EmrAddStepsOperatorTransformer.get_target_step_task_id(emr_add_steps_op.task_id, emr_add_steps_op.steps.index(step))

            if ssh_command is not None:
                step_op = AzureHDInsightSshOperator(
                    cluster_name=create_op.cluster_name,
                    azure_conn_id=create_op.azure_conn_id,
                    command=ssh_command,
                    task_id=target_step_task_id,
                    dag=self.dag
                )
            else:
                step_op = LivyBatchOperator(
                    name=name,
                    file=livy_file,
                    arguments=livy_arguments,
                    class_name=livy_main_class,
                    azure_conn_id=create_op.azure_conn_id,
                    cluster_name=create_op.cluster_name,
                    proxy_user=self.proxy_user,
                    conf=self.spark_conf,
                    task_id=target_step_task_id,
                    dag=self.dag
                )
            self.copy_op_attrs(step_op, emr_add_steps_op)
            self.sign_op(step_op)
            step_op.trigger_rule = TriggerRule.ALL_SUCCESS

            step_op.set_downstream(steps_added_op)
            dag_fragment_steps.append(step_op)

        return DAGFragment(dag_fragment_steps)