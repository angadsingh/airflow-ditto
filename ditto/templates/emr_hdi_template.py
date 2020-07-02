from typing import Dict, Type, List

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.models import BaseOperator
from airflow.operators.sensors import S3KeySensor

from ditto.api import TransformerDefaultsConf, OperatorTransformer, TransformerResolver, SubDagTransformer
from ditto.ditto import AirflowDagTransformer
from ditto.transformers.emr import EmrCreateJobFlowOperatorTransformer
from ditto.transformers.emr import EmrJobFlowSensorTransformer
from ditto.transformers.emr import EmrAddStepsOperatorTransformer
from ditto.transformers.emr import EmrStepSensorTransformer
from ditto.transformers.emr import EmrTerminateJobFlowOperatorTransformer
from ditto.transformers.s3 import S3KeySensorBlobOperatorTransformer
from ditto.resolvers import ClassTransformerResolver, AncestralClassTransformerResolver


class EmrHdiDagTransformerTemplate(AirflowDagTransformer):
    """
    This is the defacto template to use for converting an EMR-based airflow DAG
    to an Azure HDInsight based airlow DAG in ditto, unless you encounter
    more complex patterns, in which case you can always create your own template.

    This is easily sub-classable.

    .. seealso::

        See `examples/example_emr_job_flow_dag.py` for an example.
        You can find more examples in the unit tests at `tests/test_dag_transformations.py`
    """
    def __init__(self, target_dag: DAG,
                 transformer_defaults: TransformerDefaultsConf = None,
                 operator_transformers: Dict[Type[BaseOperator], Type[OperatorTransformer]] = None,
                 transformer_resolvers: List[TransformerResolver] = None,
                 subdag_transformers: List[Type[SubDagTransformer]] = None,
                 **kwargs):
        _operator_transformers = {
            EmrCreateJobFlowOperator: EmrCreateJobFlowOperatorTransformer,
            EmrJobFlowSensor: EmrJobFlowSensorTransformer,
            EmrAddStepsOperator: EmrAddStepsOperatorTransformer,
            EmrStepSensor: EmrStepSensorTransformer,
            EmrTerminateJobFlowOperator: EmrTerminateJobFlowOperatorTransformer,
            S3KeySensor: S3KeySensorBlobOperatorTransformer
        }
        if operator_transformers:
            _operator_transformers.update(operator_transformers)

        _transformer_resolvers = [
            ClassTransformerResolver(_operator_transformers),
            AncestralClassTransformerResolver(_operator_transformers)]

        if transformer_resolvers:
            _transformer_resolvers.append(transformer_resolvers)

        super().__init__(target_dag,
                         transformer_defaults=transformer_defaults,
                         transformer_resolvers=_transformer_resolvers,
                         subdag_transformers=subdag_transformers,
                         **kwargs)
