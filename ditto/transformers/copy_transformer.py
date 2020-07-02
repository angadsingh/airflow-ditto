import copy
from typing import List

from airflow import DAG
from airflow.models import BaseOperator

from ditto.api import OperatorTransformer, TransformerDefaults, DAGFragment


class CopyTransformer(OperatorTransformer):
    """
    Deep copies the source operator, clears it of its connections
    to the source DAG, and any upstream/downstream tasks and assigns
    it to the `target_dag`.

    .. note::
        If you face issues with serialization of the transformed operator
        by airflow, you can exclude some attributes to be deep copied
        by overriding the following attribute of the source operator:

        >>> input_operator.shallow_copy_attrs += ('_dag',)
    """
    def __init__(self, target_dag: DAG, defaults: TransformerDefaults):
        super().__init__(target_dag, defaults)

    def transform(self, input_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment]) -> DAGFragment:
        copied_op = copy.deepcopy(input_operator)
        copied_op._dag = None
        copied_op._upstream_task_ids.clear()
        copied_op._downstream_task_ids.clear()
        copied_op.dag = self.dag
        return DAGFragment([copied_op])