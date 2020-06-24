import copy
from typing import List

from airflow import DAG
from airflow.models import BaseOperator

from ditto.api import OperatorTransformer, TransformerDefaults, DAGFragment


class CopyTransformer(OperatorTransformer):
    def __init__(self, target_dag: DAG, defaults: TransformerDefaults):
        super().__init__(target_dag, defaults)

    def transform(self, input_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment]) -> DAGFragment:
        # input_operator.shallow_copy_attrs += ('_dag',)
        copied_op = copy.deepcopy(input_operator)
        copied_op._dag = None
        copied_op._upstream_task_ids.clear()
        copied_op._downstream_task_ids.clear()
        copied_op.dag = self.dag
        return DAGFragment([copied_op])