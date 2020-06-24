from typing import List

from airflow.models import BaseOperator

from ditto.api import OperatorTransformer, DAGFragment


class IdentityTransformer(OperatorTransformer):
    def transform(self, input_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment]) -> DAGFragment:
        input_operator._dag = None
        input_operator._upstream_task_ids.clear()
        input_operator._downstream_task_ids.clear()
        input_operator.dag = self.dag
        return DAGFragment([input_operator])