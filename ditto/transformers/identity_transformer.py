from typing import List

from airflow.models import BaseOperator

from ditto.api import OperatorTransformer, DAGFragment


class IdentityTransformer(OperatorTransformer):
    """
    Re-uses the source operator as is as the transformed operator
    Just clears its connections to the source DAG and adds it
    to the target DAG

    .. warning::
        Use this transformer with caution. Reusing an operator
        can have weird side effects

    """
    def transform(self, input_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment]) -> DAGFragment:
        input_operator._dag = None
        input_operator._upstream_task_ids.clear()
        input_operator._downstream_task_ids.clear()
        input_operator.dag = self.dag
        return DAGFragment([input_operator])