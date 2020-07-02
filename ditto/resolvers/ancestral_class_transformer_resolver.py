import inspect
from typing import Type

from airflow.models import BaseOperator

from ditto.api import OperatorTransformer
from ditto.resolvers import ClassTransformerResolver


class AncestralClassTransformerResolver(ClassTransformerResolver):
    """
    Extends the :class:`~ditto.resolvers.ClassTransformerResolver` by matching
    on all the ancestor classes of the given operator
    """
    def resolve_transformer(self, task: BaseOperator) -> Type[OperatorTransformer]:
        for task_cls in inspect.getmro(task.__class__):
            transformer_cl = self.get_transformer_for_class(task_cls)
            if transformer_cl:
                return transformer_cl