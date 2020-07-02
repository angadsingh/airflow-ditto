from typing import Dict, Type

from airflow.models import BaseOperator

from ditto.api import TransformerResolver, OperatorTransformer


class ClassTransformerResolver(TransformerResolver):
    """
    Finds a resolver for a transformer based on the class of the source operator given
    """
    def __init__(self, operator_transformers: Dict[Type[BaseOperator], Type[OperatorTransformer]]):
        """
        :param operator_transformers: a map of operator type and transformer type
        """
        super().__init__()
        self.operator_transformers = operator_transformers

    def get_transformer_for_class(self, task_cls: Type) -> Type[OperatorTransformer]:
        """
        return the transformer type for a given operator type

        :param task_cls: the operator type (class)
        :return: the transformer type
        """
        if task_cls in self.operator_transformers:
            transformer_cl = self.operator_transformers[task_cls]
            return transformer_cl

    def resolve_transformer(self, task: BaseOperator) -> Type[OperatorTransformer]:
        return self.get_transformer_for_class(task.__class__)