from typing import Dict, Type

from airflow.models import BaseOperator

from ditto.api import TransformerResolver, OperatorTransformer


class ClassTransformerResolver(TransformerResolver):
    def __init__(self, operator_transformers: Dict[Type[BaseOperator], Type[OperatorTransformer]]):
        super().__init__()
        self.operator_transformers = operator_transformers

    def get_transformer_for_class(self, task_cls: Type) -> Type[OperatorTransformer]:
        if task_cls in self.operator_transformers:
            transformer_cl = self.operator_transformers[task_cls]
            return transformer_cl

    def resolve_transformer(self, task: BaseOperator) -> Type[OperatorTransformer]:
        return self.get_transformer_for_class(task.__class__)