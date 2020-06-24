import inspect
from typing import Dict, Callable, Type

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from ditto.api import TransformerResolver, OperatorTransformer


class PythonCallTransformerResolver(TransformerResolver):
    def __init__(self, callable_transformers: Dict[Callable, Type[OperatorTransformer]],
                 nested_search: bool = True):
        self.transformers_for_callables = callable_transformers
        self.nested_search = nested_search

    def resolve_transformer(self, task: BaseOperator) -> Type[OperatorTransformer]:
        if isinstance(task, PythonOperator) or isinstance(task, BranchPythonOperator):
            python_op: PythonOperator = task
            py_callable = python_op.python_callable
            src_lines = inspect.getsourcelines(py_callable)[0]

            if self.nested_search:
                for line in src_lines:
                    for find_callable in self.transformers_for_callables:
                        if find_callable.__name__ in line:
                            return self.transformers_for_callables[find_callable]
            else:
                for find_callable in self.transformers_for_callables:
                    if find_callable is py_callable:
                        return self.transformers_for_callables[find_callable]