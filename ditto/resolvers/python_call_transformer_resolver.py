import inspect
from typing import Dict, Callable, Type

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from ditto.api import TransformerResolver, OperatorTransformer


class PythonCallTransformerResolver(TransformerResolver):
    """
    Similar to the :class:`~ditto.matchers.PythonCallTaskMatcher`, but finds a transformer
    for a operator using the same matching pattern.
    """
    def __init__(self, callable_transformers: Dict[Callable, Type[OperatorTransformer]],
                 nested_search: bool = True):
        """
        :param callable_transformers: a map of python callables and matching transformers
        :param nested_search: whether to search the method source code itself for the callable
        """
        self.transformers_for_callables = callable_transformers
        self.nested_search = nested_search

    def resolve_transformer(self, task: BaseOperator) -> Type[OperatorTransformer]:
        """
        see the behavior of :meth:`~ditto.matchers.PythonCallTaskMatcher.does_match`

        :param task: the task to resolve the transformer for
        :return: the resolved transformer
        """
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