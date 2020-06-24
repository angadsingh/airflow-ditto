import inspect
import logging
from typing import Callable

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from ditto.api import TaskMatcher

log = logging.getLogger(__name__)


class PythonCallTaskMatcher(TaskMatcher):
    def __init__(self, _callable: Callable, nested_search: bool = True):
        super().__init__()
        self.callable = _callable
        self.nested_search = nested_search

    def does_match(self, task: BaseOperator) -> bool:
        if isinstance(task, PythonOperator) or isinstance(task, BranchPythonOperator):
            python_op: PythonOperator = task
            py_callable = python_op.python_callable

            if self.nested_search:
                try:
                    src_lines = inspect.getsourcelines(py_callable)[0]
                    for line in src_lines:
                        if self.callable.__name__ in line:
                            return True
                except TypeError:
                    log.warn("Could not get the source code for %s", py_callable)
                    return False
            else:
                if self.callable is py_callable:
                    return True