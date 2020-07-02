import inspect
import logging
from typing import Callable

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from ditto.api import TaskMatcher

log = logging.getLogger(__name__)


class PythonCallTaskMatcher(TaskMatcher):
    """
    Matches a :class:`PythonOperator <airflow.operators.python_operator.PythonOperator>` or :class:`BranchPythonOperator <airflow.operators.python_operator.BranchPythonOperator>`
    if their `python_callable`\'s are making the matching method calls or are the same as the method calls to match against.
    """
    def __init__(self, find_callable: Callable, nested_search: bool = True):
        """
        :param find_callable: the python method call to search for
        :param nested_search: if True, will search the source code of the `python_callable`
            of the python airflow operators given, for the `find_callable`, otherwise will
            just match on the `python_callable` itself
        """
        super().__init__()
        self.find_callable = find_callable
        self.nested_search = nested_search

    def does_match(self, task: BaseOperator) -> bool:
        """
        Uses the python `inspect` module to get the source code for a given
        python method if `nested_search` is enabled, otherwise just matches on the
        `python_callable` of the :class:`~airflow.operators.python_operator.PythonOperator` or
        :class:`~airflow.operators.python_operator.BranchPythonOperator`

        :param task: the operator to match against
        :return: whether or not it matched
        """
        if isinstance(task, PythonOperator) or isinstance(task, BranchPythonOperator):
            python_op: PythonOperator = task
            py_callable = python_op.python_callable

            if self.nested_search:
                try:
                    src_lines = inspect.getsourcelines(py_callable)[0]
                    for line in src_lines:
                        if self.find_callable.__name__ in line:
                            return True
                except TypeError:
                    log.warn("Could not get the source code for %s", py_callable)
                    return False
            else:
                if self.find_callable is py_callable:
                    return True