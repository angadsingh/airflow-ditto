import inspect
from typing import Type

from airflow.models import BaseOperator

from ditto.api import TaskMatcher


class ClassTaskMatcher(TaskMatcher):
    def __init__(self, operator_class: Type):
        super().__init__()
        self.operator_class = operator_class

    def does_match(self, task: BaseOperator) -> bool:
        for task_cls in inspect.getmro(task.__class__):
            if task_cls is self.operator_class:
                return True