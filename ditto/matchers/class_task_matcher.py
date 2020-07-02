import inspect
from typing import Type

from airflow.models import BaseOperator

from ditto.api import TaskMatcher


class ClassTaskMatcher(TaskMatcher):
    """
    Matches a given operator based on the class being the same
    """
    def __init__(self, operator_class: Type):
        """
        :param operator_class: the class which should be checked for equality
        """
        super().__init__()
        self.operator_class = operator_class

    def does_match(self, task: BaseOperator) -> bool:
        """
        Matches this operator's entire ancestry of classes
        to the `operator_class` to match against

        :param task:
        :return:
        """
        for task_cls in inspect.getmro(task.__class__):
            if task_cls is self.operator_class:
                return True