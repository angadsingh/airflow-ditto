import copy
from abc import abstractmethod, ABC
from collections import deque
from typing import List, Type, Dict, TypeVar, Generic

from airflow import DAG
from airflow.models import BaseOperator


class DAGFragment:
    """
        A DAG fragment represents a sub-DAG of a DAG
    """
    def __init__(self, tasks: List[BaseOperator], parents=None, children=None):
        """
        :param tasks: root tasks of the fragment (operator references are used to traverse sub-DAG
        :param parents: list of parent [DAGFragment]s
        :param children: list of child [DAGFragment]s
        """
        self.tasks: List[BaseOperator] = tasks
        if not parents:
            parents = []
        self.parents = parents  # type: List[DAGFragment]
        if not children:
            children = []
        self.children = children # type: List[DAGFragment]

    def add_parent(self, dag_fragment):
        if dag_fragment not in self.parents:
            self.parents.append(dag_fragment)
            dag_fragment.add_child(self)

    def add_child(self, dag_fragment):
        if dag_fragment not in self.children:
            self.children.append(dag_fragment)
            dag_fragment.add_parent(self)

    def add_child_in_place(self, child: BaseOperator):
        frag_q: "deque[DAGFragment]" = deque()
        frag_q.append(self)

        while len(frag_q) > 0:
            frag = frag_q.popleft()

            for task in frag.tasks:
                if child in task.downstream_list:
                    frag.add_child(DAGFragment([child]))

            if frag.children:
                for child_frag in frag.children:
                    frag_q.append(child_frag)




class TransformerDefaults:
    def __init__(self, default_operator: BaseOperator, other_defaults: Dict[str, str] = None):
        self.default_operator = default_operator
        self.other_defaults = other_defaults


T = TypeVar('T', bound=BaseOperator)


class Transformer:
    TRANSFORMED_BY_HEADER = '__transformed_by'


class OperatorTransformer(Generic[T], ABC, Transformer):

    def __init__(self, target_dag: DAG, defaults: TransformerDefaults):
        self.dag = target_dag
        self.defaults = defaults if defaults is not None else \
            TransformerDefaults(default_operator=None, other_defaults={})

    @abstractmethod
    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment]) -> DAGFragment:
        pass

    @staticmethod
    def copy_op_params(to_op: BaseOperator, from_op: BaseOperator):
        to_op.owner = from_op.owner if from_op.owner else to_op.owner
        to_op.email = from_op.email if from_op.email else to_op.email
        to_op.email_on_retry = from_op.email_on_retry if from_op.email_on_retry else to_op.email_on_retry
        to_op.email_on_failure = from_op.email_on_failure if from_op.email_on_failure else to_op.email_on_failure
        to_op.retries = from_op.retries if from_op.retries else to_op.retries
        to_op.retry_delay = from_op.retry_delay if from_op.retry_delay else to_op.retry_delay
        to_op.retry_exponential_backoff = from_op.retry_exponential_backoff if from_op.retry_exponential_backoff else  to_op.retry_exponential_backoff
        to_op.max_retry_delay = from_op.max_retry_delay if from_op.max_retry_delay else to_op.max_retry_delay
        to_op.start_date = from_op.start_date if from_op.start_date else to_op.start_date
        to_op.end_date = from_op.end_date if from_op.end_date else to_op.end_date
        to_op.depends_on_past = from_op.depends_on_past if from_op.depends_on_past else to_op.depends_on_past
        to_op.wait_for_downstream = from_op.wait_for_downstream if from_op.wait_for_downstream else to_op.wait_for_downstream
        to_op.priority_weight = from_op.priority_weight if from_op.priority_weight else to_op.priority_weight
        to_op.weight_rule = from_op.weight_rule if from_op.weight_rule else to_op.weight_rule
        to_op.queue = from_op.queue if from_op.queue else to_op.queue
        to_op.pool = from_op.pool if from_op.pool else to_op.pool
        to_op.pool_slots = from_op.pool_slots if from_op.pool_slots else to_op.pool
        to_op.sla = from_op.sla if from_op.sla else to_op.sla
        to_op.execution_timeout = from_op.execution_timeout if from_op.execution_timeout else to_op.execution_timeout
        to_op.on_failure_callback = from_op.on_failure_callback if from_op.on_failure_callback else to_op.on_failure_callback
        to_op.on_success_callback = from_op.on_success_callback if from_op.on_success_callback else to_op.on_success_callback
        to_op.on_retry_callback = from_op.on_retry_callback if from_op.on_retry_callback else to_op.on_retry_callback
        to_op.trigger_rule = from_op.trigger_rule if from_op.trigger_rule else to_op.trigger_rule
        to_op.resources = from_op.resources if from_op.resources else to_op.resources
        to_op.run_as_user = from_op.run_as_user if from_op.run_as_user else to_op.run_as_user
        to_op.task_concurrency = from_op.task_concurrency if from_op.task_concurrency else to_op.task_concurrency
        to_op.executor_config = from_op.executor_config if from_op.executor_config else to_op.executor_config
        to_op.do_xcom_push = from_op.do_xcom_push if from_op.do_xcom_push else to_op.do_xcom_push
        to_op.inlets = from_op.inlets if from_op.inlets else to_op.inlets
        to_op.outlets = from_op.outlets if from_op.outlets else to_op.outlets

        return to_op

    def get_default_op(self, src_operator: BaseOperator):
        if self.defaults is not None:
            if self.defaults.default_operator is not None:
                new_op = copy.deepcopy(self.defaults.default_operator)
                new_op.task_id = src_operator.task_id
                return self.copy_op_params(new_op, src_operator)

    def sign_op(self, output_op: BaseOperator):
        output_op.params[Transformer.TRANSFORMED_BY_HEADER] = self.__class__.__name__


class TransformerResolver(ABC):
    @abstractmethod
    def resolve_transformer(self, task: BaseOperator) -> Type[OperatorTransformer]:
        pass


class TransformerException(Exception):
    pass


class UpstreamOperatorNotFoundException(TransformerException):
    def __init__(self, upstream_op: BaseOperator, transformer_op: BaseOperator):
        message = (f"Cannot transform operator {transformer_op.__name__} "
        f"without a {upstream_op.__name__} defined upstream. "
        f"Make sure you have connections between operators configured properly.")
        super(TransformerException, self).__init__(message)


class TransformerDefaultsConf:
    def __init__(self, defaults: Dict[Type[Transformer], TransformerDefaults]):
        self.defaults = defaults


class TaskMatcher(ABC):
    def __init__(self):
        self.parents = []
        self.children = []

    def set_downstream(self, other):
        self.children.append(other)
        other.parents.append(self)

    def set_upstream(self, other):
        self.parents.append(other)
        other.children.append(self)

    def __rshift__(self, other):
        """
        Implements Self >> Other == self.set_downstream(other)
        """
        if isinstance(other, list):
            for o in other:
                self.set_downstream(o)
        else:
            self.set_downstream(other)
        return other

    def __lshift__(self, other):
        """
        Implements Self << Other == self.set_upstream(other)
        """
        if isinstance(other, list):
            for o in other:
                self.set_upstream(o)
        else:
            self.set_upstream(other)
        return other

    def __rrshift__(self, other):
        self.__lshift__(other)
        return self

    def __rlshift__(self, other):
        self.__rshift__(other)
        return self

    @abstractmethod
    def does_match(self, task: BaseOperator) -> bool:
        pass


class SubDagTransformer(ABC, Transformer):
    def __init__(self, dag: DAG, defaults: TransformerDefaults):
        self.dag = dag
        self.defaults = defaults if defaults is not None else \
            TransformerDefaults(default_operator=None, other_defaults={})

    @abstractmethod
    def get_sub_dag_matcher(self) -> List[TaskMatcher]:
        pass

    @abstractmethod
    def transform(self, subdag: DAGFragment, parent_fragment: DAGFragment) -> DAGFragment:
        pass