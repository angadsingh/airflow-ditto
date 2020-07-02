import copy
from abc import abstractmethod, ABC
from collections import deque
from typing import List, Type, Dict, TypeVar, Generic

from airflow import DAG
from airflow.models import BaseOperator


class DAGFragment:
    """
        A DAG fragment represents a sub-DAG of a DAG. It is meant to be able to
        represent fragments of operator subDAGs which are not yet attached to an
        airflow DAG. DAGFragments can also be part of its own fragment DAG using
        the ``parents`` and ``children`` references. Operator references are used
        to traverse the fragment's subDAG and parent/children references are used
        to traverse the DAG of fragments themselves if needed.
    """
    def __init__(self, tasks: List[BaseOperator], parents: List['DAGFragment']=None, children: List['DAGFragment']=None):
        """
        :param tasks: root tasks of the fragment (operator references are used to
            traverse the operator subDAG)
        :param parents: parent fragments
        :param children: child fragments
        """
        self.tasks: List[BaseOperator] = tasks
        if not parents:
            parents = []
        self.parents = parents  # type: List[DAGFragment]
        if not children:
            children = []
        self.children = children # type: List[DAGFragment]

    def add_parent(self, dag_fragment: 'DAGFragment'):
        """
        set a parent-child relationship between this `DAGFragment` and another

        :param dag_fragment: the other `DAGFragment`
        """
        if dag_fragment not in self.parents:
            self.parents.append(dag_fragment)
            dag_fragment.add_child(self)

    def add_child(self, dag_fragment: 'DAGFragment'):
        """
        set a child-parent relationship between this `DAGFragment` and another

        :param dag_fragment: the other `DAGFragment`
        """
        if dag_fragment not in self.children:
            self.children.append(dag_fragment)
            dag_fragment.add_parent(self)

    def add_child_in_place(self, child: BaseOperator):
        """
        Add an operator as a child `DAGFragment` at the place
        in the `DAGFragment` dag where it belonged in this fragment's
        operator DAG

        :param child: the operator to add in place
        """
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
    """
    Used to hold configuration for a :class:`~ditto.api.OperatorTransformer`
    """
    def __init__(self, default_operator: BaseOperator, other_defaults: Dict[str, str] = None):
        #: default output operator of this transformer
        self.default_operator = default_operator

        #: any other defaults or conf you might want to set
        self.other_defaults = other_defaults


T = TypeVar('T', bound=BaseOperator)


class Transformer:
    """
    Base class for all kinds of ditto transformers
    """

    #: this header is added as a param to the transformed airflow operator
    #: and can be used to find transformed operators in a target DAG
    TRANSFORMED_BY_HEADER = '__transformed_by'


class OperatorTransformer(Generic[T], ABC, Transformer):
    """
    A type of :class:`~ditto.api.Transformer` which can transform individual airflow operators
    This is an abstract class which your operator transformers need to subclass, where `T`
    is the source operator type being transformed
    """

    def __init__(self, target_dag: DAG, defaults: TransformerDefaults):
        """
        :param target_dag: the target to which the transformed operators must be added
        :param defaults: the default configuration for this transformer
        """
        self.dag = target_dag
        self.defaults = defaults if defaults is not None else \
            TransformerDefaults(default_operator=None, other_defaults={})

    @abstractmethod
    def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment, upstream_fragments: List[DAGFragment]) -> DAGFragment:
        """
        The main method to implement for an `OperatorTransformer`

        :param src_operator: The source operator to be tranformed
        :param parent_fragment: This will give you a linked-list of parent :class:`~ditto.api.DAGFragment`\'s, uptil the root
            The transform operation can make use of the upstream parent chain, for example, to obtain information
            about the upstream transformations. A good example of this is an add-step operator transformation
            which needs to get the cluster ID from an upstream transformed create-cluster operator
        :param upstream_fragments: This will give you *all* the upstream :class:`~ditto.api.DAGFragment`\'s transformed so far
            in level-order, from the root till this node in the source DAG. This is just a more exhaustive version of
            `parent_fragment`, and can give you parent's siblings, etc.
        :return: the :class:`~ditto.api.DAGFragment` of transformed airflow :class:`operators <airflow.models.BaseOperator>`
        """
        pass

    @staticmethod
    def copy_op_attrs(to_op: BaseOperator, from_op: BaseOperator) -> BaseOperator:
        """
        A utility method to copy all the attributes of a source operator
        to the transformed operator after transformation. Does not copy
        attributes which need to be unique like task_id or dag_id, and that
        have to be set by the transformer itself

        :param to_op: the target operator to copy the attrs to
        :param from_op: the source operator to copy the attrs from
        :return: the to_op
        """
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

    def get_default_op(self, src_operator: BaseOperator) -> BaseOperator:
        """
        A utility method to deep copy a :paramref:`~ditto.api.TransformerDefaults.default_operator`
        in the :class:`~ditto.api.TransformerDefaults` of this transformer

        :param src_operator: the source operator from which attrs need to be copied
        :return: the deep copied operator
        """
        if self.defaults is not None:
            if self.defaults.default_operator is not None:
                new_op = copy.deepcopy(self.defaults.default_operator)
                new_op.task_id = src_operator.task_id
                return self.copy_op_attrs(new_op, src_operator)

    def sign_op(self, output_op: BaseOperator):
        """
        Adds the :const:`~ditto.api.Transformer.TRANSFORMED_BY_HEADER` header
        as a param to the `output_op`

        :param output_op: the op to which the header has to be added
        """
        output_op.params[Transformer.TRANSFORMED_BY_HEADER] = self.__class__.__name__


class TransformerResolver(ABC):
    """
    The abstract base class to define ditto resolvers
    """
    @abstractmethod
    def resolve_transformer(self, task: BaseOperator) -> Type[OperatorTransformer]:
        """
        The main method to be implemented by a resolver

        :param task: the source task for which a transformer has to be resolved (found)
        :return: the type of :class:`~ditto.api.OperatorTransformer` found for this :class:`~airflow.models.BaseOperator`
        """
        pass


class TransformerException(Exception):
    """
    Exceptions thrown by ditto transformers
    """
    pass


class UpstreamOperatorNotFoundException(TransformerException):
    """
    A type of :class:`~ditto.api.TransformerException` thrown
    when an operator is not found upstream of this operator
    to be transformed and was required.
    """
    def __init__(self, upstream_op: BaseOperator, transformer_op: BaseOperator):
        message = (f"Cannot transform operator {transformer_op.__name__} "
        f"without a {upstream_op.__name__} defined upstream. "
        f"Make sure you have connections between operators configured properly.")
        super(TransformerException, self).__init__(message)


class TransformerDefaultsConf:
    def __init__(self, defaults: Dict[Type[Transformer], TransformerDefaults]):
        self.defaults = defaults


class TaskMatcher(ABC):
    """
    The abstract base class used to define task matchers.
    Task matchers are used to fingerprint and match operators based on certain
    qualifying criteria. They fit in ditto's workflow for subdag transformation
    where a subdag of TaskMatchers is found in the source airflow DAG
    TaskMatchers can be linked to each other to form a DAG using its ``parents``
    and ``children`` attributes
    """
    def __init__(self):
        #: parent matchers of this matcher
        self.parents = []

        #: child matchers of this matcher
        self.children = []

    def set_downstream(self, other):
        """
        add the `other` matcher to this matcher's children

        :param other: the other matcher
        :return: self
        """
        self.children.append(other)
        other.parents.append(self)

    def set_upstream(self, other):
        """
        add the `other` matcher to this matcher's parents

        :param other: the other matcher
        :return: self
        """
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
        """
        the main method to implement, containing logic to match
        the provided operator with the matcher's criteria

        :param task: the operator to try to match
        :return: `True` if matched otherwise `False`
        """
        pass


class SubDagTransformer(ABC, Transformer):
    """
    The abstract base class to implement to define ditto SubDagTransformers

    SubDag transformers are meant to, as their name goes, match subdags of operators
    in the source airflow DAG, and then transform and replace them with a new subdag
    as per the :class:`~ditto.api.DAGFragment`\'s returned.
    """
    def __init__(self, dag: DAG, defaults: TransformerDefaults):
        """
        :param dag: the source dag where subdags are to be found
        :param defaults: the default configuration for this subdag transformer
        """
        self.dag = dag
        self.defaults = defaults if defaults is not None else \
            TransformerDefaults(default_operator=None, other_defaults={})

    @abstractmethod
    def get_sub_dag_matcher(self) -> List[TaskMatcher]:
        """
        Implement this method to construct and return a DAG of matchers for ditto to
        then try to use and find a subdag in the source airflow DAG

        :return: list of root nodes of the :class:`~ditto.api.TaskMatcher` DAG
        """
        pass

    @abstractmethod
    def transform(self, subdag: DAGFragment, parent_fragment: DAGFragment) -> DAGFragment:
        """
        Implement this method to do the actual transformation of the subdag found using
        the matchers returned by :meth:`.get_sub_dag_matcher`

        :param subdag: :class:`~ditto.api.DAGFragment` representing subdag of source DAG operators matched
        :param parent_fragment: This will give you a linked-list of parent :class:`~ditto.api.DAGFragment`\'s,
            uptil the root. See :paramref:`~ditto.api.OperatorTransformer.transform.parent_fragment`.
        :return: the :class:`~ditto.api.DAGFragment` of transformed airflow :class:`operators <airflow.models.BaseOperator>`
        """
        pass