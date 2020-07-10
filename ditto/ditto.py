import copy
from queue import Queue
from typing import Type, List

import logging

from airflow import DAG
from airflow.models import BaseOperator

from ditto import rendering
from ditto.api import TransformerDefaultsConf, SubDagTransformer, DAGFragment, \
    TransformerResolver
from ditto.transformers import CopyTransformer
from ditto.utils import TransformerUtils

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class AirflowDagTransformer:
    """
    Ditto's core logic is executed by this class. It executes two operations on a DAG in sequence.
    First it will run the provided subdag transformers to transform the source DAG itself
    Then it will transform the operators in the source dag and create a target DAG out of the
    returned :class:`~ditto.api.DAGFragment`\'s.
    """

    def __init__(self,
                 target_dag: DAG,
                 transformer_defaults: TransformerDefaultsConf = None,
                 transformer_resolvers: List[TransformerResolver] = None,
                 subdag_transformers: List[Type[SubDagTransformer]] = None,
                 debug_mode: bool = False):
        """
        :param target_dag: ditto allows you to provide a pre-fabricated airflow DAG object
            so that you can set essential parameters like it's ``schedule_interval``, ``params``,
            give it a unique ``dag_id``, etc. outside of ditto itself, instead of ditto
            copying the attributes of the DAG over from the source DAG. This gives more flexbility.
        :param transformer_defaults: allows you to pass a map of transformer type to their default
            configuration. This is helpful to pass things like a default operator to use when the
            transformer cannot transform the source operator for some reason, or any other configuration
            required by the transformer
        :param transformer_resolvers: resolvers to use to find the transformers for each kind of
            operator in the source DAG.
        :param subdag_transformers: subdag transformers to use for converting matching subdags
            in the source DAG to transformed subdags
        :param debug_mode: when `True` it will render the intermediate results of transformation
            using `networkx <https://networkx.github.io/>`_ and `maplotlib <https://matplotlib.org/>`_
            so that you can debug your transformations easily.
        """
        self.transformer_cache = {}
        self.target_dag = target_dag
        self.transformer_defaults = transformer_defaults
        self.transformer_resolvers = transformer_resolvers
        self.subdag_transformers = subdag_transformers
        self.debug_mode = debug_mode

    def transform_operators(self, src_dag: DAG):
        """
        Transforms the operators in the source DAG and creates the target DAG out of the returned
        :class:`~ditto.api.DAGFragment`\'s. Finds the transformers by running each operator through
        all the resolvers passed.

        Does a bread-first-traversal on the source DAG such that the result of the transformation
        of upstream (and previous ops in this level) are available to downstream transformers in
        level-order. This is helpful for real world use cases of transformation like having a
        spark step op transformer read the result of the transformation of a cluster create op transformer.

        Caches the results of transformations to avoid repeat work, as this is a graph, not a tree
        being traversed.

        .. note::

            Stitches the final target DAG after having transformed all operators.

        :param src_dag: the source airflow DAG to be operator-transformed
        :return: does not return anything. mutates ``self.target_dag`` directly
        """
        src_task_q: "Queue[(BaseOperator,DAGFragment)]" = Queue()
        for root in src_dag.roots:
            src_task_q.put((root, None))

        # a list representing all processed fragments so far
        # transformers can use this to fetch information from the
        # level before or even tasks before this one in the same level
        # we'll also use this to stetch our final airflow DAG together
        transformed_dag_fragments = []

        while not src_task_q.empty():
            src_task, parent_fragment = src_task_q.get()

            # since this is a graph, we can encounter the same source
            # task repeatedly if it has multiple parents in the src_dag
            # to avoid transforming it repeatedly, check if has already been seen
            task_dag_fragment = None
            cached_fragment = False
            if src_task in self.transformer_cache:
                log.info("Already transformed source task: %s", src_task)
                task_dag_fragment = self.transformer_cache[src_task]
                cached_fragment = True
            else:
                # get transformer class for this operator
                transformer_cl = None
                for resolver in self.transformer_resolvers:
                    transformer_cl = resolver.resolve_transformer(src_task)
                    if transformer_cl:
                        log.info(f"Found transformer for operator {src_task.__class__.__name__}"
                                 f": {transformer_cl.__name__} using {resolver.__class__.__name__}")
                        break

                if not transformer_cl:
                    transformer_cl = CopyTransformer

                # get transformer defaults for this operator
                transformer_defaults = None
                if self.transformer_defaults is not None:
                    if transformer_cl in self.transformer_defaults.defaults:
                        transformer_defaults = self.transformer_defaults.defaults[transformer_cl]

                # create the transformer
                transformer = transformer_cl(self.target_dag, transformer_defaults)

                # do transformation, and get DAGFragment
                task_dag_fragment = transformer.transform(src_task,
                                parent_fragment, transformed_dag_fragments)

                self.transformer_cache[src_task] = task_dag_fragment

                # add this transformed output fragment to
                # the upstream fragments processed so far
                transformed_dag_fragments.append(task_dag_fragment)

                # add children to queue
                if src_task.downstream_list:
                    for downstream_task in src_task.downstream_list:
                        src_task_q.put((downstream_task, task_dag_fragment))

            # chain it to the parent
            if parent_fragment:
                task_dag_fragment.add_parent(parent_fragment)

        # convert dag fragment relationships to airflow dag relationships
        # for the processed fragments (which are now available in topological
        # sorted order)
        for output_fragment in transformed_dag_fragments:
            # get a flattened list of roots for the child DAGFragments
            all_child_fragment_roots = [step for frag in output_fragment.children for step in frag.tasks]

            # attach the flattened roots of child DAGFragments to this DAGFragment
            TransformerUtils.add_downstream_dag_fragment(output_fragment,
                                    DAGFragment(all_child_fragment_roots))

    def transform_sub_dags(self, src_dag: DAG):
        """
        Transforms the subdags of the source DAG, as matched by the :class:`~ditto.api.TaskMatcher`
        DAG provided by the :class:`~ditto.api.SubDagTransformer`\'s :meth:`~ditto.api.SubDagTransformer.get_sub_dag_matcher`
        method.

        Multiple subdag transformers can run through the source DAG, and each of them can
        match+transform multiple subdags of the source DAG, _and_ each such transformation can
        return multiple subdags as a result, so this can get quite flexible if you want.

        The final DAG is carefully stitched with all the results of the subdag transformations.

        See the unit tests at `test_core.py` for complex examples.

        .. note::

            If your matched input subdag had different leaves pointing to different
            operators/nodes, the transformed subdags leaves will just get multiplexed
            to all the leaves of the `source DAG`, since it is not possible to know which
            new leaf is to be stitched to which node of the source DAG, and resolve
            new relationships based on old ones.

        .. warning::

            Make sure that you don't provide :class:`~ditto.api.SubDagTransformer`\'s which
            with overlapping subdag matchers, otherwise things can understandably get messy.

        .. seealso::

            The core logic behind this method lies in a graph algorithm called
            subgraph isomorphism, and is explained in detail at
            :meth:`~ditto.utils.TransformerUtils.find_sub_dag`

        :param src_dag: the source airflow DAG to be subdag-transformed
        :return: does not return anything. mutates the passed ``src_dag`` directly,
            which is why you should pass a copy of the source DAG.
        """
        for subdag_transformer_cl in self.subdag_transformers:
            transformer_defaults = None
            if self.transformer_defaults is not None:
                if subdag_transformer_cl in self.transformer_defaults.defaults:
                    transformer_defaults = self.transformer_defaults.defaults[subdag_transformer_cl]
            subdag_transformer = subdag_transformer_cl(src_dag, transformer_defaults)
            matcher_roots = subdag_transformer.get_sub_dag_matcher()

            # find matching sub-dags usng the [TaskMatcher] DAG
            src_dag_dg, subdags = TransformerUtils.find_sub_dag(src_dag, matcher_roots)
            src_dag_nodes = [t for t in src_dag_dg.nodes]

            # transform each matching sub-dag and replace it in the DAG
            cloned_subdags = copy.deepcopy(subdags)

            # deep copy since DiGraph holds weak refs and that creates a problem
            # with traversing the DiGraph after deleting nodes from the original airflow DAG
            for subdag, cloned_subdag in zip(subdags, cloned_subdags):
                # upstream tasks are nodes in the main dag in-edges of the nodes in this sub-dag
                # which do not belong to this sub-dag
                subdag_upstream_tasks = set(n for edge in src_dag_dg.in_edges(nbunch=subdag.nodes) \
                                            for n in edge if n not in subdag)
                # downstream tasks are nodes in the main dag out-edges of the nodes in this sub-dag
                # which do not belong to this sub-dag
                subdag_downstream_tasks = set(n for edge in src_dag_dg.edges(nbunch=subdag.nodes) \
                                            for n in edge if n not in subdag)

                subdag_nodes = [n for n in subdag.nodes]
                for task in subdag_nodes:
                    TransformerUtils.remove_task_from_dag(src_dag, src_dag_nodes, task)

                new_subdag_fragment = subdag_transformer.transform(cloned_subdag, DAGFragment(subdag_upstream_tasks))

                # attach new subdag to upstream
                if subdag_upstream_tasks:
                    for parent in subdag_upstream_tasks:
                        for new_root in new_subdag_fragment.tasks:
                            parent.set_downstream(new_root)

                # assign new subdag to src_dag
                TransformerUtils.add_dag_fragment_to_dag(src_dag, new_subdag_fragment)

                # attach downstream to the leaves of the new subdag
                TransformerUtils.add_downstream_dag_fragment(new_subdag_fragment, DAGFragment(subdag_downstream_tasks))

    def transform(self, src_dag: DAG):
        """
        This is the entry point to using ditto, and is the only method you
        should have to call after creating the transformer object. This calls
        the :meth:`.transform_sub_dags` and :meth:`.transform_operators` methods
        in sequence to realize the final :attr:`target_dag` and return it

        :param src_dag: the source airflow DAG to be ditto-transformed
        :return: the resultant transformed DAG
        """
        if self.debug_mode:
            rendering.debug_dags(
                [src_dag],
                figsize=[14, 14])

        # transform sub-DAGs of the src_dag
        if self.subdag_transformers:
            self.transform_sub_dags(src_dag)

        if self.debug_mode:
            rendering.debug_dags(
                [src_dag],
                figsize=[14, 14])

        # transform each step of the src_dag
        # and add it to the target_dag
        self.transform_operators(src_dag)

        if self.debug_mode:
            rendering.debug_dags(
                [src_dag, self.target_dag],
                figsize=[14, 14])

        return self.target_dag