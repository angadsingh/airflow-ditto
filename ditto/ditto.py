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
    def __init__(self,
                 target_dag: DAG,
                 transformer_defaults: TransformerDefaultsConf = None,
                 transformer_resolvers: List[TransformerResolver] = None,
                 subdag_transformers: List[Type[SubDagTransformer]] = None,
                 debug_mode: bool = False):
        self.transformer_cache = {}
        self.target_dag = target_dag
        self.transformer_defaults = transformer_defaults
        self.transformer_resolvers = transformer_resolvers
        self.subdag_transformers = subdag_transformers
        self.debug_mode = debug_mode

    def transform_operators(self, src_dag: DAG):
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