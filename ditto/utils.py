from collections import deque
from typing import Type, List

from airflow import DAG
from airflow.models import BaseOperator
import networkx as nx

from ditto.api import TaskMatcher, DAGFragment
import re
from queue import Queue


class TransformerUtils:

    @staticmethod
    def get_step_id_from_xcom_pull(xcom_template: str) -> str:
        return re.search("\[(\d+)\]", xcom_template).group(1)

    @staticmethod
    def get_task_id_from_xcom_pull(xcom_template: str) -> str:
        return re.search("{{.*xcom_pull\s*\(.*[\"|\'](.*)[\"|\'],.*}}", xcom_template).group(1)

    @staticmethod
    def add_downstream_dag_fragment(fragment_up: DAGFragment, fragment_down: DAGFragment):
        downstream_task_q: "Queue[BaseOperator]" = Queue()
        seen_tasks = set()

        if fragment_up is None:
            return fragment_down

        if fragment_down is None:
            return fragment_up

        for task in fragment_up.tasks:
            downstream_task_q.put(task)

        # add fragment_down.root_steps to the leaves of fragment_up
        while not downstream_task_q.empty():
            task = downstream_task_q.get()
            if len(task.downstream_list) > 0:
                for downstream_task in task.downstream_list:
                    if downstream_task not in seen_tasks:
                        downstream_task_q.put(downstream_task)
                        seen_tasks.add(downstream_task)
            else:
                task.set_downstream(fragment_down.tasks)

        return fragment_up

    @classmethod
    def find_op_in_parent_fragment_chain(cls, parent_fragment: DAGFragment,
                                         operator_type: Type[BaseOperator] = None,
                                         task_id: str = None) -> BaseOperator:
        op_found = None

        fragment_q: "Queue[DAGFragment]" = Queue()
        fragment_q.put(parent_fragment)

        while not fragment_q.empty():
            dag_fragment = fragment_q.get()
            op_found = cls.find_op_in_dag_fragment(dag_fragment,
                            operator_type=operator_type, task_id=task_id)
            if op_found:
                return op_found
            for parent in dag_fragment.parents:
                fragment_q.put(parent)


    @classmethod
    def find_op_in_fragment_list(cls, fragment_list: List[DAGFragment],
                                 operator_type: Type[BaseOperator] = None,
                                 task_id: str = None) -> BaseOperator:
        found_op = cls.find_op_in_fragment_list_strict(fragment_list,
                                                      operator_type=operator_type,
                                                      task_id=task_id)
        if not found_op:
            found_op = cls.find_op_in_fragment_list_strict(fragment_list,
                                                      operator_type=operator_type)

        return found_op

    @classmethod
    def find_op_in_fragment_list_strict(cls, fragment_list: List[DAGFragment],
                                        operator_type: Type[BaseOperator] = None,
                                        task_id: str = None) -> BaseOperator:
        for fragment in fragment_list:
            op_found = cls.find_op_in_dag_fragment(fragment,
                            operator_type=operator_type, task_id=task_id)
            if op_found:
                return op_found

    @staticmethod
    def find_op_in_dag_fragment(dag_fragment: DAGFragment,
                               operator_type: Type[BaseOperator] = None,
                               task_id: str = None,
                               upstream=False) -> BaseOperator:
        task_q: "Queue[BaseOperator]" = Queue()
        seen_tasks = set()

        for task in dag_fragment.tasks:
            task_q.put(task)

        while not task_q.empty():
            task = task_q.get()
            found_task = False
            if operator_type:
                if isinstance(task, operator_type):
                    found_task = True
            if task_id:
                if task.task_id == task_id:
                    found_task = True
                else:
                    found_task = False
            if found_task:
                return task

            relative_task_list = task.downstream_list
            if upstream and task.upstream_list:
                relative_task_list = task.upstream_list

            if relative_task_list:
                for relative_task in relative_task_list:
                    if relative_task not in seen_tasks:
                        task_q.put(relative_task)
                        seen_tasks.add(relative_task)

    @staticmethod
    def get_digraph_from_airflow_dag(dag: DAG) -> nx.DiGraph:
        dg = nx.OrderedDiGraph()
        task_q: "deque[BaseOperator]" = deque()
        task_q.extend(dag.roots)

        while len(task_q) > 0:
            task = task_q.popleft()
            dg.add_node(task, op=task)
            if task.downstream_list:
                task_q.extend(task.downstream_list)
                for child in task.downstream_list:
                    dg.add_node(child, op=child)
                    dg.add_edge(task, child)
        return dg

    @staticmethod
    def get_digraph_from_matcher_dag(matcher_roots: List[TaskMatcher]) -> nx.DiGraph:
        dg = nx.OrderedDiGraph()
        matcher_q: "deque[TaskMatcher]" = deque()
        matcher_q.extend(matcher_roots)

        while len(matcher_q) > 0:
            matcher = matcher_q.popleft()
            dg.add_node(matcher, m=matcher)
            if matcher.children:
                matcher_q.extend(matcher.children)
                for child in matcher.children:
                    dg.add_node(child, m=child)
                    dg.add_edge(matcher, child)
        return dg

    @classmethod
    def find_sub_dag(cls, dag: DAG, matcher_roots: List[TaskMatcher]) -> (nx.DiGraph, List[nx.DiGraph]):
        """
            This is an NP-complete problem: https://en.wikipedia.org/wiki/Subgraph_isomorphism_problem

            The problem is to find a sub-DAG in a DAG where the sub-DAG's nodes are
            matcher functions which test nodes

            It can be generalized to: find if a DAG or DiGraph G1 is isomorphic with
            a DAG G2, with the node comparison function being running of the matchers in G1
            on nodes in G2

            This uses python's NetworkX graph library which uses the VF2 algorithm for
            graph isomorphism:
            https://networkx.github.io/documentation/stable/reference/algorithms/isomorphism.vf2.html
            https://ieeexplore.ieee.org/document/1323804

            Note: We are trying to find an exact sub-DAG match. in graph theory this is
            called a node-induced subgraph. A subgraph 𝐻 of 𝐺 is called INDUCED, if for
            any two vertices 𝑢,𝑣 in 𝐻, 𝑢 and 𝑣 are adjacent in 𝐻 if and only if they
            are adjacent in 𝐺. In other words, 𝐻 has the same edges as 𝐺 between the
            vertices in 𝐻.
            source: https://math.stackexchange.com/questions/1013143/difference-between-a-sub-graph-and-induced-sub-graph

        :param task: the DAG where the sub-dag has to be found
        :param matcher: the root task matcher of the [TaskMatcher] dag
        :return:
        """
        dag_dg = cls.get_digraph_from_airflow_dag(dag)
        matcher_dg = cls.get_digraph_from_matcher_dag(matcher_roots)

        def node_matcher(n1, n2):
            task: BaseOperator = n1['op']
            matcher: TaskMatcher = n2['m']
            return matcher.does_match(task)
        digm = nx.isomorphism.DiGraphMatcher(dag_dg, matcher_dg, node_match=node_matcher)
        subdags: List[nx.DiGraph] = []
        if digm.subgraph_is_isomorphic():
            for subgraph in digm.subgraph_isomorphisms_iter():
                subdags.append(dag_dg.subgraph(subgraph.keys()))

        return (dag_dg, subdags)

    @staticmethod
    def remove_task_from_dag(dag: DAG, dag_nodes: List[BaseOperator], task: BaseOperator):
        all_other_tasks = [t for t in dag_nodes if t is not task]
        for this_task in all_other_tasks:
            if task.task_id in this_task._upstream_task_ids:
                this_task._upstream_task_ids.remove(task.task_id)

            if task.task_id in this_task._downstream_task_ids:
                this_task._downstream_task_ids.remove(task.task_id)

        task._upstream_task_ids.clear()
        task._downstream_task_ids.clear()
        task._dag = None
        del dag.task_dict[task.task_id]

    @classmethod
    def find_matching_tasks(cls, subdag: nx.DiGraph, matcher: TaskMatcher):
        matching_nodes = []
        for node in subdag.nodes:
            if matcher.does_match(node):
                matching_nodes.append(node)
        return matching_nodes

    @staticmethod
    def add_task_to_dag(op: BaseOperator, dag: DAGFragment):
        task_q: "deque[BaseOperator]" = deque()
        task_q.append(op)
        seen_tasks = set()

        while len(task_q) > 0:
            task = task_q.popleft()
            task.dag = dag
            if task.downstream_list:
                for child in task.downstream_list:
                    if child not in seen_tasks:
                        task_q.append(child)
                        seen_tasks.add(child)

    @classmethod
    def add_dag_fragment_to_dag(cls, dag: DAG, frag: DAGFragment):
        fragment_q: "deque[DAGFragment]" = deque()
        fragment_q.append(frag)
        seen_frag = set()

        while len(fragment_q) > 0:
            frag = fragment_q.popleft()
            for task in frag.tasks:
                cls.add_task_to_dag(task, dag)
            if frag.children:
                for child in frag.children:
                    if not child in seen_frag:
                        fragment_q.append(child)
                        seen_frag.add(child)