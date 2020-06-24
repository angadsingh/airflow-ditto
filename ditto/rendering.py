import random
from typing import List

import matplotlib
import networkx as nx
from airflow import DAG
from matplotlib import patches
from networkx.drawing.nx_agraph import graphviz_layout
import matplotlib.pyplot as plt

from ditto.api import Transformer
from ditto.utils import TransformerUtils

def ut_relabeler(dg: nx.DiGraph):
    labels = {}
    for node in dg.nodes:
        labels[node] = node.task_id
    return labels

def ut_colorer (dg: nx.DiGraph):
    color_map = []
    for node in dg.nodes:
        if node.task_id.startswith("tp"):
            color_map.append('red')
        elif node.task_id.startswith("t2p"):
            color_map.append('green')
        else:
            color_map.append('blue')
    return color_map

def debug_relabeler(dg: nx.DiGraph):
    labels = {}
    i = 1
    for node in dg.nodes:
        labels[node] = f"{i}"
        i+=1
    return labels

def debug_colorer (dg: nx.DiGraph):
    color_map = []
    for node in dg.nodes:
        if Transformer.TRANSFORMED_BY_HEADER in node.params:
            color_map.append('red')
        else:
            color_map.append('blue')
    return color_map

def debug_legender (dg: nx.DiGraph):
    handles = []
    i = 1
    for node in dg.nodes:
        label = f"{i}:{node.task_id}<{node.__class__.__name__}>"
        if Transformer.TRANSFORMED_BY_HEADER in node.params:
            handles.append(patches.Patch(color='red', label=label))
        else:
            handles.append(patches.Patch(color='blue', label=label))
        i += 1
    return handles

def draw_dag_graphiviz_rendering(dag: DAG,
                                 colorer=ut_colorer,
                                 relabeler=ut_relabeler,
                                 legender=None,
                                 figsize=[6.4, 4.8],
                                 legend_own_figure=False):
    dg = TransformerUtils.get_digraph_from_airflow_dag(dag)

    labels = {}
    if relabeler:
        labels = relabeler(dg)
    color_map = []
    if colorer:
        color_map = colorer(dg)

    dg.graph.setdefault('graph', {})['rankdir'] = 'LR'
    dg.graph.setdefault('graph', {})['newrank'] = 'true'
    plt.figure(figsize=figsize)
    plt.title(dag.dag_id)

    pos = graphviz_layout(dg, prog='dot', args='-Gnodesep=0.1')

    rads = random.uniform(0.05, 0.1)
    nx.draw_networkx(dg, pos=pos, labels=labels, font_size=8, node_color=color_map, node_size=900,
                     font_color='white', font_weight='bold', connectionstyle=f"arc3, rad={rads}")
    if legender:
        if legend_own_figure:
            plt.figure()
            plt.title(dag.dag_id)
            plt.rcParams["legend.fontsize"] = 8
            plt.legend(handles=legender(dg), ncol=2)
        else:
            plt.rcParams["legend.fontsize"] = 7
            plt.legend(handles=legender(dg), borderaxespad=0.9, ncol=2, loc='lower center')

def show_single_dag_graphviz(dag: DAG, **kwargs):
    matplotlib.use("TkAgg")
    draw_dag_graphiviz_rendering(dag, **{k: v for k, v in kwargs.items() if v is not None})
    plt.show()

def show_multi_dag_graphviz(daglist: List[DAG], **kwargs):
    matplotlib.use("TkAgg")
    i = 1
    for dag in daglist:
        draw_dag_graphiviz_rendering(dag, **{k: v for k, v in kwargs.items() if v is not None})
        i += 1
    plt.show()

def debug_dags(daglist: List[DAG], **kwargs):
    show_multi_dag_graphviz(daglist,
            relabeler=debug_relabeler,
            colorer=debug_colorer,
            legender=debug_legender,
            **{k: v for k, v in kwargs.items() if v is not None})