.. highlight:: python

A more complex example
~~~~~~~~~~~~~~~~~~~~~~

This is how you can transform any EMR operator based DAG to an HDInsight operator based DAG (using operators from the `airflow-hdinsight <https://gitlab.pinsightmedia.com/telco-dmp/airflow-hdinsight>`_ project)::

   ditto = ditto.AirflowDagTransformer(DAG(
       dag_id='HDI_dag',
       default_args=DEFAULT_DAG_ARGS,
       dagrun_timeout=timedelta(hours=2),
       max_active_runs=1,
       schedule_interval=None,
       ),
       transformer_resolvers=[
           AncestralClassTransformerResolver({
               EmrCreateJobFlowOperator: EmrCreateJobFlowOperatorTransformer,
               EmrJobFlowSensor: EmrJobFlowSensorTransformer,
               EmrAddStepsOperator: EmrAddStepsOperatorTransformer,
               EmrStepSensor: EmrStepSensorTransformer,
               EmrTerminateJobFlowOperator: EmrTerminateJobFlowOperatorTransformer,
               S3KeySensor: S3KeySensorBlobOperatorTransformer
           })],
       transformer_defaults=TransformerDefaultsConf({
           EmrCreateJobFlowOperatorTransformer: TransformerDefaults(
               default_operator=create_cluster_op
           ),
           CheckClusterSubDagTransformer: TransformerDefaults(
               default_operator=create_cluster_op)}),
       subdag_transformers=[CheckClusterSubDagTransformer],
       debug_mode=True)

   new_dag = ditto.transform(original_dag)

There's a lot happening here, but the above example uses (almost) all capabilities of the Ditto API. It finds source operators using a :class:`resolver <ditto.api.TransformerResolver>` which specifies it's :class:`transformers <ditto.api.Transformer>`\ , it takes :obj:`default operators <ditto.api.TransformerDefaults.default_operator>`\ , transforms operators and replaces them with new sub-DAG of operators, uses a :class:`~ditto.api.SubDagTransformer` to transform and replaces entire sub-DAG of operators, where the sub-DAGs to be found are found using :class:`matchers <ditto.api.TaskMatcher>` and so on. The above DAG transformation logic is provided out of the box as a :mod:`template <ditto.templates>`\ : :class:`~ditto.templates.EmrHdiDagTransformerTemplate`

*Source DAG* *(rendered by ditto)*


.. image:: README.assets/complex_dag_emr.png
   :target: README.assets/complex_dag_emr.png
   :alt: Figure_1


*Transformed DAG*


.. image:: README.assets/complex_dag_hdi.png
   :target: README.assets/complex_dag_hdi.png
   :alt: Figure_2

Full example DAGs
~~~~~~~~~~~~~~~~~

Two example DAGs are provided which showcase the full capability of Ditto at:

 - `examples/example_emr_job_flow_dag.py <https://github.com/angadsingh/airflow-ditto/blob/master/examples/example_emr_job_flow_dag.py>`_
 - `examples/example_emr_job_flow_dag_2.py <https://github.com/angadsingh/airflow-ditto/blob/master/examples/example_emr_job_flow_dag_2.py>`_

Concepts
========

Transformers
~~~~~~~~~~~~

At the bottom of the abstraction hierarchy of Ditto's API are :class:`Transformers <ditto.api.Transformer>`.  These are the basic nuts and bolts of the system. These are called from Ditto's core during a transformation to convert the source DAG's operators to the target DAG's operators. There are two types of :class:`Transformers <ditto.api.Transformer>`: :class:`~ditto.api.OperatorTransformer` and :class:`~ditto.api.SubDagTransformer`. We'll talk about the former first, as the latter requires munching up a few more concepts to understand. An example :class:`~ditto.api.OperatorTransformer` could look like this::

   class IAmABasicTransformer(OperatorTransformer):
       def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment,
                     upstream_fragments: List[DAGFragment]) -> DAGFragment:
           op = LivyBatchSensor(
               batch_id="foo",
               task_id=src_operator.task_id,
               azure_conn_id="foo",
               cluster_name="foo",
               verify_in="yarn",
               dag=self.dag
           )
           return DAGFragment([op])

As you can see, it can access the ``src_operator``\ , choose to copy its fields or do whatever it pleases with it in order to create the target operator. :class:`Transformers <ditto.api.Transformer>` return :class:`DAGFragments <ditto.api.DAGFragment>`, which represents an intermediate sub-DAG of the target DAG you're trying to create. :class:`DAGFragments <ditto.api.DAGFragment>` can contain one or an entire sub-DAG of operators, and are linked to each other in a graph structure.

Here's an example of an :class:`~ditto.api.OperatorTransformer` which returns a transformed sub-DAG::

   class IAmASlightlyMoreComplexTransformer(OperatorTransformer):
       def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment,
                     upstream_fragments: List[DAGFragment]) -> DAGFragment:
           tp1 = LivyBatchSensor(
               batch_id="foo",
               task_id = "t2p1",
               azure_conn_id="foo",
               cluster_name=src_operator.dest_bucket_key,
               verify_in="yarn",
               dag=self.dag
           )

           tp2 = DummyOperator(task_id='t2p2', dag=self.dag)
           tp3 = DummyOperator(task_id='t2p3', dag=self.dag)
           tp4 = DummyOperator(task_id='t2p4', dag=self.dag)
           tp5 = PythonOperator(task_id='t2p5', python_callable=print, dag=self.dag)

           tp1 >> [tp2, tp3] >> tp4

           return DAGFragment([tp1, tp5])

As you can see this returns an entire sub-DAG instead of just one target operator. This :class:`~ditto.api.DAGFragment` will replace the :paramref:`~ditto.api.OperatorTransformer.transform.src_operator`, at its position in the source DAG, in the target DAG returned by Ditto.

.. topic:: About :paramref:`~ditto.api.OperatorTransformer.transform.parent_fragment` and :paramref:`~ditto.api.OperatorTransformer.transform.upstream_fragments`

    :class:`Transformers <ditto.api.OperatorTransformer>` sometimes need access to transformations of previous upstream transformers. E.g. you created the cluster, but now the add-steps operator needs to know the cluster name or ID, or your step-sensor needs to know the job ID from add-steps. :paramref:`~ditto.api.OperatorTransformer.transform.parent_fragment` is the :class:`DAGFragments <ditto.api.DAGFragment>` of the parent op in the source DAG. You can traverse it using :meth:`~ditto.utils.TransformerUtils.find_op_in_fragment_list` or a whole bunch of other utility methods to find a matching parent previously transformed. Similarly, :paramref:`~ditto.api.OperatorTransformer.transform.upstream_fragments` contains *all* the upstream transformed tasks. Think of it as a level-order/BFS traversal, uptil this task, in the target DAG *transformed so far*.

    This is a core part of how Ditto works: it allows transformers to *talk to each other* while tranforming a DAG. Look at the :class:`~ditto.transformers.emr.EmrStepSensorTransformer` or :class:`~ditto.transformers.emr.EmrAddStepsOperatorTransformer` to see how this works.

Resolvers
~~~~~~~~~

Ok, so you made a bunch of cool transformers. But how would Ditto know which one to use for which source operator? That's where :class:`Resolvers <ditto.api.TransformerResolver>` come in. As you can see in the examples given at the beginning, when you initialize ditto's core (:class:`~ditto.ditto.AirflowDagTransformer`), you give it a bunch of resolvers to use. When traversing the source graph, it asks each resolvers to *resolve a transformer for that source task*. It then uses that :class:`Transformer <ditto.api.OperatorTransformer>` to transform that source task. Ditto provides the following :mod:`Resolvers <ditto.resolvers>` out of the box, but you can write your own:


* :class:`~ditto.resolvers.ClassTransformerResolver` : find transformer based on source task's python class
* :class:`~ditto.resolvers.AncestralClassTransformerResolver` : find transformer based on all the ancestor classes of the source task's python class
* :class:`~ditto.resolvers.PythonCallTransformerResolver` : match a transformer with an operator if its a :class:`airflow.operators.python_operator.PythonOperator` and it's ``python_callable`` is calling a specified python method inside. It uses runtime source code parsing to achieve this. This comes handy when you want to transform custom :class:`PythonOperators <airflow.operators.python_operator.PythonOperator>` or :class:`BranchPythonOperators <airflow.operators.python_operator.BranchPythonOperator>` in the source DAG and match on them.

SubDag Transformers and Matchers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is where Ditto gets serious, if it did not appear so already. Suppose you want to transform entire sub-DAGs of the source DAG and replace them with your own subgraph of operators, and not just match on individual tasks. This is where :class:`SubDagTransformers <ditto.api.SubDagTransformer>` come in. This is best explained with an example


.. image:: README.assets/check_cluster_emr_dag.png
   :target: README.assets/check_cluster_emr_dag.png
   :alt: check_cluster_emr_dag


In this DAG, there's a pattern we want to match and replace::

   check_for_emr_cluster_op >> [create_cluster_op, cluster_exists_op]
   create_cluster_op >> get_cluster_id_op
   cluster_exists_op >> get_cluster_id_op

The DAG first checks if an EMR cluster exists, creates one if it doesn't and then with getting the cluster ID for downstream tasks. If we wanted to convert this to an HDInsight DAG, we wouldn't need this shebang, because :class:`~airflowhdi.operators.AzureHDInsightCreateClusterOperator` is idempotent, in that it simply does all of the above inside the operator itself (not explicitly, due to the nature of the HDInsight management API simply ignoring the create call if the cluster already exists). So we can cook up the following :class:`~ditto.api.SubDagTransformer` to solve this problem::

   class CheckClusterSubDagTransformer(SubDagTransformer):
       def get_sub_dag_matcher(self) -> List[TaskMatcher]:
           check_for_emr_cluster_op = PythonCallTaskMatcher(check_for_existing_emr_cluster)
           create_cluster_op = ClassTaskMatcher(EmrCreateJobFlowOperator)
           cluster_exists_op = ClassTaskMatcher(DummyOperator)
           get_cluster_id_op = PythonCallTaskMatcher(xcom_pull)

           check_for_emr_cluster_op >> [create_cluster_op, cluster_exists_op]
           create_cluster_op >> get_cluster_id_op
           cluster_exists_op >> get_cluster_id_op

           return [check_for_emr_cluster_op]

       def transform(self, subdag: nx.DiGraph, parent_fragment: DAGFragment) -> DAGFragment:
           transformer = EmrCreateJobFlowOperatorTransformer(self.dag, self.defaults)
           return transformer.transform(
               TransformerUtils.find_matching_tasks(
                   subdag, ClassTaskMatcher(EmrCreateJobFlowOperator))[0], parent_fragment)

The resulting DAG looks like this:

.. image:: README.assets/check_cluster_hdi_dag.png
   :target: README.assets/check_cluster_hdi_dag.png
   :alt: check_cluster_hdi_dag

This is very powerful. You can provide multiple :class:`SubDagTransformers <ditto.api.SubDagTransformer>` to Ditto, each one of them can find a sub-DAG and replace it with their own :class:`DAGFragments <ditto.api.DAGFragment>`. The API allows you to declaratively and easily conjure up a :class:`~ditto.api.TaskMatcher` DAG which the transformer will find. :class:`~ditto.api.TaskMatcher` or just ``Matchers`` are similar to :class:`Resolvers <ditto.api.TransformerResolver>`, in that you use them to match on the signature of the operator someway (class-based, python-call-based, or what have you), but they can be expressed as a DAG of :class:`matchers <ditto.api.TaskMatcher>` using the same bitshift assignment you are used to with airflow tasks. Then, behind the scenes, Ditto solves the `subgraph isomorphism problem <https://en.wikipedia.org/wiki/Subgraph_isomorphism_problem>`_\ , which is an `NP-complete problem <https://networkx.github.io/documentation/stable/reference/algorithms/isomorphism.vf2.html>`_\ , of finding a sub-DAG inside another DAG (using the :class:`matchers <ditto.api.TaskMatcher>` as the node equivalence functions!). It uses python `networkx <https://networkx.github.io/>`_ library to do this (with some jugglery to map airflow DAGs and matcher DAGs to networkx graphs and so on). See :meth:`~ditto.utils.TransformerUtils.find_sub_dag` for more details.

Here's a more complex example, where the matching sub-DAG is ``[Matcher(op2,op6), Matcher(op3,op7)] >> Matcher(op4,op8)``\ and the resultant sub-DAG has 5 nodes.

.. image:: README.assets/complex_subdag_transformer_src_dag.png
   :target: README.assets/complex_subdag_transformer_src_dag.png
   :alt: complex_subdag_transformer_src_dag



.. image:: README.assets/complex_subdag_transformer_target_dag.png
   :target: README.assets/complex_subdag_transformer_target_dag.png
   :alt: complex_subdag_transformer_target_dag

Templates
~~~~~~~~~

Templates are nothing but a configuration of :class:`OperatorTransformers <ditto.api.OperatorTransformer>`, :class:`Resolvers <ditto.api.TransformerResolver>`, :class:`SubDagTransformers <ditto.api.SubDagTransformer>` and their :class:`Matchers <ditto.api.TaskMatcher>` stitched together with some defaults. You can then reuse templates to transform several DAGs at different places. Templates bring it all together for you to use Ditto conveniently.

API Documentation
=================

.. toctree::
   :maxdepth: 2

   source/ditto.api
   source/ditto.ditto
   source/ditto.rendering
   source/ditto.utils

Built-in implementations
========================

Here are some of the built-in implementations of Ditto's API constructs available out of the box. You can find documentation of how each of them behave and work inside their python docstrings themselves.

.. toctree::
   :maxdepth: 6

   source/ditto.matchers
   source/ditto.resolvers
   source/ditto.templates
   source/ditto.transformers