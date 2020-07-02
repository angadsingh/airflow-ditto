from airflow import DAG

from ditto.templates.emr_hdi_template import EmrHdiDagTransformerTemplate
from ditto.transformers.subdag import CheckClusterSubDagTransformer


class CheckClusterEmr2HdiDagTransformerTemplate(EmrHdiDagTransformerTemplate):
    """
    A template which uses the :class:`~ditto.transformers.subdag.CheckClusterSubDagTransformer`

    .. seealso::

        See `examples/example_emr_job_flow_dag_2.py` for an example.
        You can find more examples in the unit tests at `tests/test_dag_transformations.py`
    """
    def __init__(self, src_dag: DAG, *args, **kwargs):
        super().__init__(src_dag, subdag_transformers=[CheckClusterSubDagTransformer],
                         *args, **kwargs)
