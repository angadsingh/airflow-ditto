# Ditto

[![Build status](https://travis-ci.com/angadsingh/airflow-ditto.svg?branch=master)](https://travis-ci.com/angadsingh/airflow-ditto)
[![PyPi version](https://img.shields.io/pypi/v/airflow-ditto.svg)](https://pypi.org/project/airflow-ditto)
[![Supported versions](https://img.shields.io/pypi/pyversions/airflow-ditto.svg)](https://pypi.org/project/airflow-ditto)
[![Coverage status](https://codecov.io/github/angadsingh/airflow-ditto/coverage.svg?branch=master)](https://codecov.io/github/angadsingh/airflow-ditto)
[![PyPi downloads](https://img.shields.io/pypi/dm/airflow-ditto?label=pip%20installs)](https://pypistats.org/packages/airflow-ditto)

Ditto is a framework which allows you to do transformations to an Airflow DAG, to convert it into another DAG which is flow-isomorphic with the original DAG. i.e. it will orchestrate a flow of operators which yields the same results, but was just transformed to run in another environment or platform. The framework was built to transform EMR DAGs to run on Azure HDInsight, but you can extend the rich API for any other kind of transformation. In fact you can transform DAGs such that the result is not isomorphic too if you want (although at that point you're better off writing a whole new DAG).

The purpose of the framework is to allow you to maintain one codebase and be able to run your airflow DAGs on different execution environments (e.g. on different clouds, or even different container frameworks - spark on YARN vs kubernetes). It is not meant for a one-time transformation, but for continuous and parallel DAG deployments, although you can use it for that purpose too.

At the heart, Ditto is a graph manipulation library, which extendable APIs for the actual transformation logic. It does come with out of the box support for EMR to HDInsight transformation though.

#### Installation

    pip install airflow-ditto

#### A quick example

Ditto is created for conveniently transforming a large number of DAGs which follow a similar pattern quickly. Here's how easy it is to use Ditto:

```python
ditto = ditto.AirflowDagTransformer(DAG(
    dag_id='transformed_dag',
    default_args=DEFAULT_DAG_ARGS
), transformer_resolvers=[
    ClassTransformerResolver(
        {SlackAPIOperator: TestTransformer1,
         S3CopyObjectOperator: TestTransformer2,
         BranchPythonOperator: TestTransformer3,
         PythonOperator: TestTransformer4}
    )])

new_dag = ditto.transform(original_dag)
```
 
You can put the above call in any python file which is visible to airflow and the resultant dag loads up thanks to how airflow's dagbag finds DAGs.

*Source DAG* *(airflow view)*

![simple_dag_emr](https://raw.githubusercontent.com/angadsingh/airflow-ditto/master/README.assets/simple_dag_emr.png)

*Transformed DAG*

![simple_dag_hdi](https://raw.githubusercontent.com/angadsingh/airflow-ditto/master/README.assets/simple_dag_hdi.png)


***Read the detailed documentation [here](https://angadsingh.github.io/airflow-ditto/)***