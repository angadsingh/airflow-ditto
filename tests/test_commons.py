import os

from airflow.models import Connection
from airflow.utils import dates

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dates.days_ago(1),
    "retries": 0
}

AZURE_CONN_ID = 'azure_hdi_default'
HDI_CONN_ID = 'azure_hdi_cluster_params_default'
AWS_CONN_ID = 'aws_default'
EMR_CONN_ID = 'emr_default'
CLUSTER_NAME = 'PiCalc'
EMR_CLUSTER_ID = 'test'

with open(f"{os.path.dirname(__file__)}/resources/test_azure_conn.json") as f:
    AZURE_CONN = Connection(extra=f.read())

with open(f"{os.path.dirname(__file__)}/resources/test_hdi_conn_extra.py") as f:
    HDI_CONN = Connection(extra=f.read())

with open(f"{os.path.dirname(__file__)}/resources/test_aws_conn.json") as f:
    AWS_CONN = Connection(extra=f.read())

with open(f"{os.path.dirname(__file__)}/resources/test_emr_conn.json") as f:
    EMR_CONN = Connection(extra=f.read())
