import copy
import unittest
from unittest import mock

from airflow.models import Connection
from parameterized import parameterized

from ditto import rendering
from tests.test_commons import AZURE_CONN, AZURE_CONN_ID, HDI_CONN_ID, HDI_CONN, AWS_CONN_ID, EMR_CONN_ID, AWS_CONN, EMR_CONN
from tests.test_dags import simple_dag, complex_dag, check_cluster_dag
from alchemy_mock.mocking import UnifiedAlchemyMagicMock

from tests.test_utils import TestUtils


class TestDagTransformations(unittest.TestCase):
    def setUp(self) -> None:
        self.show_graphs = False

    @parameterized.expand([
        (simple_dag.emr_dag, simple_dag.transform_call, simple_dag.expected_hdi_dag, [10, 4.8]),
        (complex_dag.emr_dag, complex_dag.transform_call, complex_dag.expected_hdi_dag, [10, 8]),
        (check_cluster_dag.emr_dag, check_cluster_dag.transform_call, check_cluster_dag.expected_hdi_dag, [10, 4.8])
    ])
    def test_transform_parameterized(self, emr_dag, transform_call, expected_hdi_dag, figsize):
        self.mocked_transform(emr_dag, transform_call, expected_hdi_dag, figsize)

    @mock.patch('airflow.settings.Session')
    def mocked_transform(self, emr_dag, transform_call, expected_hdi_dag, figsize, mock_session):
        # mock airflow stuff
        session = UnifiedAlchemyMagicMock(data=[
            (
                [mock.call.query(Connection),
                 mock.call.filter(Connection.conn_id == AWS_CONN_ID)],
                [AWS_CONN]
            ),
            (
                [mock.call.query(Connection),
                 mock.call.filter(Connection.conn_id == EMR_CONN_ID)],
                [EMR_CONN]
            ),
            (
                [mock.call.query(Connection),
                 mock.call.filter(Connection.conn_id == AZURE_CONN_ID)],
                [AZURE_CONN]
            ),
            (
                [mock.call.query(Connection),
                 mock.call.filter(Connection.conn_id == HDI_CONN_ID)],
                [HDI_CONN]
        )])
        mock_session.return_value = session

        src_dag = emr_dag.create_dag()
        target_dag = transform_call(copy.deepcopy(src_dag))
        exp_dag = expected_hdi_dag.create_dag()

        print("Input DAG")
        src_dag.tree_view()
        print("Transformed DAG:")
        target_dag.tree_view()
        print("Expected DAG:")
        exp_dag.tree_view()

        if self.show_graphs:
            rendering.show_multi_dag_graphviz([src_dag, exp_dag, target_dag],
                                              relabeler=rendering.debug_relabeler,
                                              colorer=rendering.debug_colorer,
                                              legender=rendering.debug_legender,
                                              figsize=figsize)

        TestUtils.assert_dags_equals(self, exp_dag, target_dag)


if __name__ == '__main__':
    unittest.main()