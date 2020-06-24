import math
import unittest

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator

from ditto.api import DAGFragment, OperatorTransformer
from ditto.resolvers import PythonCallTransformerResolver


class TestTransformerResolvers(unittest.TestCase):

    def test_python_call_transformer_resolver(self):
        def test_callable(l):
            print(f"i do something with {l}")
            print(f"foo bar {l}")
            math.factorial(l)

        class TestTransformer(OperatorTransformer):
            def transform(self, src_operator: BaseOperator, parent_fragment: DAGFragment) -> DAGFragment:
                pass

        py_op = PythonOperator(python_callable=test_callable,
                               op_args=[1,2],
                               task_id='test_task')

        resolver = PythonCallTransformerResolver(
            {math.factorial: TestTransformer},
            nested_search=True)

        self.assertEqual(TestTransformer, resolver.resolve_transformer(py_op))


if __name__ == '__main__':
    unittest.main()