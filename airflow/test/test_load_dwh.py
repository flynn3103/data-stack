import unittest
from airflow.models import DagBag
from airflow.utils.state import State
import datetime

class TestLoadIntoDWH(unittest.TestCase):
    """
    Unit test for the load_into_dwh DAG
    """
    def setUp(self):
        """
        Setup the DAGBag to load the DAG
        """
        self.dagbag = DagBag()
        self.dag_id = 'load_into_dwh'
        self.dag = self.dagbag.get_dag(self.dag_id)

    def test_dag_loaded(self):
        """
        Test if the DAG is properly loaded
        """
        self.assertIsNotNone(self.dag)
        self.assertEqual(self.dag.dag_id, self.dag_id)

    def test_task_count(self):
        """
        Test if the DAG has the expected number of tasks
        """
        task_count = len(self.dag.tasks)
        self.assertEqual(task_count, 3)

    def test_task_dependencies(self):
        """
        Test the task dependencies in the DAG
        """
        # Check task order
        create_table_pg = self.dag.get_task('create_table_pg')
        insert_table_pg = self.dag.get_task('insert_table_pg')
        gx_validate_pg = self.dag.get_task('gx_validate_pg')

        # Test task relationships
        self.assertIn(insert_table_pg, create_table_pg.downstream_list)
        self.assertIn(gx_validate_pg, insert_table_pg.downstream_list)

    def test_task_types(self):
        """
        Test if the tasks are of correct types
        """
        from airflow.providers.postgres.operators.postgres import PostgresOperator
        from airflow.operators.python import PythonOperator
        from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

        self.assertIsInstance(self.dag.get_task("create_table_pg"), PostgresOperator)
        self.assertIsInstance(self.dag.get_task("insert_table_pg"), PythonOperator)
        self.assertIsInstance(self.dag.get_task("gx_validate_pg"), GreatExpectationsOperator)

    def test_default_args(self):
        """
        Test the default arguments of the DAG
        """
        self.assertEqual(self.dag.default_args["start_date"], '2023-07-01')
        self.assertEqual(self.dag.schedule_interval, None)

    def test_insert_table_execution(self):
        """
        Test the actual execution of the insert_table task
        """
        # Use the provided context to simulate task execution
        task = self.dag.get_task("insert_table_pg")
        context = {"execution_date": datetime(2023, 7, 1)}
        
        try:
            self.task.execute(context=context)
        except Exception as e:
            self.fail(f"Task execution failed with exception: {e}")

if __name__ == '__main__':
    unittest.main()
