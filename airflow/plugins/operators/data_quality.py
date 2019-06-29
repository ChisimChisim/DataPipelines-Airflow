from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 tests="",
                 test_results="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests
        self.test_results = test_results

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        for key, value in kwargs["params"].items():
            records = redshift_hook.get_records(value.keys()[0])
            if records != value.values()[0]:
                raise ValueError(f"Data quality check {key} failed.")
            logging.info(f"Data quality on {key} check passed.")