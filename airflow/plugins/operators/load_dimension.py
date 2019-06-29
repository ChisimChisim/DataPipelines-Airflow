from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql="",
                 table="",
                 loding_mode="",   #"append-only" or  "delete-load"
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.sql=sql
        self.table=table
        self.loding_mode=loding_mode

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        #apppend pattern
        if self.loding_mode=="append-only":
            self.log.info(f'Appending {table} table')
            redshift_hook.run(f'ALTER TABLE {table} APPEND FROM {sql}')
        #truncate-insert pattern
        elif self.loding_mode=="delete-load":
            self.log.info(f'Cleaning {table} table')
            redshift_hook.run(f'TRUNCATE {table}')
            self.log.info(f'Inserting {table} table')
            redshift_hook.run(f'INSERT INTO {table} {sql}')
        
        self.log.info(f'completed to load {table} table')
        
