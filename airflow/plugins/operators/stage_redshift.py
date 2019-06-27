from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql_cvs = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    copy_sql_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 delimiter=",",
                 ignore_headers=1,
                 json="", # "auto" or JsonPath
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.json = json

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clear data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}".format(self.s3_bucket)
        if self.json="":
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter,
            #self.json)
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            #self.ignore_headers,
            #self.delimiter,
            self.json)
                
        redshift.run(formatted_sql)





