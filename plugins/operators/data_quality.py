from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Class used to run checks on the data

    Attributes:
        redshift_conn_id: redshift connection
        table: redshift table name
        column: redshift column name
        expected_result: expected result of the query
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 column="",
                 expected_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.column = column
        self.expected_result = expected_result

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table} WHERE {self.column} IS NULL;")

        self.log.info("Running test")

        num_records = records[0][0]

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")

        if num_records != self.expected_result:
            raise ValueError(f"""Data quality check failed. {records[0][0]} does not equal {self.expected_result}""")

        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
