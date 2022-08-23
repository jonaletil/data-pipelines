from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Class for loading and transforming data from redshift
    to fact tables based on parameters

    Attributes:
        table: redshift table name
        redshift_conn_id: redshift connection
        sql_stmt: sql query
    """

    ui_color = '#F98866'
    insert_sql = """
            INSERT INTO {}
            {};
        """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading fact table: {self.table}")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql_stmt
        )

        self.log.info(f"Executing query: {formatted_sql}")
        redshift.run(formatted_sql)
        self.log.info(f"Load completed for table: {self.table}")
