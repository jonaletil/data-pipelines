from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Class for loading and transforming data from redshift
    to dimension table based on parameters

    Attributes:
        table: redshift table name
        redshift_conn_id: redshift connection
        sql_stmt: sql query
        delete_table: if True data will be deleted before inserting
    """

    ui_color = '#80BD9E'
    insert_sql = """
                INSERT INTO {}
                {};
            """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_stmt="",
                 delete_table=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.delete_table = delete_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_table:
            self.log.info("Deleting redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info(f"Loading dimension table: {self.table}")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql_stmt
        )

        self.log.info(f"Executing query: {formatted_sql}")
        redshift.run(formatted_sql)
        self.log.info(f"Load completed for table: {self.table}")
