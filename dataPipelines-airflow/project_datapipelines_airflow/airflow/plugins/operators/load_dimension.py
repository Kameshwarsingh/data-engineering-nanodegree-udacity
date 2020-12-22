from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                sql_query = "",
                truncate_table = False,
                table_name = "",
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.truncate_table = truncate_table
        self.table_name = table_name
        

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f"Running TRUNCATE statement on table {self.table_name}")
            redshift_hook.run(f"TRUNCATE {self.table_name}")
            
        self.log.info(f"Load data into Dimension Table {self.table_name}")
        redshift_hook.run(self.sql_query)
        self.log.info('LoadDimensionOperator executed successfuly')
