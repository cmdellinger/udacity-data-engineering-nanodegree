from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 append=True, # default behavior is append
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_query=sql_query
        self.append=append

    def execute(self, context):
        # establish connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # default behavior is append (self.append == True)
        # Redshift docs say that TRUNCATE is prefered method to DELETE all
        if not self.append:
            self.log.info(f"Clearing data in destination Redshift table {self.table} table")
            redshift.run(f"TRUNCATE {self.table}")
        # load data
        self.log.info(f"Loading data into destination Redshift table {self.table}")
        redshift.run(f"INSERT INTO {self.table}\n" + self.sql_query)