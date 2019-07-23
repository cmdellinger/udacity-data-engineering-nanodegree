from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    # template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_type="",
                 json_file="auto",
                 delimiter=",",
                 ignore_headers=True,
                 append=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.file_type=file_type
        self.json_file=json_file
        self.delimiter=delimiter
        self.ignore_headers=ignore_headers
        self.append=append

    def execute(self, context):
        # get Airflow's stored AWS environment variables from hook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        # create COPY query skeleton
        query_skeleton = f"""COPY {self.table}
                             FROM 's3://{self.s3_bucket}/{self.s3_key}'
                             ACCESS_KEY_ID '{credentials.access_key}'
                             SECRET_ACCESS_KEY '{credentials.secret_key}'
                          """
        # add file specific modifiers
        if self.file_type:
            file_type_mods = {"JSON": f"""FORMAT AS JSON '{self.json_file}' """,
                              "CSV" : f"""FORMAT AS CSV
                                          DELIMITER '{self.delimiter}'
                                          IGNOREHEADER {int(self.ignore_headers)}"""}
            try:
                query_skeleton = query_skeleton + file_type_mods[self.file_type.upper()]
            except: pass
        # create Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # delete staging table if exists (TRUNCATE recommended)
        # note: IF EXISTS not supported in Redshift, so creating IF NOT EXISTS
        #       to ensure TRUNCATE has a table to execute on
        self.log.info(f"Clearing {self.table} table in Redshift")
        redshift.run(f"TRUNCATE {self.table}")

        # load file data into Redshift table
        self.log.info(f"Copying {self.table} data from S3 to Redshift")
        redshift.run(query_skeleton)