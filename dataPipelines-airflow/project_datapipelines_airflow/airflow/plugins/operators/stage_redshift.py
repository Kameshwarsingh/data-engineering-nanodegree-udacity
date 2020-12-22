from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    s3_copy = " COPY {} \
    FROM '{}' \
    ACCESS_KEY_ID '{}' \
    SECRET_ACCESS_KEY '{}' \
    FORMAT AS json '{}'; \
    "
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id="",
                 table_name="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 log_json_file="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credential_id=aws_credential_id
        self.table_name=table_name
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.file_format=file_format
        self.log_json_file=log_json_file
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        
        #build S3 path
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        #JSON considerations 
        if self.log_json_file != "":
            self.log_json_file = "s3://{}/{}".format(self.s3_bucket, self.log_json_file)
            s3_copy = self.s3_copy.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, self.log_json_file)
        else:
            s3_copy = self.s3_copy.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, 'auto')

        ##Load s3 data to Redshift
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f"Executing copy query : {s3_copy}")
        redshift_hook.run(s3_copy)
        self.log.info('StageToRedshiftOperator implemented yet')





