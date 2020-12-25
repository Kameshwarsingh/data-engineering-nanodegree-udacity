from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
import boto3
import logging

class TerminateEMRClusterOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 region_name="us-east-1",
                 *args, **kwargs):

        super(TerminateEMRClusterOperator, self).__init__(*args, **kwargs)
        self.region_name = region_name

    def terminate_cluster(self,clusterId):
        try:
            aws_hook = AwsHook("aws_credentials")
            credentials = aws_hook.get_credentials()
            emr_connection = boto3.client('emr', region_name=self.region_name,aws_access_key_id=credentials.access_key,aws_secret_access_key=credentials.secret_key,)
            emr_connection.terminate_job_flows(JobFlowIds=[clusterId])
        except Exception as e:
            self.logger.error("Error deleting the cluster",exc_info=True)
            raise AirflowException("Failed to terminate the EMR cluster")

    def execute(self, context):
        task_instance = context['task_instance']
        clusterId = Variable.get("cluster_id")
        self.log.info("Deleting the cluster EMR cluster cluster id={0}".format(clusterId))
        self.terminate_cluster(clusterId)
