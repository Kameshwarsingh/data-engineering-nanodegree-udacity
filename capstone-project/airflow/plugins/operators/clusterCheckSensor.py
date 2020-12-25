from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
import boto3
import logging

def get_cluster_status(emr_conn, cluster_id):
    response = emr_conn.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['Status']['State']

class ClusterCheckSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self,region_name, *args, **kwargs):
        self.region_name = region_name
        return super(ClusterCheckSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        ti = context['ti']
        try:
            task_instance = context['task_instance']
            clusterId = Variable.get("cluster_id")
            self.log.info("The cluster id from create_emr_cluster {0}".format(clusterId))
            aws_hook = AwsHook("aws_credentials")
            credentials = aws_hook.get_credentials()
            emr_conn = boto3.client('emr', region_name=self.region_name,aws_access_key_id=credentials.access_key,aws_secret_access_key=credentials.secret_key,)
            status = get_cluster_status(emr_conn, clusterId)
            self.log.info(status)
            if status in ['STARTING','RUNNING','BOOTSTRAPPING']:
                return False
            else:
                return True
        except Exception as e:
            self.log.info(e)
            return False
