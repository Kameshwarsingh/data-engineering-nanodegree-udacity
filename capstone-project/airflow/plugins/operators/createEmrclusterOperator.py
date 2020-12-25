from airflow.models import BaseOperator
import datetime
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
import boto3
import logging

class CreateEMRClusterOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 region_name,
                 cluster_name,
                 release_label='emr-5.9.0',
                 master_instance_type='m3.xlarge',
                 num_core_nodes=2,
                 core_node_instance_type='m3.2xlarge',
                 *args, **kwargs):

        super(CreateEMRClusterOperator, self).__init__(*args, **kwargs)
        self.region_name=region_name
        self.num_core_nodes=num_core_nodes
        self.cluster_name = cluster_name
        self.master_instance_type=master_instance_type
        self.release_label=release_label
        self.core_node_instance_type=core_node_instance_type

    def get_security_group_id(self,group_name):
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        ec2 = boto3.client('ec2', region_name=self.region_name,aws_access_key_id=credentials.access_key,aws_secret_access_key=credentials.secret_key,)
        response = ec2.describe_security_groups(GroupNames=[group_name])
        return response['SecurityGroups'][0]['GroupId']

    def create_cluster(self):
        response = ""
        try:
            aws_hook = AwsHook("aws_credentials")
            credentials = aws_hook.get_credentials()
            emr_connection = boto3.client('emr', region_name=self.region_name,aws_access_key_id=credentials.access_key,aws_secret_access_key=credentials.secret_key,)
            cluster_response = emr_connection.run_job_flow(
                Name='my-' + self.cluster_name + "-" + str(datetime.datetime.utcnow()),
                ReleaseLabel=self.release_label,
                Instances={
                    'InstanceGroups': [
                        {
                            'Name': "Master nodes",
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'MASTER',
                            'InstanceType': self.master_instance_type,
                            'InstanceCount': 1
                        },
                        {
                            'Name': "Slave nodes",
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'CORE',
                            'InstanceType': self.core_node_instance_type,
                            'InstanceCount': self.num_core_nodes
                        }
                    ],
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False,
                    'Ec2KeyName': 'my-keypair'
                },
                VisibleToAllUsers=True,
                JobFlowRole='EMR_EC2_DefaultRole',
                ServiceRole='EMR_DefaultRole',
                Applications=[
                    {'Name': 'hadoop'},
                    {'Name': 'spark'},
                    {'Name': 'hive'},
                    {'Name': 'livy'},
                    {'Name': 'zeppelin'}
                ]
            )
            response = cluster_response['JobFlowId']
        except Exception as e:
            self.logger.error("Could not create cluster",exc_info=True)
            raise AirflowException("Create cluster exception!")
        return response

    def execute(self, context):
        self.log.info("Creating EMR cluster cluster={0} at region={1}".format(self.cluster_name,self.region_name))
        self.log.info("EMR cluster number_of_nodes={0}".format(self.num_core_nodes))
        task_instance = context['task_instance']
        cluster_id = self.create_cluster();
        Variable.set("cluster_id", cluster_id)
        task_instance.xcom_push('cluster_id', cluster_id)
        self.log.info("The newly create_cluster_id = {0}".format(cluster_id))
        return cluster_id

