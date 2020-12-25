from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from operators import (CreateEMRClusterOperator,ClusterCheckSensor,SubmitSparkJobToEmrOperator)
import logging

default_args = {
    'owner': 'udacity-capstone',
    'start_date': datetime(2020, 12, 12),
    'end_date': datetime(2020, 12, 30),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_create_emr_cluster_dag',
          default_args=default_args,
          description='Create EMR cluster, execute ETL.',
          concurrency=3,
          schedule_interval=None          
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_cluster=CreateEMRClusterOperator(
    task_id = "udac_create_emr_cluster",
    dag = dag,
    provide_context=True,
    region_name="us-east-1",
    cluster_name="udacity_capstone_cluster",
    release_label='emr-5.9.0',
    master_instance_type='m3.xlarge',
    num_core_nodes=3,
    core_node_instance_type='m3.2xlarge'
)


check_cluster = ClusterCheckSensor(
    task_id="udac_check_cluster_status",
    dag=dag,
    provide_context=True,
    poke=60,
    region_name="us-east-1"
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)


##DAG - sequence/relations/dependencies
start_operator >> create_cluster >> check_cluster >> end_operator

