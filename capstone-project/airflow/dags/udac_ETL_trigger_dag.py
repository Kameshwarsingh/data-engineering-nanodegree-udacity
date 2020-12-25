from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from operators import (ClusterCheckSensor)
import boto3
from airflow import AirflowException
import logging

'''
emr_conn=None
try:
    emr_conn = boto3.client('emr', region_name=region_name)
except Exception as e:
    logging.info(emr_conn)
    raise AirflowException("emr_connection fail!")
'''

region_name="us-east-1"
default_args = {
    'owner': 'udacity-capstone',
    'start_date': datetime(2020, 12, 12),
    'end_date': datetime(2020, 12, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_ETL_trigger_dag',
          default_args=default_args,
          description='Trigger ETL',
          concurrency=3,
          schedule_interval=None
         )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

check_cluster = ClusterCheckSensor(
    task_id="udac_check_cluster_status",
    dag=dag,
    provide_context=True,
    poke=60,
    region_name="us-east-1"
)


'''
def trigger_imigration_dag_with_context(context,dag_run_obj):
    return dag_run_obj
    
trigger_dag = TriggerDagRunOperator(
    task_id = "trigger_immigration_dag",
    trigger_dag_id = "immigration_etl_dag",
    python_callable=trigger_imigration_dag_with_context,
    dag= dag,
    provide_context=True
)
'''
end_operator = DummyOperator(task_id='End_execution',  dag=dag)


##DAG - sequence/relations/dependencies
start_operator>>check_cluster>>end_operator