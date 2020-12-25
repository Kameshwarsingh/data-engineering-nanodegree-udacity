from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from operators import (TerminateEMRClusterOperator)


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

dag = DAG('udac_terminate_emr_dag',
          default_args=default_args,
          description='Terminates EMR cluster',
          concurrency=3,
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

terminate_emr_cluster = TerminateEMRClusterOperator(
    task_id="emr_terminate_dag",
    dag=dag,
    provide_context=True,
    region_name="us-east-1"
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

##DAG - sequence/relations/dependencies
start_operator >> terminate_emr_cluster >> end_operator