from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import CreateTableOperator


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 12, 12),
    'end_date': datetime(2020, 12, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_createTable_dag',
        default_args=default_args,
        description='Creates table in Redshift',
        schedule_interval=None,
        max_active_runs=1
        )


create_table_operator = CreateTableOperator(
    task_id='createTable_Redshift',
    redshift_conn_id = 'redshift', 
    dag=dag
)

start_operator = DummyOperator(task_id='Begin_CreateTable_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_CreateTable_execution',  dag=dag)

start_operator >> create_table_operator >> end_operator 