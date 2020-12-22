from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
s3_bucket = 'udacity-dend'
song_s3_key = "song_data"
log_s3_key = "log_data"

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

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          #schedule_interval=None,
           max_active_runs=3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table_name="staging_events",
    s3_bucket = s3_bucket,
    s3_key = log_s3_key,
    file_format="JSON",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table_name="staging_songs",
    s3_bucket = s3_bucket,
    s3_key = log_s3_key,
    file_format="JSON",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.user_table_insert,
    truncate_table = True,
    table_name = "users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.song_table_insert,
    truncate_table = True,
    table_name = "songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.artist_table_insert,
    truncate_table = True,
    table_name = "artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.time_table_insert,
    truncate_table = True,
    table_name = "time"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id = "redshift",
    tables = ["artists", "songplays", "songs", "time", "users"]
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



##Build dependencies

start_operator>> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator