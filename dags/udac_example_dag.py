from datetime import datetime, timedelta
import os
from airflow import DAG

from operators.create_tables import CreateTablesOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.drop_tables import DropTablesOperator

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'DinhNDA',
    'start_date': datetime(2023, 7, 21),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = CreateTablesOperator(
    task_id='Begin_execution',
    dag=dag,
    connection_id="redshift-cluster-1", # redshift connection
    sql_file="/opt/airflow/dags/scripts/create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    region="us-west-1",
    s3_json_format_path="s3://dinh-nda/log_json_path.json",
    s3={
        "bucket_name": "dinh-nda",
        "prefix": "log-data"
    },
    connection_id={
        "credentials": "aws_credentials",
        "redshift": "redshift-cluster-1"
    }
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    region="us-west-1",
    s3_json_format_path="auto",
    s3={
        "bucket_name": "dinh-nda",
        "prefix": "song-data"
    },
    connection_id={
        "credentials": "aws_credentials",
        "redshift": "redshift-cluster-1"
    }
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    connection_id="redshift-cluster-1",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    connection_id="redshift-cluster-1",
    table="users",
    sql_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    connection_id="redshift-cluster-1",
    table="songs",
    sql_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    connection_id="redshift-cluster-1",
    table="artists",
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    connection_id="redshift-cluster-1",
    table="time",
    sql_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    connection_id={
        "credentials": "aws_credentials",
        "redshift": "redshift-cluster-1"
    },
    dq_checks=[
        {
            "test_sql": "SELECT COUNT(*) FROM artists",
            "expected_results": 1627
        },
        {
            "test_sql": "SELECT COUNT(*) FROM songplays",
            "expected_results": 6820
        },
        {
            "test_sql": "SELECT COUNT(*) FROM songs",
            "expected_results": 1745
        },
        {
            "test_sql": "SELECT COUNT(*) FROM time",
            "expected_results": 6820
        },
        {
            "test_sql": "SELECT COUNT(*) FROM users",
            "expected_results": 104
        },
    ]
)

end_operator = DropTablesOperator(task_id='Stop_execution', dag=dag, connection_id="redshift-cluster-1",
                                  tables=["staging_events", "staging_songs", "artists", "songplays", "songs", "time", "users"])

start_operator >> [stage_songs_to_redshift, stage_events_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                         load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
