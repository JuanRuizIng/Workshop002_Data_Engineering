from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
from etl import extract_csv, extract_db, transform_db, transform_csv, merge, load_function


default_args = {
    'owner': 'JuanRuizING',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 13),  # Update the start date to today or an appropriate date
    'email': ['juan_andres.ruiz@uao.edu.co'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    default_args=default_args,
    description='Grammys and Spotify ETL Process',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
)
def music_etl_analysis():

    @task
    def extract_task_csv ():
        return extract_csv(),

    @task
    def extract_task_db():
        return extract_db(),

    @task
    def transform_task_csv (json_data):
        return transform_csv(json_data),

    @task
    def transform_task_db (json_data):
        return transform_db(json_data),

    @task
    def merge_task (json_data_spotify, json_data_grammys):
        return merge(json_data_spotify, json_data_grammys),

    @task
    def load_task(json_data):
        load_function(json_data)

    data_spotify = extract_task_csv()
    data_grammys = extract_task_db()
    transformed_data_spotify = transform_task_csv(data_spotify)
    transformed_data_grammys = transform_task_db(data_grammys)
    merge_data = merge_task(transformed_data_spotify, transformed_data_grammys)
    load_task(merge_data)

workflow_music_etl_analysis = music_etl_analysis()