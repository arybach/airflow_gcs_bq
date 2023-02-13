from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from google.cloud import storage

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
# from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
#from airflow.contrib.operators.gcs_to_local_operator import LocalFilesystemToGCSOperator
from airflow.models import Variable

import requests
import pandas as pd
import os
from datetime import datetime, timedelta

# python url_to_gcs_dag.py --color 'green' --year 2020 --month 1 
today = datetime.combine(datetime.today(), datetime.min.time())

def call_with_desired_context(**context):
    ds = context["dag_run"].conf["ds"]
    context["execution_date"] = ds
    # trigger lambda, do whatever you want with this ds
    # which will now be the same as the one from dag_a

# for when Variable has not been set yet by a diff dag
try: 
    color = Variable.get("color")
except KeyError:
    color = "green"

try: 
    year = Variable.get("year")
except KeyError:
    year = 2020

try:
    month = Variable.get("month")
except KeyError:
    month = 1

default_args = {
    'owner': 'groot',
    'depends_on_past': False,
    'start_date': datetime.now(), # if set to a diff value scheduler will determine when to run it even if trigger dag is triggered mannually
    'color': color,
    'year': year,
    'month': month,
    'retries': 0,
    'execution_date': '{{ ds }}',
    'dag_run.conf': {"external_trigger": True}
}

#dataset_file = f"{default_args['color']}_tripdata_{default_args['year']}-{default_args['month']:02}"
dataset_file = default_args['color'] + "_tripdata_" + str(default_args['year']) + "-" + str(default_args['month']).zfill(2)
url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{default_args['color']}/{dataset_file}.csv.gz"


def fetch_clean_save_to_parquet(url:str, filepath: str, dataset_file: str)-> str:
    """ fetches cvs file from url and reads it into pandas Dataframe"""
    response = requests.get(url)
    df = pd.read_csv(url)
   
    """ fix dtype issues """
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(f"rows: {len(df)}")

    """ save as parquet file """
    path2file = filepath + dataset_file + '.parquet'
    df.to_parquet(path2file, compression='gzip')
    return path2file


def download_nyc_tlc_data(url, dataset_file):
    """ fetches cvs file from url, cleans it and saves it to a current folder"""
    path2file = fetch_clean_save_to_parquet(url, '/opt/bitnami/airflow/tmp/', dataset_file)
    print("saved: ", path2file )


dag = DAG(
    'nyc_tlc_data_to_gcs_bucket',
    default_args=default_args,
    description='Downloads, modifies permissions and saves the NYC TLC data to a Google Cloud Storage bucket',
    schedule_interval=None,
)

pull_context_task = PythonOperator(
    dag=dag,
    task_id='pull_context',
    python_callable=call_with_desired_context,
    provide_context=True
)

download_task = PythonOperator(
    task_id='download_nyc_tlc_data',
    python_callable=download_nyc_tlc_data,
    op_args=[url, dataset_file],
    dag=dag,
)

change_permissions_task = BashOperator(
    task_id='change_permissions',
    bash_command='chmod a+rw /opt/bitnami/airflow/tmp/' + dataset_file + '.parquet',
    dag=dag,
)

upload_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs_bucket',
    bucket='gcpzoomcamp',
    dst=dataset_file + '.parquet',
    src='/opt/bitnami/airflow/tmp/' + dataset_file + '.parquet',
    #google_cloud_conn_id='google_cloud_default',
    dag=dag,
)

# these 3 simultaneously
pull_context_task >> download_task >> change_permissions_task >> upload_task
