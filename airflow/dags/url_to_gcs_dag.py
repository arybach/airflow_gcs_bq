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
yesterday = datetime.combine(datetime.today() - timedelta(days=1), datetime.min.time())

default_args = {
    'owner': 'groot',
    'depends_on_past': False,
    'start_date': yesterday,
    'execution_date': '{{ ti.xcom_pull(task_ids="push_to_xcom", key="execution_date") }}', # to depend on args_to_xcom_dag
    'color': Variable.get("color"),
    'year': Variable.get("year"),
    'month': Variable.get("month"),
    'retries': 3,
    'retry_delay': 120,
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
    schedule_interval=timedelta(days=7),
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
download_task >> change_permissions_task >> upload_task
