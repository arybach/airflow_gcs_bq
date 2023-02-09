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

import requests
import pandas as pd
import os
from datetime import datetime, timedelta
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--color', type=str, required=False)
parser.add_argument('--year', type=int, required=False)
parser.add_argument('--month', type=int, required=False)
args = parser.parse_args()

# python url_to_gcs_dag.py --color 'green' --year 2020 --month 1 
if not args.color: color = 'green' else: color = args.color
if not args.year: year = 2020 else: year = args.year
if not args.month: month = 1 else: month = args.month

yesterday = datetime.combine(datetime.today() - timedelta(days=1), datetime.min.time())

default_args = {
    'owner': 'groot',
    'depends_on_past': False,
    'start_date': yesterday,
    'color': color,
    'year': year,
    'month': month,
    'retries': 3,
    'retry_delay': 120,
}

dataset_file = f"{default_args['color']}_tripdata_{default_args['year']}-{default_args['month']:02}"
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


def download_nyc_tlc_data(color, year, month, url, dataset_file):
    """ fetches cvs file from url, cleans it and saves it to a current folder"""
    path2file = fetch_clean_save_to_parquet(url, '/opt/bitnami/airflow/tmp/', dataset_file)
    print("saved: ", path2file )

# def download_nyc_tlc_data(color, year, month, url, dataset_file):
#     """ fetches cvs file from url and saves it to a current folder"""
#     response = requests.get(url)
#     with open('/opt/bitnami/airflow/tmp/' + dataset_file, 'wb') as f:
#         f.write(response.content)


dag = DAG(
    'nyc_tlc_data_to_gcs_bucket',
    default_args=default_args,
    description='Downloads, modifies permissions and saves the NYC TLC data to a Google Cloud Storage bucket',
    schedule_interval=timedelta(days=7),
)


download_task = PythonOperator(
    task_id='download_nyc_tlc_data',
    python_callable=download_nyc_tlc_data,
    op_args=[default_args['color'], default_args['year'], default_args['month'], url, dataset_file],
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

download_task >> change_permissions_task >> upload_task
