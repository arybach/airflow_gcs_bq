import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
#from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from datetime import datetime, timedelta
import argparse

parser = argparse.ArgumentParser()
# python gcs_to_bq_dag.py --projectid 'projectid' --objectid 'objectid' --datasetid 'datasetid' --datatable 'datatable' 
parser.add_argument('--projectid', type=str, required=False)
parser.add_argument('--objectid', type=str, required=False)
parser.add_argument('--datasetid', type=str, required=False)
parser.add_argument('--datatable', type=str, required=False)
args = parser.parse_args()

if not args.projectid: projectid = 'gcpzoomcamp' else: projectid = args.projectid 
if not args.objectid: objectid = 'green_tripdata_2020-01.parquet' else: objectid = args.objectid
if not args.datasettid: datasetid = 'green_taxi' else: datasetid = args.datasetid
if not args.datatable: datatable = 'trip_data' else: datatable = args.datatable

yesterday = datetime.combine(datetime.today() - timedelta(days=1), datetime.min.time())
timestamp_str = yesterday.strftime('%Y-%m-%dT%H:%M:%S') # for sql timestamp 

default_args = {
    'owner': 'groot',
    'depends_on_past': False,
    'start_date': yesterday,
    'email': ['mats.tumblebuns@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(hours=12)
}

dag = DAG(
    'download_from_gcs_upload_to_bq',
    default_args=default_args,
    description=f'Retrieve {objectid} from GCS and load into BigQuery',
    schedule_interval=timedelta(hours=24),
)

download_data = GCSToLocalFilesystemOperator(
    task_id = 'download_data',
    bucket = projectid,
    object_name = objectid,
    filename='/opt/bitnami/airflow/tmp/' + objectid,
    dag=dag
    # google_cloud_storage_conn_id='google_conn_default' - is used by default
)

# not needed here
# create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id='gcpzoomcamp')

create_table = BigQueryCreateEmptyTableOperator(
    task_id = "create_table",
    dataset_id = datasetid,
    table_id = datatable,
    table_resource = {"location": 'asia-east1'},
    schema_fields = [ 
          {"name": "VendorID", "type": "INT64", "mode": "REQUIRED"},
          {"name": "lpep_pickup_datetime", "type": "TIMESTAMP", "mode": "REQUIRED"},
          {"name": "lpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "REQUIRED"},
          {"name": "passenger_count", "type": "INT64", "mode": "NULLABLE"},
          {"name": "trip_distance", "type": "FLOAT64", "mode": "NULLABLE"},
          {"name": "RatecodeID", "type": "INT64", "mode": "NULLABLE"},
          {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
          {"name": "PULocationID", "type": "INT64", "mode": "REQUIRED"},
          {"name": "DOLocationID", "type": "INT64", "mode": "REQUIRED"},
          {"name": "payment_type", "type": "INT64", "mode": "NULLABLE"},
          {"name": "fare_amount", "type": "FLOAT64", "mode": "NULLABLE"},
          {"name": "extra", "type": "FLOAT64", "mode": "NULLABLE"},
          {"name": "mta_tax", "type": "FLOAT64", "mode": "NULLABLE"},
          {"name": "tip_amount", "type": "FLOAT64", "mode": "NULLABLE"},
          {"name": "tolls_amount", "type": "FLOAT64", "mode": "NULLABLE"},
          {"name": "improvement_surcharge", "type": "FLOAT64", "mode": "NULLABLE"},
          {"name": "total_amount", "type": "FLOAT64", "mode": "NULLABLE"},
          ], 
    dag=dag
)

def load_preped_parquet_to_bq():
        df = pd.read_parquet('/opt/bitnami/airflow/tmp/' + objectid)
        # not needed, but lets prep 
        print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
        df["passenger_count"].fillna(0, inplace=True)
        print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
        
        df.to_gbq(destination_table=f'{datasetid}.{datatable}',
                  if_exists = 'replace', # append
                  project_id = projectid)

upload_data = PythonOperator(
    task_id = 'upload_data',
    python_callable = load_preped_parquet_to_bq,
    dag = dag
)

add_column_ds = BigQueryOperator(
    task_id = 'add_column_ds',
    sql = f'''ALTER TABLE '{datasetid}.{datatable}' ADD COLUMN ds DATE''',
    use_legacy_sql = False,
    dag = dag
)

set_default_ds = BigQueryOperator(
    task_id = 'set_default_ds',
    sql = f'''ALTER TABLE '{datasetid}.{datatable}' ALTER COLUMN ds SET DEFAULT CURRENT_DATE()''',
    use_legacy_sql = False,
    dag = dag,
    trigger_rule = 'all_done'
)

update_ds = BigQueryOperator(
    task_id = 'update_ds',
    sql = f'''UPDATE '{datasetid}.{datatable}' SET ds = CURRENT_DATE() WHERE TRUE''',
    use_legacy_sql = False,
    dag = dag,
    trigger_rule = 'all_done'
)

partition_table = BigQueryOperator(
    task_id = 'partition_table',
    sql = f'''SELECT * FROM '{datasetid}.{datatable}'''',
    destination_dataset_table = f'{datasetid}.{datatable}'+'_partitioned',
    write_disposition = 'WRITE_TRUNCATE',
    time_partitioning = {
        'field': 'ds',
        'type': 'DAY'
    },
    use_legacy_sql = False,
    dag = dag,
    trigger_rule = 'all_done'
)

download_data >> create_table >> upload_data >> add_column_ds >> set_default_ds >> update_ds >> partition_table