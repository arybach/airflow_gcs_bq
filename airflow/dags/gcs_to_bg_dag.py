import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
#from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from datetime import datetime, timedelta
from airflow.models import Variable

yesterday = datetime.combine(datetime.today() - timedelta(days=1), datetime.min.time())
timestamp_str = yesterday.strftime('%Y-%m-%dT%H:%M:%S') # for sql timestamp 

default_args = {
    'owner': 'groot',
    'depends_on_past': False,
    'start_date': yesterday,
    'projectid': Variable.get('projectid'),
    'objectid': Variable.get('objectid'),
    'datasetid': Variable.get('datasetid'),
    'datatable': Variable.get('datatable'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(hours=12)
}

dag = DAG(
    'download_from_gcs_upload_to_bq',
    default_args=default_args,
    description=f"Retrieve {default_args.get('objectid')} from GCS and load into BigQuery",
    schedule_interval=timedelta(hours=24),
)

download_data = GCSToLocalFilesystemOperator(
    task_id = 'download_data',
    bucket = default_args.get('projectid'),
    object_name = default_args.get('objectid'),
    filename='/opt/bitnami/airflow/tmp/' + default_args.get('objectid'),
    dag=dag
    # google_cloud_storage_conn_id='google_conn_default' - is used by default
)

# not needed here
# create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id='gcpzoomcamp')

create_table = BigQueryCreateEmptyTableOperator(
    task_id = "create_table",
    dataset_id = default_args.get('datasetid'),
    table_id = default_args.get('datatable'),
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
        df = pd.read_parquet('/opt/bitnami/airflow/tmp/' + default_args.get('objectid'))
        # not needed, but lets prep 
        # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
        # df["passenger_count"].fillna(0, inplace=True)
        # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
        print(f"dataset rows total: {len(df)}")

        df.to_gbq(destination_table=f"{default_args.get('datasetid')}.{default_args.get('datatable')}",
                  if_exists = 'replace', # append
                  project_id = default_args.get('projectid'))

upload_data = PythonOperator(
    task_id = 'upload_data',
    python_callable = load_preped_parquet_to_bq,
    dag = dag
)

add_column_ds = BigQueryOperator(
    task_id = 'add_column_ds',
    sql = f'''ALTER TABLE {default_args.get('datasetid')}.{default_args.get('datatable')} ADD COLUMN ds DATE''',
    use_legacy_sql = False,
    dag = dag
)

set_default_ds = BigQueryOperator(
    task_id = 'set_default_ds',
    sql = f'''ALTER TABLE {default_args.get('datasetid')}.{default_args.get('datatable')} ALTER COLUMN ds SET DEFAULT CURRENT_DATE()''',
    use_legacy_sql = False,
    dag = dag,
    trigger_rule = 'all_done'
)

update_ds = BigQueryOperator(
    task_id = 'update_ds',
    sql = f'''UPDATE {default_args.get('datasetid')}.{default_args.get('datatable')} SET ds = CURRENT_DATE() WHERE TRUE''',
    use_legacy_sql = False,
    dag = dag,
    trigger_rule = 'all_done'
)

partition_table = BigQueryOperator(
    task_id = 'partition_table',
    sql = f"SELECT * FROM {default_args.get('datasetid')}.{default_args.get('datatable')}",
    destination_dataset_table = f"{default_args.get('datasetid')}.{default_args.get('datatable')}_partitioned",
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