import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

yesterday = datetime.combine(datetime.today() - timedelta(days=1), datetime.min.time())
timestamp_str = yesterday.strftime('%Y-%m-%dT%H:%M:%S') # for sql timestamp 

default_args = {
    'owner': 'groot',
    'depends_on_past': False,
    'start_date': yesterday,
    'retries': 0,
    # set execution day
    'execution_date': datetime.today().strftime('%Y-%m-%d'),
}

dag = DAG(
    dag_id="args_to_var_for_gcs",
    tags=["args_for_gcs_to_bq_pipeline"],
    schedule_interval=None,
    default_args=default_args
)

# 'projectid': 'gcpzoomcamp',
# 'objectid': 'green_tripdata_2020-01.parquet',
# 'datasetid': 'green_taxi',
# 'datatable': 'trip_data',

dag.trigger_arguments = {
    "projectid": "string",
    "objectid": "string",
    "datasetid": "string",
    "datatable": "string"
}

def parse_job_args_fn(**kwargs):
    dag_run_conf = kwargs["dag_run"].conf
    projectid = dag_run_conf.get("projectid", 'gcpzoomcamp')
    objectid = dag_run_conf.get("objectid", 'green_tripdata_2020-01.parquet')
    datasetid = dag_run_conf.get("datasetid", 'green_taxi')
    datatable = dag_run_conf.get("datatable", 'trip_data')

    Variable.set("projectid", projectid)
    Variable.set("objectid", objectid)
    Variable.set("datasetid", datasetid)
    Variable.set("datatable", datatable)
    

parse_job_args_task = PythonOperator(
    task_id="parse_job_args",
    python_callable=parse_job_args_fn,
    provide_context=True,
    dag=dag
)
