import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.models import Variable

"""
{
  "projectid": "gcpzoomcamp",
  "objectid": "green_tripdata_2020-02.parquet",
  "datasetid": "green_taxi",
  "datatable": "trip_data"
}
"""
today = datetime.combine(datetime.today(), datetime.min.time())
timestamp_str = today.strftime('%Y-%m-%dT%H:%M:%S') # for sql timestamp 

# def trigger_dag_run_op(**kwargs):
#     return {"dag_run_obj": "dag_run_obj_value"}

def trigger_gcs_to_bq_dag(context):
    ds = context['execution_date'].strftime('%Y-%m-%d')
    trigger_run_task = TriggerDagRunOperator(
        task_id = 'trigger_gcs_to_bq_dag',
        trigger_dag_id = 'download_from_gcs_upload_to_bq',
        dag=dag,
        conf={ 'ds': ds },
    )
    trigger_run_task.execute(context)    

default_args = {
    'owner': 'groot',
    'depends_on_past': False,
    'start_date': today,
    'retries': 0,
    'on_success_callback': trigger_gcs_to_bq_dag
}

dag = DAG(
    dag_id="args_to_var_for_gcs",
    tags=["args_for_gcs_to_bq_pipeline"],
    schedule_interval=None,
    default_args=default_args
)

# double-triggered ...
# trigger_dag_run = PythonOperator(
#     task_id='trigger_dag_run',
#     python_callable=trigger_dag_run_op,
#     dag=dag
# )

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
