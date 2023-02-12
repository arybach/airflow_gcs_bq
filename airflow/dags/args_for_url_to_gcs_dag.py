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
    dag_id="args_to_var_for_url",
    tags=["args_for_url_to_gcs_pipeline"],
    schedule_interval=None,
    default_args=default_args
)

dag.trigger_arguments = {
    "color": "string",
    "year": "int",
    "month": "int"
}

def parse_job_args_fn(**kwargs):
    dag_run_conf = kwargs["dag_run"].conf
    color = dag_run_conf.get("color", "green")
    year = dag_run_conf.get("year", 2020)
    month = dag_run_conf.get("month", 1)
    
    Variable.set("color", color)
    Variable.set("year", year)
    Variable.set("month", month)


parse_job_args_task = PythonOperator(
    task_id="parse_job_args",
    python_callable=parse_job_args_fn,
    provide_context=True,
    dag=dag
)
