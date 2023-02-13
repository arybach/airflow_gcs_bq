import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# to trigger with config
"""
{
"color": "green",
"year": 2020,
"month": 3
}
"""

today = datetime.combine(datetime.today(), datetime.min.time())
timestamp_str = today.strftime('%Y-%m-%dT%H:%M:%S') # for sql timestamp 

# def trigger_dag_run_op(**kwargs):
#     return {"dag_run_obj": "dag_run_obj_value"}


def trigger_url_to_gcs_dag(context):
    ds = context['execution_date'].strftime('%Y-%m-%d')
    trigger_run_task = TriggerDagRunOperator(
        task_id='trigger_url_to_gcs_dag',
        trigger_dag_id='nyc_tlc_data_to_gcs_bucket',
        dag=dag,
        conf={ 'ds': ds },
    )
    trigger_run_task.execute(context)        

default_args = {
    'owner': 'groot',
    'depends_on_past': False,
    'start_date': today,
    'retries': 0,
    'on_success_callback': trigger_url_to_gcs_dag,
}

dag = DAG(
    dag_id="args_to_var_for_url",
    tags=["args_for_url_to_gcs_pipeline"],
    schedule_interval=None,
    default_args=default_args,
    catchup=True
)

# double triggered...
# trigger_dag_run = PythonOperator(
#     task_id='trigger_dag_run',
#     python_callable=trigger_dag_run_op,
#     dag=dag
# )

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
