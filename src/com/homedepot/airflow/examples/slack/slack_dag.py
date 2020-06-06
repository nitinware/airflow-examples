import  airflow
import datetime
from airflow import DAG
from airflow.operators import dummy_operator, python_operator

from utils.slack_alert import task_fail_slack_alert

default_args = {
    'owner': 'Nitin Ware',
    'depends_on_past': False,
    'retries': 0,
    'provide_context': True,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(1),
}

slack_dag = DAG('slack_dag', 'catchup=False', default_args=default_args, on_failure_callback=task_fail_slack_alert, schedule_interval="@once",)

def div_method(**kwargs):
    print(kwargs)
    nv = 0 / 0
    print(nv)

div_by_zero = python_operator.PythonOperator(
    task_id='div_by_zero',
    python_callable=div_method,
    provide_context=True,
    dag=slack_dag,
)

div_by_zero

