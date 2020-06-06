import  airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta

tabDays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

default_args = {
    'owner': 'Nitin Ware',
    'depends_on_past': False,
    'retries': 1,
    'provide_context': True,
    'retry_delay': timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(
    'branch_dag',
    default_args=default_args,
    schedule_interval="@once",)

def get_current_day(**kwargs):
    kwargs['ti'].xcom_push(key='day', value=datetime.now().weekday())

def branch(**kwargs):
    return 'task_for_' + tabDays[kwargs['ti'].xcom_pull(task_ids='weekday', key='day')]

get_weekday = PythonOperator(
    task_id='weekday',
    python_callable=get_current_day,
    provide_context=True,
    dag=dag
)

fork = BranchPythonOperator(
    task_id='branching',
    python_callable=branch,
    provide_context=True,
    dag=dag
)

get_weekday.set_downstream(fork)

# One dummy operator for each week day, all branched to the fork
for day in range(0, 7):
    fork.set_downstream(DummyOperator(task_id='task_for_' + tabDays[day], dag=dag))





