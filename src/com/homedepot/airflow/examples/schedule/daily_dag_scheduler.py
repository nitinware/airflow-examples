import  airflow
import datetime
from airflow import DAG
from airflow.operators import python_operator, dummy_operator

default_args = {
    'owner': 'Nitin Ware',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG('daily_dag', 'catchup=False', default_args=default_args, schedule_interval="@daily",)

start_dag = dummy_operator.DummyOperator(
    task_id='start',
    default_args=default_args,
    dag=dag,
)

def print_dag_info(**kwargs):
    context = kwargs
    print("Dag: ", context['dag_run'].dag_id)
    print("Task: ", context['task'].task_id)
    print("Current Date Time: ", datetime.datetime.now())

daily_dag = python_operator.PythonOperator(
    task_id='daily_dag',
    python_callable=print_dag_info,
    provide_context=True,
    default_args=default_args,
    dag=dag,
)

start_dag.set_downstream(daily_dag)
