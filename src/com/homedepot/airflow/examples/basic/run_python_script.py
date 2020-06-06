import  airflow
import datetime
from airflow import DAG
from airflow.operators import bash_operator, dummy_operator

default_args = {
    'owner': 'Nitin Ware',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG('run_python_script', 'catchup=False', default_args=default_args, schedule_interval="@once",)

py_script_file = '/home/airflow/gcs/data/greetings.py'

start_dag = dummy_operator.DummyOperator(
    task_id='start',
    default_args=default_args,
    dag=dag,
)

call_py_file_task = bash_operator.BashOperator(
    task_id='call_py_file_task',
    bash_command='python ' + py_script_file + ' Airflow',
    dag=dag
)

start_dag.set_downstream(call_py_file_task)

