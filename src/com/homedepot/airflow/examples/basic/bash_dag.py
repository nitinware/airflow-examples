import airflow
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

dag = DAG('bash_dag', 'catchup=False', default_args=default_args, schedule_interval="@once",)

start_dag = dummy_operator.DummyOperator(
    task_id='start',
    dag=dag,
)

bash_dag = bash_operator.BashOperator(
    task_id='bash_command',
    bash_command='echo Hello Bash.',
    dag=dag
)

start_dag >> bash_dag
