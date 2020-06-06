from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.subdag_operator import SubDagOperator
from utils.subdag import my_sub_dag

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2020, 6, 4),
  'email': ['nitinware@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),

}

# Step 2 - Create a DAG object
dag = DAG(  'sub_dag_demo', schedule_interval='0 0 * * *' , default_args=default_args)

# Step 3 - Create tasks
some_task = BashOperator(
  task_id= 'someTask',
  bash_command="echo I am some task",
  dag=dag
)

sub_dag = SubDagOperator(
    subdag=my_sub_dag('sub_dag_demo'),
    task_id='my_sub_dag',
    dag=dag
)

final_task = BashOperator(
  task_id= 'finalTask',
  bash_command="echo I am the final task",
  dag=dag
)

# Step 4 - Define the sequence of tasks.
sub_dag.set_upstream(some_task)
final_task.set_upstream(sub_dag)





