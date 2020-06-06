import  airflow
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators import bash_operator, dummy_operator, python_operator

default_args = {
    'owner': 'Nitin Ware',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG('ariflow_variables', 'catchup=False', default_args=default_args, schedule_interval="@once",)

def calc():
    var = Variable.get("var2")
    nv = 5 + int(var)
    print(nv)

start_dag = dummy_operator.DummyOperator(
    task_id='start',
    default_args=default_args,
    dag=dag,
)

read_var_bash = bash_operator.BashOperator(
    task_id='read_value_bash',
    bash_command='echo {{var.value.var3}}',
    dag=dag
)

read_var_python = python_operator.PythonOperator(
    task_id='read_value_python',
    python_callable=calc,
    dag=dag
)

end_dag = dummy_operator.DummyOperator(
    task_id='end',
    default_args=default_args,
    dag=dag,
)

start_dag >> [read_var_bash, read_var_python]

end_dag << [read_var_bash, read_var_python]

