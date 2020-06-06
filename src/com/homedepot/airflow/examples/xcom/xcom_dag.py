import  airflow
import datetime
from airflow import DAG
from airflow.operators import dummy_operator, python_operator

default_args = {
    'owner': 'Nitin Ware',
    'depends_on_past': False,
    'retries': 1,
    'provide_context': True,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG('xcom_dag', 'catchup=False', default_args=default_args, schedule_interval="@once",)

value_1 = [1, 2, 3]
value_2 = {'a': 'b'}

def push(**kwargs):
    print(kwargs)
    # pushes an XCom without a specific target
    kwargs['ti'].xcom_push(key='value from pusher 1', value=value_1)

def push_by_returning(**kwargs):
    return value_2

def puller(**kwargs):
    ti = kwargs['ti']

    # get value_1
    v1 = ti.xcom_pull(key=None, task_ids='push')
    assert v1 == value_1

    # get value_2
    v2 = ti.xcom_pull(task_ids='push_by_returning')
    assert v2 == value_2

    v1, v2 = ti.xcom_pull(key=None, task_ids=['push', 'push_by_returning'])
    print('value V1', v1)
    print('value V2', v2)
    assert (v1, v2) == (value_1, value_2)

start_dag = dummy_operator.DummyOperator(
    task_id='start',
    default_args=default_args,
    dag=dag,
)

push1 = python_operator.PythonOperator(
    task_id='push',
    dag=dag,
    python_callable=push,
)

push2 = python_operator.PythonOperator(
    task_id='push_by_returning',
    dag=dag,
    python_callable=push_by_returning,
)

pull = python_operator.PythonOperator(
    task_id='puller',
    dag=dag,
    python_callable=puller,
)

end_dag = dummy_operator.DummyOperator(
    task_id='end',
    default_args=default_args,
    dag=dag,
)

start_dag >> [push1, push2]

pull << [push1, push2]

pull >> end_dag

