import airflow
from airflow import DAG
from airflow.contrib.operators import bigquery_operator
from airflow.operators import dummy_operator

default_args = {
    'owner': 'Nitin Ware',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG('bq_conn_dag', 'catchup=False', default_args=default_args, schedule_interval="@once",)

task_default = bigquery_operator.BigQueryOperator(
        task_id='task_default_connection',
        bql='SELECT 1', use_legacy_sql=False, dag=dag)

task_explicit = bigquery_operator.BigQueryOperator(
    task_id='task_explicit_connection',
    bql='SELECT 1', use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default', dag=dag)

task_custom = bigquery_operator.BigQueryOperator(
    task_id='task_custom_connection',
    bql='SELECT 1', use_legacy_sql=False,
    bigquery_conn_id='my_gcp_connection', dag=dag)

start_task = dummy_operator.DummyOperator(
    task_id='start',
    default_args=default_args,
    dag=dag,
)

start_task >> [task_default, task_explicit, task_custom]


