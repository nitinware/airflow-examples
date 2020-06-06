import airflow
from airflow import DAG
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.operators import dummy_operator

default_args = {
    'owner': 'Nitin Ware',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
}

output_file = 'gs://us-east1-example-environmen-3bb7fd7a-bucket/data/product.csv'

dag = DAG('bq_export_dag', 'catchup=False', default_args=default_args, schedule_interval="@once",)

start_task = dummy_operator.DummyOperator(
    task_id='start',
    default_args=default_args,
    dag=dag,
)

bq_query_task = bigquery_operator.BigQueryOperator(
    task_id='bq_query',
    bql='''SELECT * FROM [hd-personalization-dev.temp.Prod] ''',
    dag=dag
)

export_to_gcs_task = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_to_gcs',
    source_project_dataset_table='hd-personalization-dev.temp.Prod',
    destination_cloud_storage_uris=[output_file],
    export_format='CSV',
    dag=dag
)

start_task >> bq_query_task >> export_to_gcs_task

