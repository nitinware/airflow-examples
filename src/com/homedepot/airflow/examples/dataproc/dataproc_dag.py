import  airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators import dataproc_operator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta
from airflow.utils import trigger_rule

default_args = {
    'owner': 'Nitin Ware',
    'depends_on_past': False,
    'retries': 1,
    'provide_context': True,
    'retry_delay': timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(
    'dataproc_dag',
    default_args=default_args,
    schedule_interval="@once",)


# Create a Cloud Dataproc cluster.
create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    cluster_name='airflow-dataproc-cluster',
    project_id='hd-personalization-dev',
    num_workers=0,
    init_actions_uris=['gs://hd-personalization-dev-data/vdc2136/training/updated/initialization_actions.sh'],
    zone='us-east1-c',
    master_machine_type='n1-standard-8',
    subnetwork_uri='https://www.googleapis.com/compute/v1/projects/hd-personalization-dev/regions/us-east1/subnetworks/batch-us-east1-subnet',
    tags=['all-bastion-ssh','dataproc','cassandra'],
    storage_bucket= 'hd-personalization-dev-batch',
    properties= {'dataproc:dataproc.allow.zero.workers': 'true'},
    dag=dag
)

dataproc_pyspark_submit = dataproc_operator.DataProcPySparkOperator(
    task_id='pyspark_task',
    main='gs://hd-personalization-dev-artifacts/releases/com.homedepot.recommendations/collections-model-training/python-scripts/v0.0.0+16/__main__.py',
    pyfiles=['gs://hd-personalization-dev-artifacts/releases/com.homedepot.recommendations/collections-model-training/python-scripts/v0.0.0+16/collections_model_training-0.0.1-py3.7.egg'],
    arguments=['LSTM_DATAGEN', '--project', 'hd-personalization-dev', '--category', 'AreaRugs',
               '--dupletsData', 'gs://hd-personalization-dev-data/vdc2136/training/duplets/2020-06-01/',
               '--featuresData', 'gs://hd-personalization-dev-data/vdc2136/training/data/AllFeatures.csv',
               '--finalOutputPath', 'gs://hd-personalization-dev-data/vdc2136/training/lstm/2020-06-02/',
               '--appName', 'LSTM_DATA_GEN', '--mode=cluster'],
    job_name='airflow_pyspark_job',
    cluster_name='airflow-dataproc-cluster',
    project_id='hd-personalization-dev',
    dag=dag
)

delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
    task_id='delete_dataproc_cluster',
    cluster_name='airflow-dataproc-cluster',
    # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
    # even if the Dataproc job fails.
    trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
    project_id='hd-personalization-dev',
    dag=dag
)

start_dag = DummyOperator(
    task_id='start',
    default_args=default_args,
    dag=dag,
)

start_dag >> create_dataproc_cluster >> dataproc_pyspark_submit >> delete_dataproc_cluster
