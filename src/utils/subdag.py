from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

def my_sub_dag(parent_dag_id):

    # Step 1 - define the default parameters for the DAG
    default_args = {
        'owner': 'Nitin Ware',
        'depends_on_past': False,
        'start_date': datetime(2019, 7, 28),
        'email': ['nitinware@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    p_val = Variable.get('v_val')  # Variable passed to registered method

    # Step 2 - Create a DAG object
    my_sub_dag = DAG(dag_id=parent_dag_id + '.' + 'my_sub_dag',
                     schedule_interval='0 0 * * *',
                     default_args=default_args
                     )

    # Step 3 - Define the method to check the condition for branching
    def my_check_condition(**kwargs):
      if int(p_val)>=15 :
        return 'greater_Than_equal_to_15'
      else:
        return 'less_Than_15'

    # Step 4 - Create a Branching task to Register the method in step 3 to the branching API
    checkTask = BranchPythonOperator(
        task_id='check_task',
        python_callable=my_check_condition,  # Registered method
        provide_context=True,
        dag=my_sub_dag
    )

    # Step 5 - Create tasks
    greaterThan15 = BashOperator(
        task_id='greater_Than_equal_to_15',
        bash_command="echo value is greater than or equal to 15",
        dag=my_sub_dag
    )

    lessThan15 = BashOperator(
        task_id='less_Than_15',
        bash_command="echo value is less than 15",
        dag=my_sub_dag
    )

    finalTask = BashOperator(
        task_id='join_task',
        bash_command="echo This is a join",
        trigger_rule=TriggerRule.ONE_SUCCESS,
        dag=my_sub_dag
    )

    # Step 6 - Define the sequence of tasks.
    lessThan15.set_upstream(checkTask)
    greaterThan15.set_upstream(checkTask)
    finalTask.set_upstream([lessThan15, greaterThan15])

    # Step 7 - Return the DAG
    return my_sub_dag


