from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow import DAG

default_args = {
    'owner': 'logan',
    'retries': 0,
    'catchup': False,
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='simple_one_dag',
    description='Simple dag just with a hello',
    default_args=default_args,
    schedule='@daily',
    tags=['bash', 'beginner']
)

task = BashOperator(
    task_id='hello_task',
    bash_command='echo HELLO!!!!',
    dag=dag
)

task