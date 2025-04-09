from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG

default_args = {
    'owner': 'logan',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'start_date': datetime(2025,4,8)
}

with DAG(
    dag_id='two_task_dag',
    description='A dag with 2 tasks, one depending to each other',
    schedule=None,
    default_args=default_args
) as dag:
    task_one = BashOperator(
        task_id='task_one',
        bash_command='echo "FIRST TASK"'
    )

    task_two = BashOperator(
        task_id='task_two',
        bash_command='echo "Sleeping..." && sleep 5 && echo "SECOND TASK"'
    )

    task_one >> task_two
