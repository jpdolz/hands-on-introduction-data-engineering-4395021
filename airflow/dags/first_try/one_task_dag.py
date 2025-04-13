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
    'start_date': datetime(2025,4,1)
}

with DAG(
    dag_id='one_task_dag',
    description='A one task airflow DAG',
    schedule=None,
    default_args=default_args
) as dag:
    task = BashOperator(
        task_id='one_task',
        bash_command='echo "FOO" > /workspaces/hands-on-introduction-data-engineering-4395021/lab/temp/test_file',
        dag=dag
    )