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
    'start_date': datetime(2025,4,9)
}

with DAG(
    dag_id='extract_dag',
    description='DAG to download CSV file from INET',
    schedule=None,
    default_args=default_args,
) as dag:
    task_wget = BashOperator(
        task_id='task_wget',
        bash_command='wget -c https://datahub.io/core/top-level-domain-names/r/top-level-domain-names.csv.csv -O $AIRFLOW_HOME/../lab/orchestrated/airflow_extract_data.csv',
        dag=dag
    )