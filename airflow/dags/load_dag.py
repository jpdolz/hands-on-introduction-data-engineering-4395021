from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG

default_args = {
    'owner': 'logan',
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0,
    'catchup': False,
    'start_date': datetime(2025,4,9)
}

with DAG(
    dag_id='load_dag',
    description='Dag for loading the transformed data into a SQLITE table',
    schedule=None,
    default_args=default_args
) as dag:
    load_task = BashOperator(
        task_id='load_task',
        bash_command='''
        echo -e ".mode csv \n.import --skip 1 $AIRFLOW_HOME/../lab/orchestrated/airflow_transform_data.csv top_level_domains" | sqlite3 $AIRFLOW_HOME/../lab/orchestrated/airflow-load-db.db
        ''',
        dag=dag
    )