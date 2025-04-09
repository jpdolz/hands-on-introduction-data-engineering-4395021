from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG

import pandas as pd
from pathlib import Path
import os


default_args = {
    'owner': 'logan',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'start_date': datetime(2025,4,8)
}

with DAG(
    dag_id='transform_dag',
    description='A DAG for transforming downloaded data',
    schedule=None,
    default_args=default_args,
) as dag:
    
    def transform_data():
        input_file = Path(os.getenv('AIRFLOW_HOME'), '../lab/orchestrated/airflow_extract_data.csv')
        output_file = Path(os.getenv('AIRFLOW_HOME'), '../lab/orchestrated/airflow_transform_data.csv')
        data = pd.read_csv(input_file)
        generic_data = data[data.Type == 'generic'].reset_index(drop=True)
        generic_data['Date'] = datetime.today().strftime('%Y-%m-%d')
        generic_data.to_csv(output_file, index=False)

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        dag=dag
    )