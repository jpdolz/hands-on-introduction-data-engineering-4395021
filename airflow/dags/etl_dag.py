from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

from pathlib import Path
import os
import pandas as pd


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
    dag_id='etl_dag',
    default_args=default_args
) as dag:
    def transform_data():
        input_file = Path(os.getenv('AIRFLOW_HOME'), '../lab/orchestrated/airflow_extract_data.csv')
        output_file = Path(os.getenv('AIRFLOW_HOME'), '../lab/orchestrated/airflow_transform_data.csv')
        data = pd.read_csv(input_file)
        generic_data = data[data.Type == 'generic'].reset_index(drop=True)
        generic_data['Date'] = datetime.today().strftime('%Y-%m-%d')
        generic_data.to_csv(output_file, index=False)
    
    extract_task = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://datahub.io/core/top-level-domain-names/r/top-level-domain-names.csv.csv -O $AIRFLOW_HOME/../lab/orchestrated/airflow_extract_data.csv'
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
    )

    load_task = BashOperator(
        task_id='load_task',
        bash_command='''
        echo -e ".mode csv \n.import --skip 1 $AIRFLOW_HOME/../lab/orchestrated/airflow_transform_data.csv top_level_domains" | sqlite3 $AIRFLOW_HOME/../lab/orchestrated/airflow-load-db.db
        '''
    )

    extract_task >> transform_task >> load_task
