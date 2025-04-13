from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
import requests
import os
from pathlib import Path
import pandas as pd


default_args = {
    'owner': 'logan',
    'email_on_retry': False,
    'email_on_failure': False,
    'depends_on_past': False,
    'retries': 0,
    'catchup': False,
    'start_date': datetime(2025,4,8)
}

with DAG(
    dag_id='challenge_dag',
    default_args=default_args,
    schedule=None,
) as dag:
    
    extract_file = Path(os.getenv('AIRFLOW_HOME'), '../lab/challenge/extract_data.csv')
    transform_file = Path(os.getenv('AIRFLOW_HOME'), '../lab/challenge/transform_data.csv')

    def extract_data():
        url = 'https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/constituents.csv'
        data = requests.get(url=url)
        open(extract_file, 'w').write(data.text.strip())
        
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
        dag=dag
    )

    def transform_data():
        df = pd.read_csv(extract_file)
        data = df.groupby('Sector').Symbol.count().reset_index()
        data.rename({'Symbol': 'Count'}, inplace=True)
        data['Date'] = datetime.today().strftime('%Y-%m-%d')
        data.to_csv(transform_file, index=False)


    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        dag=dag
    )

    load_task = BashOperator(
        task_id='load_task',
        bash_command='''
        echo -e ".mode csv \n.import --skip 1 $AIRFLOW_HOME/../lab/challenge/transform_data.csv sp_500_sector_count" | sqlite3 $AIRFLOW_HOME/../lab/challenge/challenge-load-db.db
        ''',
        dag=dag
    )

    extract_task >> transform_task >> load_task
