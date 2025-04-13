from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow import DAG

from datetime import timedelta

with DAG(
    dag_id='fourth_multiple_task_dag',
    default_args={'owner': 'logan', 'start_date': days_ago(1)},
    schedule=timedelta(days=1),
    tags=['bash'],
    template_searchpath='/workspaces/hands-on-introduction-data-engineering-4395021/airflow/dags/bash_scripts'
) as dag:
    task_a = BashOperator(
        task_id='task_a',
        bash_command='task_a.sh'
    )

    task_b = BashOperator(
        task_id='task_b',
        bash_command='task_b.sh'
    )

    task_c = BashOperator(
        task_id='task_c',
        bash_command='task_c.sh'
    )

    task_d = BashOperator(
        task_id='task_d',
        bash_command='task_d.sh'
    )

    task_e = BashOperator(
        task_id='task_e',
        bash_command='task_e.sh'
    )

    task_f = BashOperator(
        task_id='task_f',
        bash_command='task_f.sh'
    )

    task_g = BashOperator(
        task_id='task_g',
        bash_command='task_g.sh'
    )


task_a >> task_b >> task_e

task_a >> task_c >> task_f

task_a >> task_d >> task_g