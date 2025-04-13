from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow import DAG

with DAG(
    dag_id='third_four_task_dag',
    default_args={'owner':'logan', 'start_date': days_ago(1), 'retries':0},
    schedule=timedelta(days=1),
    tags=['bash']
) as dag:
    task_one = BashOperator(
        task_id='task_one',
        bash_command='''
            echo Task ONE begin
            for number in `seq 10`; do
                echo TASK ONE printing $number
            done
            echo TASK ONE end
        '''
    )

    task_two=BashOperator(
        task_id='task_two',
        bash_command='''
            echo TASK TWO begin
            sleep 5
            echo TASK TWO end
        '''
    )

    task_three=BashOperator(
        task_id='task_three',
        bash_command='''
            echo TASK THREE begin
            sleep 15
            echo TASK THREE end
        '''
    )

    task_four=BashOperator(
        task_id='task_four',
        bash_command='echo TASK FOUR completed!!!!'
    )

# RARELY USED IN AIRFLOW
# task_one.set_downstream(task_two)
# task_one.set_downstream(task_three)
# 
# task_four.set_upstream(task_two)
# task_four.set_upstream(task_three)

# WE USE NORMALLY THE BIGSHIFT OPERATOR (task_two and task_three are executed in parallel)
task_one >> [task_two, task_three]
task_four << [task_two, task_three]

# SEQUENTIAL EXECUTION
# task_one >> task_two >> task_three >> task_four