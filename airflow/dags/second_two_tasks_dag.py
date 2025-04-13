from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow import DAG

default_args = {
    'owner': 'logan',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'start_date': days_ago(1),
}

with DAG(
    dag_id='second_two_tasks_dag.py',
    description='Two task dag',
    default_args=default_args,
    schedule='@once',
    tags=['begineer','bash'],
) as dag:
    task_one = BashOperator(
        task_id='task_one',
        bash_command='echo "FIRST TASK"'
    )

    task_two = BashOperator(
        task_id='task_two',
        bash_command='echo "SECOND TASK"'
    )

    # task_one >> task_two

# ONE -> TWO
# task_one.set_downstream(task_two)

# TWO -> ONE
task_one.set_upstream(task_two)
