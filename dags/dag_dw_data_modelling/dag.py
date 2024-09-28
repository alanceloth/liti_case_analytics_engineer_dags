# dags/dag_test.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}

with DAG('dag_test', default_args=default_args, schedule_interval='@daily') as dag:
    run_this = BashOperator(
        task_id='print_date',
        bash_command='date',
    )
