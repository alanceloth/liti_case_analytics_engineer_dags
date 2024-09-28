# dags/dag1.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def my_task():
    print("Hello from DAG1!")

with DAG('dag1', start_date=datetime(2023, 1, 1), schedule_interval='@daily') as dag:
    task1 = PythonOperator(
        task_id='task1',
        python_callable=my_task
    )
