import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import json
from tasks.extractor import extractor
    
def execute_task_2():
    print("Executinyg!uhijopoklpg Task 2")

with DAG(
    dag_id="main-project",
    schedule=None,
    start_date=days_ago(0)
) as dag:

    extraction = PythonOperator(
        task_id='extractor',
        python_callable=extractor
    )

    # task_2 = PythonOperator(
    #     task_id='task_2',
    #     python_callable=execute_task_2
    # )

    # task_3 = PythonOperator(
    #     task_id='task_3',
    #     python_callable=execute_task_3
    # )

    # extraction >> task_2 >> task_3  # Définit l'ordre d'exécution des tâches

    extraction