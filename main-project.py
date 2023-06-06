from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from tasks.scrapFromWeb import scrapFromWeb
from tasks.createRequest import createRequest
    
with DAG(
	dag_id="main-project",
	schedule=None,
	start_date=days_ago(0)
) as dag:

	def sendToPg(ti):
		d = ti.xcom_pull(key="insert_request", task_ids="create_request")
		print(d)

	scrap_from_web = PythonOperator(
		task_id='scrap_from_web',
		python_callable=scrapFromWeb
	)

	create_request = PythonOperator(
		task_id='create_request',
		python_callable=createRequest
	)

	send_to_pg = PostgresOperator(
		task_id="send_to_pg",
		postgres_conn_id="localhost_postgres",
		sql=
		"""
		{{ti.xcom_pull(key="insert_request", task_ids="create_request")}}
		"""

	)

	scrap_from_web >> create_request >> send_to_pg