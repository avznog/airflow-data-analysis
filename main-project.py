from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from tasks.scrapFromWebPrimary import scrapFromWebPrimary
from tasks.scrapFromWebSecond import scrapFromWebSecond
from tasks.createRequestPrimary import createRequestPrimary
from tasks.createRequestSecond import createRequestSecond


with DAG(
	dag_id="main-project",
	schedule=None,
	start_date=days_ago(0)
) as dag:

	def sendToPg(ti):
		d = ti.xcom_pull(key="insert_request", task_ids="create_request")
		print(d)

	scrap_from_web_primary = PythonOperator(
		task_id='scrap_from_web_primary',
		python_callable=scrapFromWebPrimary
	)

	scrap_from_web_second = PythonOperator(
		task_id='scrap_from_web_second',
		python_callable=scrapFromWebSecond
	)

	create_request_primary = PythonOperator(
		task_id='create_request_primary',
		python_callable=createRequestPrimary
	)

	create_request_second = PythonOperator(
		task_id='create_request_second',
		python_callable=createRequestSecond
	)

	send_to_pg_primary = PostgresOperator(
		task_id="send_to_pg_primary",
		postgres_conn_id="from_web_pg",
		sql=
		"""
		{{ti.xcom_pull(key="insert_request", task_ids="create_request_primary")}}
		"""
	)

	send_to_pg_second = PostgresOperator(
		task_id="send_to_pg_second",
		postgres_conn_id="from_web_pg",
		sql=
		"""
		{{ti.xcom_pull(key="insert_request", task_ids="create_request_second")}}
		"""
	)
	 

	scrap_from_web_primary >> create_request_primary >> send_to_pg_primary
	scrap_from_web_second >> create_request_second >> send_to_pg_second
