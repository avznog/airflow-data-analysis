from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from tasks.scrapFromWebPrimary import scrapFromWebPrimary
from tasks.scrapFromWebSecond import scrapFromWebSecond
from tasks.createRequestPrimary import createRequestPrimary
from tasks.createRequestSecondary import createRequestSecondary
from tasks.formatDataPrimary import formatDataPrimary
from tasks.formatDataSecondary import formatDataSecondary
from tasks.formatForMeanPrimary import formatForMeanPrimary
from tasks.formatForMeanSecondary import formatForMeanSecondary
from tasks.joinDatasets import joinDatasets


with DAG(
	dag_id="main-project",
	schedule=None,
	start_date=days_ago(0)
) as dag:

	# ? STEP 1 - Scraping from the web

	scrap_from_web_primary = PythonOperator(
		task_id='scrap_from_web_primary',
		python_callable=scrapFromWebPrimary
	)

	scrap_from_web_second = PythonOperator(
		task_id='scrap_from_web_second',
		python_callable=scrapFromWebSecond
	)

	# ? STEP 2 - Creating insert request

	create_request_primary = PythonOperator(
		task_id='create_request_primary',
		python_callable=createRequestPrimary
	)

	create_request_second = PythonOperator(
		task_id='create_request_second',
		python_callable=createRequestSecondary
	)

	# ? STEP 3 - Insert into PG


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

	# ? Pull from PG & Formating data

	format_data_primary = PythonOperator(
		task_id="format_data_primary",
		python_callable=formatDataPrimary
	)

	format_data_secondary = PythonOperator(
		task_id="format_data_secondary",
		python_callable=formatDataSecondary
	)


	# ? compute mean & final format data
	format_for_mean_primary = PythonOperator(
		task_id="format_for_mean_primary",
		python_callable=formatForMeanPrimary
	)

	format_for_mean_secondary = PythonOperator(
		task_id="format_for_mean_secondary",
		python_callable=formatForMeanSecondary
	)

	# ? Join two datasets

	join_datasets = PythonOperator(
		task_id="join_datasets",
		python_callable=joinDatasets
	)

	# ! ----------------- workflow ------------------

	[scrap_from_web_primary >> create_request_primary >> send_to_pg_primary >> format_data_primary >> format_for_mean_primary,
   scrap_from_web_second >> create_request_second >> send_to_pg_second >> format_data_secondary >> format_for_mean_secondary] >> join_datasets
