import requests
from airflow.models import Variable

def scrapFromWebPrimary (ti): 
	# ? url to scrap
	print(Variable.get("count_rows_request_primary"))
	url = f'https://data.economie.gouv.fr/api/records/1.0/search/?dataset=prix-des-carburants-en-france-flux-instantane-v2&q=&lang=fr&rows={Variable.get("count_rows_request_primary")}&facet=departement&facet=region&facet=Gazole_maj&facet=Gazole_prix'

	# ? getting data from api.gouv.fr
	response = requests.get(url)
	
	if response.status_code == 200:
		ti.xcom_push(key="data_primary", value=response.json())