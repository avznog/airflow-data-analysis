import requests
from airflow.models import Variable

def scrapFromWebSecond (ti): 
	# ? url to scrap
	url = f'https://odre.opendatasoft.com/api/records/1.0/search/?dataset=conso-epci-annuelle&q=&rows={Variable.get("rows")}&facet=annee&facet=libelle_epci&facet=libelle_departements&facet=libelle_regions&facet=e_operateurs&facet=g_operateurs'

	# ? getting data from api.gouv.fr
	response = requests.get(url)
	
	if response.status_code == 200:
		ti.xcom_push(key="data_second", value=response.json())
