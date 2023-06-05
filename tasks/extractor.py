import requests
import json

def extractor (): 
    url = 'https://data.economie.gouv.fr/api/records/1.0/search/?dataset=prix-des-carburants-en-france-flux-instantane-v2&q=&lang=fr&rows=1&facet=departement&facet=region&facet=Gazole_maj&facet=Gazole_prix'

    response = requests.get(url)

    # Vérifiez que la requête a réussi
    if response.status_code == 200:
        data = response.json()  # convertit la réponse en JSON
        print("dataaaaaaa", data)

    else:
        print(f"Request failed with status code {response.status_code}")

