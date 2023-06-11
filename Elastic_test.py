import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Création du DataFrame fictif
data = {
    'prix_essences': [1.5, 1.8, 1.6, 1.7, 1.9],
    'consommation_e': [8.5, 9.2, 7.8, 8.1, 9.5],
    'Region': ['A', 'B', 'C', 'D', 'E']
}
df = pd.DataFrame(data)

# Configuration Elasticsearch
es = Elasticsearch(['http://localhost:9200'])

# Indexation du DataFrame dans Elasticsearch
index_name = 'test_index'  

actions = [
    {
        '_index': index_name,
        '_source': {
            'prix_essences': row['prix_essences'],
            'consommation_e': row['consommation_e'],
            'Region': row['Region']
        }
    }
    for _, row in df.iterrows()
]

bulk(es, actions) 

# Vérification des documents indexés
res = es.search(index=index_name)
for doc in res['hits']['hits']:
    print(doc['_source'])
