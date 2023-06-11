import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import Elasticsearch, RequestError, TransportError
from airflow.models import Variable
def indexDataElastic():
  
  # Define the mapping

  df = pd.DataFrame({'code_departements': {'0': '1', '1': '10', '2': '11', '3': '12', '4': '13', '5': '14', '6': '21', '7': '24', '8': '25', '9': '27', '10': '28', '11': '29', '12': '33', '13': '35', '14': '36', '15': '40', '16': '41', '17': '44', '18': '45', '19': '56', '20': '57', '21': '58', '22': '59', '23': '60', '24': '62', '25': '63', '26': '64', '27': '65', '28': '67', '29': '68', '30': '69', '31': '71', '32': '72', '33': '73', '34': '77', '35': '78', '36': '79', '37': '81', '38': '82', '39': '83', '40': '84', '41': '92', '42': '94', '43': '95', '44': '07', '45': '15', '46': '18', '47': '18, 36', '48': '23', '49': '2A', '50': '30, 34', '51': '31', '52': '32', '53': '34', '54': '37', '55': '38', '56': '39', '57': '42', '58': '43', '59': '44, 49', '60': '44, 56', '61': '47', '62': '52', '63': '53', '64': '64, 65', '65': '66', '66': '70', '67': '74', '68': '85', '69': '86', '70': '91'}, 'gazole_prix': {'0': 1.655, '1': 1.6785, '2': 1.649, '3': 1.639, '4': 1.619, '5': 1.6615, '6': 1.664, '7': 1.662, '8': 1.6853333333333333, '9': 1.6564999999999999, '10': 1.647, '11': 1.633, '12': 1.659, '13': 1.648, '14': 1.67475, '15': 1.651, '16': 1.659, '17': 1.7073333333333334, '18': 1.659, '19': 1.6840000000000002, '20': 1.675, '21': 1.719, '22': 1.639, '23': 1.667, '24': 1.639, '25': 1.669, '26': 1.6600000000000001, '27': 1.653, '28': 1.67425, '29': 1.689, '30': 1.699, '31': 1.6556666666666668, '32': 1.639, '33': 1.672, '34': 1.54, '35': 1.7300000000000002, '36': 1.615, '37': 1.649, '38': 1.611, '39': 1.6665, '40': 1.649, '41': 1.689, '42': 1.789, '43': 1.679, '44': 0.0, '45': 0.0, '46': 0.0, '47': 0.0, '48': 0.0, '49': 0.0, '50': 0.0, '51': 0.0, '52': 0.0, '53': 0.0, '54': 0.0, '55': 0.0, '56': 0.0, '57': 0.0, '58': 0.0, '59': 0.0, '60': 0.0, '61': 0.0, '62': 0.0, '63': 0.0, '64': 0.0, '65': 0.0, '66': 0.0, '67': 0.0, '68': 0.0, '69': 0.0, '70': 0.0}, 'sp95_prix': {'0': 1.858, '1': 1.876, '2': 1.839, '3': 1.829, '4': 1.85, '5': 1.8765, '6': 1.88, '7': 1.864, '8': 1.854, '9': 1.874, '10': 1.846, '11': 1.825, '12': 1.847, '13': 1.847, '14': 1.8785, '15': 1.858, '16': 1.875, '17': 1.888, '18': 1.839, '19': 1.8639999999999999, '20': 1.829, '21': 1.929, '22': 1.779, '23': 1.859, '24': 1.849, '25': 1.889, '26': 1.8623333333333332, '27': 1.898, '28': 1.85225, '29': 1.839, '30': 1.8940000000000001, '31': 1.8323333333333334, '32': 1.859, '33': 1.865, '34': 1.90575, '35': 1.8536666666666666, '36': 1.82, '37': 1.849, '38': 1.797, '39': 1.855, '40': 1.859, '41': 1.919, '42': 1.949, '43': 1.879, '44': 0.0, '45': 0.0, '46': 0.0, '47': 0.0, '48': 0.0, '49': 0.0, '50': 0.0, '51': 0.0, '52': 0.0, '53': 0.0, '54': 0.0, '55': 0.0, '56': 0.0, '57': 0.0, '58': 0.0, '59': 0.0, '60': 0.0, '61': 0.0, '62': 0.0, '63': 0.0, '64': 0.0, '65': 0.0, '66': 0.0, '67': 0.0, '68': 0.0, '69': 0.0, '70': 0.0}, 'e10_prix': {'0': 1.779, '1': 1.8375, '2': 1.809, '3': 1.779, '4': 1.81, '5': 1.829, '6': 1.821, '7': 1.853, '8': 1.8103333333333333, '9': 1.8375, '10': 1.804, '11': 1.783, '12': 1.799, '13': 1.7985, '14': 1.8335, '15': 1.802, '16': 1.8229999999999997, '17': 1.8626666666666667, '18': 1.819, '19': 1.837, '20': 1.779, '21': 1.899, '22': 1.819, '23': 1.8355000000000001, '24': 1.809, '25': 1.859, '26': 1.8356666666666666, '27': 1.838, '28': 1.80525, '29': 1.795, '30': 1.8639999999999999, '31': 1.7990000000000002, '32': 1.8039999999999998, '33': 1.826, '34': 1.867, '35': 1.8786666666666667, '36': 1.795, '37': 1.779, '38': 1.749, '39': 1.8165, '40': 1.799, '41': 1.869, '42': 1.919, '43': 1.819, '44': 0.0, '45': 0.0, '46': 0.0, '47': 0.0, '48': 0.0, '49': 0.0, '50': 0.0, '51': 0.0, '52': 0.0, '53': 0.0, '54': 0.0, '55': 0.0, '56': 0.0, '57': 0.0, '58': 0.0, '59': 0.0, '60': 0.0, '61': 0.0, '62': 0.0, '63': 0.0, '64': 0.0, '65': 0.0, '66': 0.0, '67': 0.0, '68': 0.0, '69': 0.0, '70': 0.0}, 'sp98_prix': {'0': 1.876, '1': 1.9035, '2': 1.899, '3': 1.8790000000000002, '4': 1.87, '5': 1.902, '6': 1.891, '7': 1.912, '8': 1.8860000000000001, '9': 1.884, '10': 1.871, '11': 1.856, '12': 1.879, '13': 1.887, '14': 1.913, '15': 1.913, '16': 1.899, '17': 1.9193333333333333, '18': 1.869, '19': 1.929, '20': 1.885, '21': 1.949, '22': 1.889, '23': 1.9140000000000001, '24': 1.889, '25': 1.929, '26': 1.8883333333333334, '27': 1.918, '28': 1.89125, '29': 1.895, '30': 1.924, '31': 1.8663333333333334, '32': 1.904, '33': 1.899, '34': 1.947, '35': 1.9646666666666668, '36': 1.86, '37': 1.879, '38': 1.848, '39': 1.8820000000000001, '40': 1.919, '41': 1.959, '42': 1.979, '43': 1.899, '44': 0.0, '45': 0.0, '46': 0.0, '47': 0.0, '48': 0.0, '49': 0.0, '50': 0.0, '51': 0.0, '52': 0.0, '53': 0.0, '54': 0.0, '55': 0.0, '56': 0.0, '57': 0.0, '58': 0.0, '59': 0.0, '60': 0.0, '61': 0.0, '62': 0.0, '63': 0.0, '64': 0.0, '65': 0.0, '66': 0.0, '67': 0.0, '68': 0.0, '69': 0.0, '70': 0.0}, 'consot': {'0': 0.0, '1': 0.0, '2': 0.0, '3': 6307.642, '4': 130447.575, '5': 107212.19499999999, '6': 32291.17, '7': 7785.02, '8': 13084.741999999998, '9': 0.0, '10': 0.0, '11': 347532.005, '12': 37803.36, '13': 19563.39, '14': 3028.93, '15': 11751.9, '16': 5324.53, '17': 29343.26, '18': 15697.06, '19': 17768.62, '20': 14985.87, '21': 0.0, '22': 535950.025, '23': 90899.40333333332, '24': 0.0, '25': 0.0, '26': 12106.45, '27': 0.0, '28': 593624.615, '29': 22307.079999999998, '30': 54546.695, '31': 33260.83000000001, '32': 151108.14, '33': 21506.12, '34': 234211.795, '35': 0.0, '36': 0.0, '37': 101149.05, '38': 0.0, '39': 51064.455, '40': 99846.48, '41': 0.0, '42': 0.0, '43': 22210.04, '44': 1988.5, '45': 6144.915, '46': 17833.62, '47': 70110.13, '48': 14429.585, '49': 0.0, '50': 13278.67, '51': 6071.36, '52': 17895.645, '53': 58934.48, '54': 512095.12, '55': 106289.425, '56': 17860.39, '57': 6342.360000000001, '58': 5806.92, '59': 76846.51999999999, '60': 197214.31, '61': 3788.68, '62': 6569.19, '63': 12789.48, '64': 27763.61, '65': 8848.900000000001, '66': 3191.94, '67': 24890.235, '68': 46414.795, '69': 175357.44, '70': 143681.87}, 'consor': {'0': 0.0, '1': 0.0, '2': 0.0, '3': 35085.666, '4': 347997.18, '5': 255869.81, '6': 114669.46, '7': 69146.57, '8': 56036.683999999994, '9': 0.0, '10': 0.0, '11': 635864.415, '12': 116302.755, '13': 88481.965, '14': 24235.15, '15': 42737.255, '16': 39763.57, '17': 163190.90500000003, '18': 9040.0, '19': 97739.01, '20': 178055.55, '21': 0.0, '22': 1366733.4449999998, '23': 166005.81333333332, '24': 0.0, '25': 0.0, '26': 57700.89, '27': 0.0, '28': 724608.57, '29': 78314.81, '30': 147682.3, '31': 57375.84, '32': 163837.31, '33': 45048.72, '34': 331341.72250000003, '35': 0.0, '36': 0.0, '37': 265726.505, '38': 0.0, '39': 151480.45, '40': 348405.43, '41': 0.0, '42': 0.0, '43': 152757.21000000002, '44': 18317.42, '45': 38241.82, '46': 57311.655, '47': 140591.41999999998, '48': 47204.524999999994, '49': 0.0, '50': 59298.16, '51': 88167.63, '52': 52908.795, '53': 183994.555, '54': 1001113.215, '55': 323987.72, '56': 59584.47, '57': 58436.51, '58': 30718.9, '59': 289931.6, '60': 594757.24, '61': 24635.93, '62': 28177.82, '63': 63475.43, '64': 164107.78999999998, '65': 35893.59, '66': 33707.91, '67': 80808.475, '68': 119490.09, '69': 478877.18999999994, '70': 417186.12}, 'consoi': {'0': 0.0, '1': 0.0, '2': 0.0, '3': 4099.736, '4': 150226.31, '5': 47829.245, '6': 67362.07, '7': 25706.46, '8': 16913.192000000003, '9': 0.0, '10': 0.0, '11': 420706.005, '12': 78263.56499999999, '13': 36505.315, '14': 3150.02, '15': 76308.80500000001, '16': 37849.0, '17': 20201.865, '18': 15315.0, '19': 84978.9, '20': 49883.52, '21': 0.0, '22': 6306965.495, '23': 140108.8533333333, '24': 0.0, '25': 0.0, '26': 14042.12, '27': 0.0, '28': 726458.9975, '29': 55020.465, '30': 32514.475000000002, '31': 9137.869999999999, '32': 171701.38, '33': 235917.75, '34': 993848.5025, '35': 0.0, '36': 0.0, '37': 212574.62999999998, '38': 0.0, '39': 12211.505000000001, '40': 237494.85, '41': 0.0, '42': 0.0, '43': 5386.65, '44': 411.58, '45': 7646.840000000001, '46': 25594.030000000002, '47': 148784.31, '48': 11604.52, '49': 0.0, '50': 3105.16, '51': 1197.09, '52': 10479.365, '53': 27846.29, '54': 383258.03500000003, '55': 221073.635, '56': 13913.16, '57': 24759.22, '58': 4720.929999999999, '59': 381612.89, '60': 116115.84, '61': 2244.84, '62': 6965.39, '63': 44966.59, '64': 47931.02, '65': 26100.8, '66': 12805.805, '67': 25124.747499999998, '68': 62058.945, '69': 366708.44, '70': 45949.34}})
  print(df)
  mapping = {
      "properties": {
          "code_departements": {"type": "integer"},  # Change the mapping type to "text"
          "gazole_prix": {"type": "long"},
          "sp95_prix": {"type": "long"},
          "sp98_prix": {"type": "long"},
          "e10_prix": {"type": "long"},
          "consoi": {"type": "long"},
          "consor": {"type": "long"},
          "consot": {"type": "long"},
      }
  }

  # Connexion à Elasticsearch
  # Password for the 'elastic' user generated by Elasticsearch
  ELASTIC_PASSWORD = "22cLkzs4rn1OIMU7LqCZzahQ"
  # # Found in the 'Manage Deployment' page
  CLOUD_ID = "fe8da02c947645afad26db448035c26f:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQyMmU5ZTc4MGNlZDM0ZjAyYTJmZDQ2NzRlM2Y1N2NjYyQ4ZDkyOTg5M2JmODc0MTcxYjFlZTU3NmM1OWViYzQyZg=="
  # # Create the client instance
  es = Elasticsearch(
      cloud_id=CLOUD_ID,
      basic_auth=("elastic", ELASTIC_PASSWORD)
  )
    
  
  # # Nom de l'index Elasticsearch
  index_name = 'bigdata'
  try:
      # Delete the index if it already exists
      if es.indices.exists(index=index_name):
          es.indices.delete(index=index_name)
          print(f"Deleted existing index: {index_name}")
      # Create the index with the new mapping
      es.indices.create(index=index_name, body={"mappings": mapping})
      print(f"Created index: {index_name}")
  except TransportError as e:
      print(f"Failed to connect to Elasticsearch: {e}")
      raise e
  except Exception as e:
      print(f"Failed to load data to Elasticsearch: {e}")
      raise e
  # Parcours des lignes du DataFrame
  for _, row in df.iterrows():
      # Conversion de la ligne en dict pour l'importation dans Elasticsearch
      document = row.to_dict()
      try:
      # Index the document
          es.index(index=index_name, body=document)
          print(f"Indexed document: {document} -> done")
      except RequestError as e:
          print(f"Failed to index document: {document}")
          print(f"Error message: {e}")
          raise e
  # Fermeture de la connexion à Elasticsearch
  es.close()