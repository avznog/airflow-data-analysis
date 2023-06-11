import pandas as pd

def formatForMeanPrimary(ti):
  # ! getting the dataframes
  # ? carburants
  dataset_carburants = pd.DataFrame(ti.xcom_pull(key="data_formated_carburants", task_ids="format_data_primary"))


  # ? carburants
  dataset_carburants[['gazole_prix', 'code_departement', 'sp95_prix', 'e10_prix', 'sp98_prix']] = dataset_carburants[['gazole_prix', 'code_departement', 'sp95_prix', 'e10_prix', 'sp98_prix']].apply(pd.to_numeric, errors='coerce')

  # ? carburants
  df_carburants_sorted = dataset_carburants.sort_values('code_departement')
  df_mean_carburants = df_carburants_sorted.groupby('code_departement')[['gazole_prix', 'sp95_prix', 'e10_prix', 'sp98_prix']].mean().reset_index()
  
  # ? carburants
  df_carburants_mean = pd.DataFrame()
  df_carburants_mean['code_departement'] = df_mean_carburants['code_departement']
  df_carburants_mean['gazole_prix'] = df_mean_carburants['gazole_prix']
  df_carburants_mean['sp95_prix'] = df_mean_carburants['sp95_prix']
  df_carburants_mean['e10_prix'] = df_mean_carburants['e10_prix']
  df_carburants_mean['sp98_prix'] = df_mean_carburants['sp98_prix']

  ti.xcom_push(key="data_for_join_carburants", value = df_carburants_mean)