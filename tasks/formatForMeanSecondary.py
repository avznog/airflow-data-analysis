import pandas as pd

def formatForMeanSecondary(ti):
  # ! getting the dataframes
  
  # ? consommation
  dataset_consommation = pd.DataFrame(ti.xcom_pull(key="data_formated_consommation", task_ids="format_data_secondary"))

  # ! applying numerics for being able to compute the means
  # ? consommation
  dataset_consommation[['consoi', 'consot', 'consor']] = dataset_consommation[['consoi', 'consot', 'consor']].apply(pd.to_numeric, errors='coerce')

  # ! sorting by departement number
  # ? consommation
  df_consommation_sorted = dataset_consommation.sort_values('code_departements')
  df_mean_consommation = df_consommation_sorted.groupby('code_departements')[['consot', 'consor', 'consoi']].mean().reset_index()

  # ! creating the dataframe consommation with the mean
  # ? consommation
  df_consommation_mean = pd.DataFrame()
  df_consommation_mean['code_departements'] = df_mean_consommation['code_departements']
  df_consommation_mean['consot'] = df_mean_consommation['consot']
  df_consommation_mean['consor'] = df_mean_consommation['consor']
  df_consommation_mean['consoi'] = df_mean_consommation['consoi']

  ti.xcom_push(key="data_for_join_consommation", value=df_consommation_mean)