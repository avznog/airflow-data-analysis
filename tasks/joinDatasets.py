import pandas as pd

def joinDatasets(ti):

  # # ! getting the dataframes
  # # ? carburants
  dataset_carburants = pd.DataFrame(ti.xcom_pull(key="data_for_join_carburants", task_ids="format_for_mean_primary"))

  # # ? consommation
  dataset_consommation = pd.DataFrame(ti.xcom_pull(key="data_for_join_consommation", task_ids="format_for_mean_secondary"))

  print(dataset_consommation)
  print(dataset_carburants)

  dataset_carburants = dataset_carburants.rename(columns={'code_departement': 'code_departements'})

  dataset_carburants['code_departements'] = dataset_carburants['code_departements'].astype(str)
  dataset_consommation['code_departements'] = dataset_consommation['code_departements'].astype(str)

  joined_df = dataset_carburants.merge(dataset_consommation, on='code_departements', how='outer')
  for col in joined_df.columns:
    if col == 'code_departements':
      joined_df[col].fillna(0, inplace=True)
    else:
      joined_df[col].fillna(0.0, inplace=True)
  
  ti.xcom_push(key="joined_df", value=joined_df)