import pandas as pd

def joinDatasets(ti):

  # # ! getting the dataframes
  # # ? carburants
  dataset_carburants = pd.DataFrame(ti.xcom_pull(key="data_for_join_carburants", task_ids="format_for_mean_primary"))

  # # ? consommation
  dataset_consommation = pd.DataFrame(ti.xcom_pull(key="data_for_join_consommation", task_ids="format_for_mean_secondary"))

  dataset_carburants = dataset_carburants.rename(columns={'code_departement': 'code_departements'})

  dataset_carburants['code_departements'] = dataset_carburants['code_departements'].astype(str)
  dataset_consommation['code_departements'] = dataset_consommation['code_departements'].astype(str)

  joined_df = dataset_carburants.merge(dataset_consommation, on='code_departements', how='outer')

  # ? purging NaN values from dataframe
  for col in joined_df.columns:
    if col == 'code_departements':
      joined_df[col].fillna(0, inplace=True)
    else:
      joined_df[col].fillna(0.0, inplace=True)
  

  # ? purging rows that have multiple departements
  for index, row in joined_df.iterrows():
    # Check if the 'code_departement' is not numeric
    if not row['code_departements'].isnumeric():
        # Delete the row
        joined_df = joined_df.drop(index)

  # Reset the index of the DataFrame
  joined_df = joined_df.reset_index(drop=True)
  ti.xcom_push(key="joined_df", value=joined_df)