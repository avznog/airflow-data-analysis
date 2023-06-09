import psycopg2 as pg
import pandas as pd
from airflow.models import Variable

def formatDataPrimary(ti):
  with pg.connect(
    host=f"{Variable.get('host')}",
    user=f"{Variable.get('user')}",
    password=f"{Variable.get('password')}",
    port=5432,
    database=f"{Variable.get('database')}"
  ) as connection:
    with connection.cursor() as cursor:
      
      cursor.execute("select * from data_carburants")
      data = pd.DataFrame(cursor.fetchall())
      cursor.execute("select * from information_schema.columns where table_name = 'data_carburants'")
      temp = cursor.fetchall()
      columns = []
      for c in temp:
        columns.append(c[3])
      counter = 0
      for col in data.columns:
        data.rename(columns= { col: columns[counter]}, inplace=True)
        counter += 1
      data = data.drop(["region", "ville", "code_region","code_departement", "longitude", "latitude", "carburants_indisponibles", "geomi", "geom1", "prix", "carburants_disponibles", "adresse"], axis=1)
      
      ti.xcom_push(key="data_formated_1", value=data)
    connection.commit()
    return data