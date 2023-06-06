import psycopg2 as p
import pandas as pd
import json

def createRequest(ti):
  request = ""
  # ? connection do db
  elements = []

  # ? reading the data
  data = ti.xcom_pull(key="data", task_ids="scrap_from_web")
  data = data["records"]

  # ? setting the elements array with all the data
  for i in range(0, len(data)):
    element = {
      "region": data[i]["fields"]["region"],
      "gazole_maj": data[i]["fields"]["gazole_maj"],
      "gazole_prix": data[i]["fields"]["gazole_prix"],
      "ville": data[i]["fields"]["ville"],
      "code_region": data[i]["fields"]["code_region"],
      "code_departement": data[i]["fields"]["code_departement"],
      "longitude": data[i]["fields"]["longitude"],
      "latitude": data[i]["fields"]["latitude"],
      "cp": data[i]["fields"]["cp"],
      "departement": data[i]["fields"]["departement"],
      "carburants_indisponibles": data[i]["fields"]["carburants_indisponibles"],
      "geomi": data[i]["fields"]["geom"][0],
      "geom1": data[i]["fields"]["geom"][1],
      "prix": data[i]["fields"]["prix"],
      "carburants_disponibles": data[i]["fields"]["carburants_disponibles"],
      "adresse": data[i]["fields"]["adresse"]
    }
    elements.append(element)
    
  columns = ""

  for key in elements[0]:
    columns += ", " + key + ' varchar(1000)'
  
  request += f""" create table if not exists data_carburants
    (id serial primary key
    {columns}
    ); """
  
  # ? creating the table if it does not exists

  # ? for each element, insert in the db 
  for el in elements:
    columnNames = ", ".join(el.keys())
    valueNames = ""
    for key in el:
      valueNames+= ", '" + str(el[key]).replace("'", " ") + "'"
    valueNames = valueNames[1:]
    request += f" insert into data_carburants ({columnNames}) values ({valueNames}); " 
    ti.xcom_push(key="insert_request", value=request)
  return request