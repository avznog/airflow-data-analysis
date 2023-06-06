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
    temp = data[i]["fields"]

    if "region" in temp and "gazole_maj" in temp and "gazole_prix" in temp and "ville" in temp and "code_region" in temp and "code_departement" in temp and "longitude" in temp and "latitude" in temp and "cp" in temp and "departement" in temp and "carburants_indisponibles" in temp and "geom" in temp and "prix" in temp and "carburants_disponibles" in temp and "adresse" in temp:
      element = {
      "region": temp["region"],
      "gazole_maj": temp["gazole_maj"],
      "gazole_prix": temp["gazole_prix"],
      "ville": temp["ville"],
      "code_region": temp["code_region"],
      "code_departement": temp["code_departement"],
      "longitude": temp["longitude"],
      "latitude": temp["latitude"],
      "cp": temp["cp"],
      "departement": temp["departement"],
      "carburants_indisponibles": temp["carburants_indisponibles"],
      "geomi": temp["geom"][0],
      "geom1": temp["geom"][1],
      "prix": temp["prix"],
      "carburants_disponibles": temp["carburants_disponibles"],
      "adresse": temp["adresse"]
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