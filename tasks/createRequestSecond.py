import psycopg2 as p
import pandas as pd
import json

def createRequestSecond(ti):
  request = ""
  # ? connection do db
  elements = []

  # ? reading the data
  data = ti.xcom_pull(key="data_second", task_ids="scrap_from_web_second")
  data = data["records"]

  # ? setting the elements array with all the data
  for i in range(0, len(data)):
    temp = data[i]["fields"]

    if "annee" in temp and "code_departements" in temp and "code_regions" in temp and "operateurs" in temp and "operateurs" in temp and "consoi" in temp and "e_consor" in temp and "libelle_departements" in temp and "consot" in temp and "libelle_regions" in temp and "g_consot" in temp and "g_consototale" in temp and "consor" in temp:
      element = {
      "annee": temp["annee"],
      "code_departements": temp["code_departements"],
      "code_regions": temp["code_regions"],
      "operateurs": temp["operateurs"],
      "consoi": temp["consoi"],
      "e_consor": temp["e_consor"],
      "libelle_departements": temp["libelle_departements"],
      "consot": temp["consot"],
      "libelle_regions": temp["libelle_regions"],
      "g_consot": temp["g_consot"],
      "g_consototale": temp["g_consototale"],
      "consor": temp["consor"]
      }
      elements.append(element)
    
    
  columns = ""

  for key in elements[0]:
    columns += ", " + key + ' varchar(1000)'
  
  request += f""" create table if not exists data_consommation
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
    request += f" insert into data_consommation ({columnNames}) values ({valueNames}); " 
    ti.xcom_push(key="insert_request", value=request)
  return request