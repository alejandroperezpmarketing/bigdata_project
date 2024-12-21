import pandas as pd
import os
import numpy as np
import json


def extractdata():

    # 1. verificaciones

    # 1.1 verificar si tenemos documentos en el directorio de trabajo.
    # print("hello world")
    # https://www.geeksforgeeks.org/check-if-directory-contains-file-using-python/

    path = "/home/vagrant/Documents/bigdata/bigdata_project/data"
    trafic_forlder_name = 'trafico'
    meteo_forlder_name = 'estaciones_metereologicos'
    meteo_path = f'{path}/{meteo_forlder_name}'
    trafic_path = f'{path}/{trafic_forlder_name}'
    # print(meteo_path)
    # print(trafic_path)

    if os.listdir(meteo_path) == "":

        print("ERROR: No meteo data files in forlder available")
        exit

    else:

        print("ok meteo")
        # https://www.programiz.com/python-programming/examples/file-name-from-file-path

    if os.listdir(trafic_path) == "":

        print("ERROR: No meteo data files in forlder available")
        exit
        # https://www.programiz.com/python-programming/examples/file-name-from-file-path

    else:

        print("ok trafic")
        count = np.size(os.listdir(trafic_path))
        print(f'Number od files in directory: {count}')

        for i, y in enumerate(os.listdir(trafic_path)):
            try:

                # print(i, y)
                #files = {i: y}
                df_path = os.path.join(path,trafic_forlder_name)
                print(df_path)

                date_parts = [y.split(".")][0][0]
                # print(y)
                year, month, hour = y.split(
                    "-")[0], y.split("-")[1], y.split("-")[-1]

                # extract the data inside each document
                
                
                df = pd.read_csv(df_path, delimiter=';',
                                 encoding="windows-1252")
                #id_tramo_values = df["Id. Tram / ID. Tramo"].unique

                # select in a dictionary by category a especific column

                
                #lectura = df.groupby("Id. Tram / ID. Tramo")['Lectura'].apply(list)
                #lectura = lectura.to_dict()
                
                lectura = 0
                val_lectura = []
                cod_sensor = 0
                Latitude = 0
                Longitude = 0
                Direccion = ""

                
                # Lectura_dic = Lectura_dic.to_dict()

                # Formating each document
                
                document = {'cod_sensor':cod_sensor,
                            "Dirección":Direccion,
                            "Coordenadas":[Latitude, Longitude],
                            "Valor" : [{'date': {'date_cod': i,
                                     'compl_date': y,
                                     'year': year,
                                     'month': month,
                                     'hour': hour}, 'Lectura':lectura}]
                            }

                print(json.dumps(document, indent=8))
                break

            except Exception as e:
                print(f"ERROR reading {df_path}: {e}")


extractdata()
