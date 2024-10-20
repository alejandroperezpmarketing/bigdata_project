# Author: ALEJANDRO PEREZ PEÑALVER
# This document allow you to connect to a mongo 4.4.29 database in a docker container to
# import and update documents on a collections
# Project URL: https://github.com/alejandroperezpmarketing/bigdata_project
# MongoDB Docker container URL: https://github.com/alejandroperezpmarketing/mogodb
# The update_one() method followed by insert_one() is fine for relatively small datasets, 
# bulk_write() or another batch operation with bid databases to optimize MongoDB interactions.
#https://stackoverflow.com/questions/4796764/read-file-from-line-2-or-skip-header-row

import pandas as pd
import os
import json
import pymongo as pmo
from pymongo import MongoClient as mc


# Database connection

    #########################
    ###### MONGODB CONEXTION#############################
# 1. Client
#client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata')
client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')

db = client.bigdata


def extractdata():
    # Set the paths for your data directories
    path = "/home/vagrant/Documents/bigdata/data"
    trafic_folder_name = 'trafico'
    meteo_folder_name = 'estaciones_metereologicos'
    df_trafic_path = os.path.join(path, trafic_folder_name)
    df_meteo_path = os.path.join(path, meteo_folder_name)

    grouped_data = {}  # Dictionary to store grouped data

    #########################
    # Check if the traffic directory contains any files
    if not os.listdir(df_trafic_path):
        print("ERROR: No traffic data files in folder available")
        return  # Exit the function if no files are found
    else:
        print("ok trafic")
        count_trafic = len(os.listdir(df_trafic_path))  # Get number of files
        print(f'Number of files in directory: {count_trafic}')

        # Loop through each file in the traffic directory
        for i, y in enumerate(os.listdir(df_trafic_path)):
            try:
                trafic_file_path = os.path.join(df_trafic_path, y)
                print(f"Processing file: {trafic_file_path}")

                # Extract date parts from filename assuming it's in the format '2024-5-3T3H0m.csv'
                date_parts = y.split(".")[0]  # Strip file extension
                year, month, day_and_time = date_parts.split(
                    "-")[0], date_parts.split("-")[1], date_parts.split("-")[-1]

                with open(trafic_file_path, 'r', encoding="windows-1252") as file:

                    while True:
                        linea = file.readline()
                        if not linea:
                            break

                        lect_linea = linea.split(";")

                        try:
                            id_tramo = str(lect_linea[1])
                            
                            geo_point = lect_linea[-1]
                            
                            if "," in geo_point:
                                # Split 'geo_point_2d' into latitude and longitude
                                latitude, longitude = str(geo_point).replace(" ","").strip().split(",")
                                latitude, longitude = latitude.strip(), longitude.strip()
                                # print(float(latitude))
                                # print(float(longitude))
                            else:
                                latitude, longitude = None, None
                                print(f"Invalid geo_point_2d value in {trafic_file_path}: {geo_point}")                                
                            
                            lectura_value = lect_linea[2].strip()
                            if lectura_value.replace('.','',1).isnumeric():
                                
                                # print(latitude, longitude)
                                lectura = float(lectura_value)
                                # print(lectura)
                            
                            else:
                                # Skip rows where the 'lectura' value is invalid (e.g., header or non-numeric)
                                print(f"Invalid lectura value in {trafic_file_path}: {lectura_value}")
                                continue  # Skip this row and move to the next iteration
                            direccion = str(lect_linea[3]).lower()
                            estado = float(lect_linea[4])

                            # Dictionary definition
                            if lectura == -1 or lectura > 5000 or estado != 0:
                                lectura = None
                                dictionary = [{"fecha": f"{year}-{month}-{day_and_time}",
                                               "lectura": lectura}]
                            else:
                                # Dictionary definition
                                dictionary = [{"fecha": f"{year}-{month}-{day_and_time}",
                                               "lectura": lectura}]

                                # print(dictionary)

                            #########################
                            # create document on MONGODB
                            
                            result = db.trafico.update_one({"id_tramo":id_tramo},{"$push":{"valores":{"fecha":date_parts, "lectura":lectura}}})
                           
                            if result.raw_result["nModified"] == 0 and result.modified_count == 0 and result.upserted_id == None:
                                db.trafico.insert_one({"id_tramo":id_tramo,
                                                       "direccion":direccion,
                                                        "coordenadas":[latitude,longitude],
                                                        "valores":dictionary})
                            
                             
                                print(f"A new document has been created with id_tramo: {id_tramo}")
                            
                            else:
                                print(f"New entry for the document with id_tramo: {id_tramo}")

                            
                                                 
                            
                        except Exception as e:
                            print(f"ERROR reading {trafic_file_path}: {e}")
                        
            except Exception as e:
                print(f"ERROR reading {trafic_file_path}: {e}")



# Run the function
extractdata()
