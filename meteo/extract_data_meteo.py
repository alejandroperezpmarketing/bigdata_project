# Author: ALEJANDRO PEREZ PEÑALVER
# This document allow you to connect to a mongo 4.4.29 database in a docker container to
# import and update documents on a collections
# Project URL: https://github.com/alejandroperezpmarketing/bigdata_project
# MongoDB Docker container URL: https://github.com/alejandroperezpmarketing/mogodb
# The update_one() method followed by insert_one() is fine for relatively small datasets, 
# bulk_write() or another batch operation with bid databases to optimize MongoDB interactions.

import pandas as pd
import os
import json
import re
import pymongo as pmo
from pymongo import MongoClient as mc


coordinates = {
"Av França": [39.457504, -0.342689],
"Bulevard Sud": [39.450378, -0.396313],
"Molí del Sol": [39.481138, -0.408558],
"Pista de Silla": [39.458060, -0.376653],
"Politècnic": [39.479621, -0.337407],
"Centre": [39.470718, -0.376384],
"Vivers": [39.479488, -0.369550],
"València Port Moll Trans Ponent": [39.459264, -0.323217],
"Nazaret Met": [39.448554, -0.333289],
"Conselleria Meteo": [39.472034, -0.404878],
"València Olivereta": [39.469238, -0.406037],
"València Port llit antic Túria": [39.450518, -0.328945]}

patterns = [
        r"^\d{4}-\d{2}-\d{2}",    # YYYY-MM-DD
        r"^\d{2}/\d{2}/\d{4}",    # DD/MM/YYYY or MM/DD/YYYY
        r"^[A-Za-z]+\s\d{1,2},\s\d{4}"  # Month Day, Year
    ]

client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')

db = client.bigdata




def extractdata_meteo():
    path="/home/vagrant/Documents/bigdata/bigdata_project/meteo"
    #path = "/home/vagrant/Documents/bigdata/data"
    meteo_folder_name = 'estaciones_metereologicas'
    df_meteo_path = os.path.join(path, meteo_folder_name)
    
    
    #########################
    # Check if the traffic directory contains any files
    if not os.listdir(df_meteo_path):
        print("ERROR: No traffic data files in folder available")
        return  # Exit the function if no files are found
    else:
        print("ok meteo")
        count_meteo = len(os.listdir(df_meteo_path))  # Get number of files
        print(f'Number of files in directory: {count_meteo}')
        
        if count_meteo == 12:
            print("ok: 12 meteo stations in this directory")
             # Loop through each file in the traffic directory
            for i, y in enumerate(os.listdir(df_meteo_path)):
                try:
                    meteo_file_path = os.path.join(df_meteo_path, y)
                    
                    print(f"Opening file: {meteo_file_path}")
                    with open(meteo_file_path, 'r', encoding="iso-8859-1") as file:
                        result = file.readlines()
                        
                        
                        #print(lect_line)
                                
                        print(f"Working with the document: {meteo_file_path}")
                        for id, value in enumerate(result):
                            
                            lect_line = value.replace('\n','').strip().split("\t")
                            
                            if "Estaci" in value:
                                #print(values)
                                
                                #extracting the meteo station name to asign it their coordinates
                                parts = value.strip().split("-")
                                if len(parts)>2:
                                    estacion = parts[2].strip()
                                    estacion = estacion.replace('.','')
                                else:
                                    estacion = parts[1].strip()
                                    estacion = estacion.replace('.','')
                                    

                                if estacion.startswith(' '):
                                    estacion = estacion[1:]
                                    estacion = estacion.replace('.','')
                                #identifing the station coordiantes in the coordinates dictionary
                                station_coordinates = coordinates.get(estacion)
                                print(f'Coordinates for the station {estacion} are: {station_coordinates}')
                               
                            
                            elif "FECHA" in value:
                                #print(result[i])
                                if "NO2" in value:
                                    lect_line = result[id].split("\t")
                                    #print(lect_line)
                                    pos_NO2 = lect_line.index("NO2")
                                    #print(lect_line[pos_NO2])
                                    print(f'position of NO2 column in the document:{meteo_file_path} is: {pos_NO2}')
                                    pos_date, pos_hour = lect_line.index("FECHA"), lect_line.index("HORA")
                                            
                                    # print(lect_line)
                               
                                            

                                            
                                else:
                                    print(f"ERROR: No NO2 value in: {meteo_file_path}")
                                    print("Closing the program and deleting pre procesed document...")
                                    continue
                                ############
                                # NO2 = lect_line[pos_NO2]
                                # print(NO2)
                             # Iterate over patterns and check if any pattern matches the start of the line
                            
                            for pattern in patterns:
                                if re.match(pattern, lect_line[0]):
                                # print(True)
                                    NO2 = lect_line[pos_NO2]
                                    date = lect_line[pos_date]
                                    hour = lect_line[pos_hour]
                                    print(NO2, lect_line, station_coordinates, date,hour)
                                # else:
                                #     date = lect_line[id].next
                                #     print(date)


                          
                                
                                
                except Exception as e:
                    print(f'ERROR reading {meteo_file_path}: {e}')

        else:
            print("ERROR: not enouth files to start working")
            
            
extractdata_meteo()

    
    

