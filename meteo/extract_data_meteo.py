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
                    print(f"Processing file: {meteo_file_path}")
                    
                    with open(meteo_file_path, 'r', encoding="iso-8859-1") as file:
                        result = file.readlines()
                        for i, value in enumerate(result):
                            if i==1:
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
                            
                                
                                print(estacion)
                                print(station_coordinates)

                            elif i>6:
                                print("ok")
                                for i, line in enumerate(result):
                                    line = line.split('\t')
                                    print(line)
                except Exception as e:
                    print(f'ERROR reading {meteo_file_path}: {e}')

        else:
            print("ERROR: not enouth files to start working")
            
            
extractdata_meteo()

    
    

