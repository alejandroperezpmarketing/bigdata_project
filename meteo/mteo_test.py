# Author: ALEJANDRO PEREZ PEÑALVER
# This document allow you to connect to a mongo 4.4.29 database in a docker container to
# import and update documents on a collections
# Project URL: https://github.com/alejandroperezpmarketing/bigdata_project
# MongoDB Docker container URL: https://github.com/alejandroperezpmarketing/mogodb
# The update_one() method followed by insert_one() is fine for relatively small datasets, 
# bulk_write() or another batch operation with bid databases to optimize MongoDB interactions.

import os
import json
import re
from datetime import datetime
import pymongo as pmo
from pymongo import MongoClient as mc
#import logging

logs_path = "/home/vagrant/Documents/bigdata/bigdata_project/meteo"
#logging.basicConfig(file=f'{logs_path}/meteo_data.log',level=logging.INFO)

coordinates = {
"Av França": [39.457504, -0.342689],
"Bulevard Sud": [39.450378, -0.396313],
"Molí del Sol": [39.481138, -0.408558],
"Pista de Silla": [39.458060, -0.376653],
"Politècnic": [39.479621, -0.337407],
"Centre": [39.470718, -0.376384],
"Vivers": [39.479488, -0.369550],
"València Port Moll Trans Ponent": [39.459264, -0.323217],
"Nazaret Met-2": [39.448554, -0.333289],
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
    id_estacion = 1

    
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
                    #logging.info(f'Prcesing file: {meteo_file_path}')
                    
                    print(f"Opening file: {meteo_file_path}")
                    with open(meteo_file_path, 'r', encoding="iso-8859-1") as file:
                        lecture = file.readlines()
                        
                        
                        #print(lect_line)
                                
                        print(f"Working with the document: {meteo_file_path}")
                        for id, value in enumerate(lecture):
                            
                            lect_line = value.replace('\n','').strip().split("\t")
                            #print(lect_line)
                            if "Estaci" in lect_line[0]:
                                #print(lect_line)
                                
                                #extracting the meteo station name to asign it their coordinates
                                parts = value.strip().split("-",1)
                                """ if len(parts)==2:
                                    estacion = parts[-1].replace('.','').split(' ')
                                   """
                                parts = parts[1].split("-",1)
                                #print(parts[-1])
                     
                                estacion = parts[-1].strip()
                                estacion = estacion.replace('.','')
                                estacion = estacion.replace('\n','')
                                if estacion.startswith(' '):
                                    estacion = estacion[1:]
                                
                                print(estacion)
                                #identifing the station coordiantes in the coordinates dictionary
                                station_coordinates = coordinates.get(estacion) 
                                
                                             
                                # #identifing the station coordiantes in the coordinates dictionary
                                station_coordinates = coordinates.get(estacion)
                                print(station_coordinates)
                            
                            
                            elif "FECHA" in lect_line[0]:
                                #print(result[i])
                                if "NO2" in lect_line:
                                    #print(lect_line)
                                    pos_NO2 = lect_line.index("NO2")
                                    #print(lect_line[pos_NO2])
                                    
                                    print(pos_NO2, lect_line)
                                    print(f'position of NO2 column in the document:{meteo_file_path} is: {pos_NO2}')
                                    #pos_date, pos_hour = lect_line.index("FECHA"), lect_line.index("HORA")
                                            
                                    # print(lect_line)
                               
                                        
                                else:
                                    print(f"ERROR: No NO2 value in: {meteo_file_path}")
                                    print("Closing the program and deleting pre procesed document...")
                                    continue
                                 
                except Exception as e:
                    print(f'{e}')
                    
                    
extractdata_meteo()