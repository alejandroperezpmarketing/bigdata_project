

import os
import json
import re
from datetime import datetime
import pymongo as pmo
from pymongo import MongoClient as mc
#import logging
import re

##########
id_estacion = 1
###########

# Define date patterns to match different date formats
patterns = [
    r"^\d{4}-\d{2}-\d{2}",    # YYYY-MM-DD
    r"^\d{2}/\d{2}/\d{4}",    # DD/MM/YYYY or MM/DD/YYYY
    r"^[A-Za-z]+\s\d{1,2},\s\d{4}"  # Month Day, Year
]



#########################
###### MONGODB CONEXTION#############################
# 1. Client
#client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata')
client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
db = client.bigdata


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

def contains_date(line):
    # Loop through each pattern to see if the line contains a date
    for pattern in patterns:
        if re.search(pattern, line):
            return True
    return False

def extractdata_meteo():
    path = "/home/vagrant/Documents/bigdata/bigdata_project/meteo"
    meteo_folder_name = 'estaciones_metereologicas'
    df_meteo_path = os.path.join(path, meteo_folder_name)
    
    # Check if the directory contains files
    if not os.listdir(df_meteo_path):
        print("ERROR: No traffic data files in folder available")
        return  # Exit if no files found
    else:
        print("ok meteo")
        count_meteo = len(os.listdir(df_meteo_path))
        print(f'Number of files in directory: {count_meteo}')
        
        if count_meteo == 12:
            print("ok: 12 meteo stations in this directory")
            for i, y in enumerate(os.listdir(df_meteo_path)):
                try:
                    meteo_file_path = os.path.join(df_meteo_path, y)
                    print(f"Opening file: {meteo_file_path}")
                    
                    with open(meteo_file_path, 'r', encoding="iso-8859-1") as file:
                        lecture = file.readlines()
                        pos_NO2 = None
                        pos_date = None
                        pos_hour = None
                        
                        # Process each line in the file
                        for id, line in enumerate(lecture):
                            lect_line = line.replace('\n','').strip().split("\t")
                            
                            if "Estaci" in lect_line[0]:
                                #print(lect_line)
                                
                                #extracting the meteo station name to asign it their coordinates
                                parts = line.strip().split("-",1)
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
                                latitude, longitude = float(station_coordinates[0]), float(station_coordinates[1])
                                             
                                # #identifing the station coordiantes in the coordinates dictionary
                                station_coordinates = coordinates.get(estacion)
                                #print(station_coordinates)
                            
                            
    
                            # Check for the header line containing "FECHA" and "NO2"
                            if "FECHA" in lect_line and "NO2" in lect_line:
                                pos_date = lect_line.index("FECHA")
                                pos_hour = lect_line.index("HORA")
                                pos_NO2 = lect_line.index("NO2")
                                print(f'Position of NO2: {pos_NO2}')
                                continue
                            
                            # After identifying the columns, extract the NO2 value if the line contains a valid date
                            if pos_date is not None:
                                try:
                                    if contains_date(lect_line[pos_date]):
                                        date_value = lect_line[pos_date]
                                        if pos_NO2 is not None:
                                            if  lect_line[pos_NO2]:
                                                NO2_value = lect_line[pos_NO2]
                                            else:
                                                continue
                                                
                                        else:
                                            print(f"ERROR parsing NO2 value from {meteo_file_path} at line {id}")
                                            break
                                        if pos_hour is not None:
                                            if lect_line[pos_hour]:
                                                hour = lect_line[pos_hour]
                                            else:
                                                continue 
                                        else:
                                            print(f"ERROR parsing Hour from {meteo_file_path} at line {id}")
                                            break
                                        
                                        print(f'station {estacion} {station_coordinates} working on line {id} from file {meteo_file_path}')
                                        print(f'Date: {date_value},Hora: {hour}, NO2: {NO2_value}')

                                        ####MONGODB
                                        # Dictionary definition
                                        dictionary = [{"fecha": date_value, "hora": hour, "NO2": NO2_value}]
                                        
                                        
                                        result = db.meteo.update_one({"id_estacion":id_estacion},{"$push":{"valores":{"fecha":date_value,"hora":hour, "NO2":NO2_value}}})
                                        if result.raw_result["nModified"] == 0 and result.modified_count == 0 and result.upserted_id == None:
                                            db.meteo.insert_one({"id_estacion":id_estacion,
                                                                   "nombre":estacion,
                                                                    "coordenadas":[latitude,longitude],
                                                                    "valores":dictionary})                             
                           
                                        
                                    
                                    
                                    else:
                                        continue
                                except Exception as e:  
                                    print(f"ERROR parsing NO2, Date, or Hour from {meteo_file_path} at line {id}: {e}")      

                except Exception as e:
                    print(f'Error processing file {y}: {e}')



extractdata_meteo()